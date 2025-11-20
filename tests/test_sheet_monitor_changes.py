import asyncio
import pytest
from datetime import datetime

from src.services.sheet_monitor import GroupSheetMonitorService
from src.db.repositories.group_sheet_repository import GroupSheetRepository
from src.db.repositories.session_repository import SessionRepository


class FakeCollection:
    def __init__(self):
        self.docs = []

    async def create_index(self, *args, **kwargs):
        return None

    async def find_one_and_update(self, filter, update, upsert=False, return_document=None):
        for d in self.docs:
            if d.get("session_id") == filter.get("session_id") and d.get("owner_id") == filter.get("owner_id"):
                for k, v in update.get("$set", {}).items():
                    d[k] = v
                if "$unset" in update:
                    for k in update["$unset"].keys():
                        d.pop(k, None)
                d["updated_at"] = datetime.utcnow()
                return d
        if upsert:
            new_doc = {**filter}
            for k, v in update.get("$set", {}).items():
                new_doc[k] = v
            if "$setOnInsert" in update:
                for k, v in update["$setOnInsert"].items():
                    new_doc[k] = v
            new_doc["updated_at"] = datetime.utcnow()
            self.docs.append(new_doc)
            return new_doc
        return None

    async def find(self, filter):
        async def gen():
            for d in list(self.docs):
                yield d
        return gen()


class FakeDB:
    def __init__(self):
        self.collection = FakeCollection()

    def get_collection(self, name):
        return self.collection


class StubSessionRepo(SessionRepository):
    def __init__(self):
        pass

    async def set_broadcast_groups(self, session_id, groups, owner_id, unique_groups=None, stats=None):  # noqa: D401,E501
        self.last_saved = {
            "session_id": session_id,
            "groups": groups,
            "owner_id": owner_id,
            "stats": stats or {},
        }
        return True


class StubBotClient:
    def __init__(self):
        self.messages = []

    async def send_message(self, user_id, text):
        self.messages.append((user_id, text))
        return True


@pytest.mark.asyncio
async def test_monitor_detects_change(monkeypatch):
    db = FakeDB()
    repo = GroupSheetRepository(db)
    await repo.upsert_link(
        session_id="s1",
        owner_id=10,
        url="https://docs.google.com/spreadsheets/d/abc/edit#gid=0",
        spreadsheet_id="abc",
        gid="0",
        content_hash="old",
        last_sync_ts=datetime.utcnow(),
    )

    async def fake_fetch(url):
        return [
            ["Название", "Username", "Ссылка"],
            ["G1", "g1", ""],
        ]

    from src.services import google_sheets as gs_mod
    monkeypatch.setattr(gs_mod, "fetch_rows_from_link", fake_fetch)

    monitor = GroupSheetMonitorService(
        repository=repo,
        session_repository=StubSessionRepo(),
        bot_client=StubBotClient(),
        interval_seconds=0.1,
    )

    await monitor._poll_once()  # type: ignore[attr-defined]
    assert monitor._bot_client.messages, "Expected notification on change"
    assert "обнаружены изменения" in monitor._bot_client.messages[0][1]


@pytest.mark.asyncio
async def test_monitor_no_change(monkeypatch):
    db = FakeDB()
    repo = GroupSheetRepository(db)
    # Precompute hash for same content
    rows = [["Название", "Username", "Ссылка"], ["G1", "g1", ""]]
    from src.bot.commands import groups as groups_cmd
    parsed = groups_cmd._parse_rows_to_groups(rows)
    import hashlib
    h = hashlib.sha256()
    for g in parsed:
        h.update(f"{g.name or ''}|{g.username or ''}|{g.link or ''}\n".encode())
    digest = h.hexdigest()
    await repo.upsert_link(
        session_id="s1",
        owner_id=10,
        url="https://docs.google.com/spreadsheets/d/abc/edit#gid=0",
        spreadsheet_id="abc",
        gid="0",
        content_hash=digest,
        last_sync_ts=datetime.utcnow(),
    )

    async def fake_fetch(url):
        return rows

    from src.services import google_sheets as gs_mod
    monkeypatch.setattr(gs_mod, "fetch_rows_from_link", fake_fetch)

    monitor = GroupSheetMonitorService(
        repository=repo,
        session_repository=StubSessionRepo(),
        bot_client=StubBotClient(),
        interval_seconds=0.1,
    )
    await monitor._poll_once()  # type: ignore[attr-defined]
    assert monitor._bot_client.messages == [], "No notification expected when no change"
