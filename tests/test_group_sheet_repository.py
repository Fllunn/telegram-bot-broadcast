import asyncio
import pytest
from datetime import datetime

from src.db.repositories.group_sheet_repository import GroupSheetRepository


class FakeCollection:
    def __init__(self):
        self.docs = []

    async def create_index(self, *args, **kwargs):  # noqa: D401
        return None

    async def find_one_and_update(self, filter, update, upsert=False, return_document=None):  # noqa: ANN001
        # Simple match
        for d in self.docs:
            if d.get("session_id") == filter.get("session_id") and d.get("owner_id") == filter.get("owner_id"):
                # Apply $set
                set_payload = update.get("$set", {})
                for k, v in set_payload.items():
                    d[k] = v
                if "$unset" in update:
                    for k in update["$unset"].keys():
                        d.pop(k, None)
                d["updated_at"] = datetime.utcnow()
                return d
        if upsert:
            new_doc = {**filter}
            set_payload = update.get("$set", {})
            for k, v in set_payload.items():
                new_doc[k] = v
            if "$setOnInsert" in update:
                for k, v in update["$setOnInsert"].items():
                    new_doc[k] = v
            new_doc["updated_at"] = datetime.utcnow()
            self.docs.append(new_doc)
            return new_doc
        return None

    async def find(self, filter):  # noqa: ANN001
        async def gen():
            for d in list(self.docs):
                yield d
        return gen()


class FakeDB:
    def __init__(self):
        self.collection = FakeCollection()

    def get_collection(self, name):  # noqa: D401
        return self.collection


@pytest.mark.asyncio
async def test_repository_upsert_and_update_state():
    db = FakeDB()
    repo = GroupSheetRepository(db, collection_name="group_sheet_links")

    doc = await repo.upsert_link(
        session_id="s1",
        owner_id=123,
        url="https://docs.google.com/spreadsheets/d/abc/edit#gid=0",
        spreadsheet_id="abc",
        gid="0",
    )
    assert doc["session_id"] == "s1"
    assert doc["content_hash"] is None

    updated = await repo.update_state(session_id="s1", owner_id=123, content_hash="hash1", last_sync_ts=datetime.utcnow())
    assert updated is not None
    assert updated["content_hash"] == "hash1"


@pytest.mark.asyncio
async def test_repository_error_state():
    db = FakeDB()
    repo = GroupSheetRepository(db, collection_name="group_sheet_links")
    await repo.upsert_link(session_id="s1", owner_id=1, url="u", spreadsheet_id="a", gid="0")
    err_doc = await repo.update_error(session_id="s1", owner_id=1, error_message="fail")
    assert err_doc is not None
    assert err_doc["last_error"] == "fail"
