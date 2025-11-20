from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import datetime
from typing import Sequence

from telethon import TelegramClient

from src.db.repositories.group_sheet_repository import GroupSheetRepository
from src.db.repositories.session_repository import SessionRepository
from src.services.google_sheets import (
    FetchError as GFetchError,
    InvalidLinkError as GInvalidError,
    NotFoundError as GNotFoundError,
    PublicAccessRequiredError as GAccessError,
    fetch_rows_from_link,
    parse_google_sheets_link,
)
from src.services.broadcast_shared import deduplicate_broadcast_groups
from typing import Any

logger = logging.getLogger(__name__)


def _groups_hash(parsed: Sequence[Any]) -> str:
    h = hashlib.sha256()
    for g in parsed:
        line = f"{g.name or ''}|{g.username or ''}|{g.link or ''}\n".encode("utf-8")
        h.update(line)
    return h.hexdigest()


class GroupSheetMonitorService:
    """Background monitor that periodically checks Google Sheets links and updates group lists.

    Interval defaults to 600 seconds (10 minutes).
    """

    def __init__(
        self,
        *,
        repository: GroupSheetRepository,
        session_repository: SessionRepository,
        bot_client: TelegramClient,
        # Интервал скачивания таблицы с гугл док
        interval_seconds: float = 600.0,
    ) -> None:
        self._repository = repository
        self._session_repository = session_repository
        self._bot_client = bot_client
        self._interval = interval_seconds
        self._task: asyncio.Task | None = None
        self._stopping = False

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._stopping = False
            self._task = asyncio.create_task(self._run_loop(), name="group-sheet-monitor-loop")

    async def stop(self) -> None:
        self._stopping = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("Sheet monitor loop terminated with error")

    async def _run_loop(self) -> None:
        # Initial immediate poll
        await self._poll_once()
        while not self._stopping:
            try:
                await asyncio.sleep(self._interval)
            except asyncio.CancelledError:
                break
            await self._poll_once()

    async def _poll_once(self) -> None:
        links = await self._repository.list_all_links()
        if not links:
            return
        for link_doc in links:
            if self._stopping:
                break
            await self._process_link(link_doc)

    async def _process_link(self, doc: dict) -> None:
        session_id = doc.get("session_id") or ""
        owner_id = doc.get("owner_id")
        url = doc.get("url") or ""
        old_hash = doc.get("content_hash") or ""
        if not session_id or not isinstance(owner_id, int) or not url:
            return
        try:
            rows = await fetch_rows_from_link(url)
        except (GAccessError, GInvalidError, GNotFoundError, GFetchError) as exc:
            # User notification on connection issue
            try:
                await self._bot_client.send_message(
                    owner_id,
                    "Не удалось подключиться к таблице — проверьте ссылку и права доступа",
                )
            except Exception:  # noqa: BLE001
                logger.debug("Failed to send sheet error message", exc_info=True)
            await self._repository.update_error(
                session_id=session_id,
                owner_id=owner_id,
                error_message=str(exc) or "fetch_failed",
            )
            return
        except Exception as exc:  # noqa: BLE001
            logger.warning("Unexpected sheet fetch error", extra={"session_id": session_id, "err": str(exc)})
            try:
                await self._bot_client.send_message(
                    owner_id,
                    "Не удалось подключиться к таблице — проверьте ссылку и права доступа",
                )
            except Exception:
                pass
            await self._repository.update_error(
                session_id=session_id,
                owner_id=owner_id,
                error_message="unexpected_error",
            )
            return

        try:
            # Local import to avoid circular dependency with bot.context
            from src.bot.commands import groups as groups_cmd  # type: ignore

            parsed_groups = groups_cmd._parse_rows_to_groups(rows)  # type: ignore[attr-defined]
        except Exception:
            # Treat parse failure as connection issue for user simplicity
            try:
                await self._bot_client.send_message(
                    owner_id,
                    "Не удалось подключиться к таблице — проверьте ссылку и права доступа",
                )
            except Exception:
                pass
            await self._repository.update_error(
                session_id=session_id,
                owner_id=owner_id,
                error_message="parse_error",
            )
            return

        if not parsed_groups:
            # Empty table; hash of empty list
            new_hash = _groups_hash(parsed_groups)
            if new_hash == old_hash:
                return
            # Update stored groups to empty set only if previously had content
            await self._repository.update_state(
                session_id=session_id,
                owner_id=owner_id,
                content_hash=new_hash,
                last_sync_ts=datetime.utcnow(),
            )
            try:
                await self._session_repository.set_broadcast_groups(
                    session_id,
                    [],
                    owner_id=owner_id,
                    unique_groups=[],
                    stats={"file_rows": 0, "unique_groups": 0, "actual_targets": 0},
                )
            except Exception:
                logger.exception("Failed to persist empty groups list from sheet", extra={"session_id": session_id})
                return
            try:
                await self._bot_client.send_message(owner_id, "Список групп обновлён: обнаружены изменения в таблице")
            except Exception:
                pass
            return

        new_hash = _groups_hash(parsed_groups)
        if new_hash == old_hash:
            # No changes
            return

        # Enrich and deduplicate similar to manual upload pipeline
        enriched: list[dict] = []
        # Local import for functions to avoid circular import at module level
        from src.bot.commands import groups as groups_cmd  # type: ignore

        for g in parsed_groups:
            try:
                chat_id, is_member = await groups_cmd._resolve_chat_id(self._bot_client, g.username, g.link)  # noqa: SLF001
            except Exception:
                chat_id, is_member = None, None
            enriched.append(groups_cmd._serialize_group(g, chat_id, is_member))  # noqa: SLF001

        unique_groups = deduplicate_broadcast_groups(enriched)
        stats = {
            "file_rows": len(enriched),
            "unique_groups": len(unique_groups),
            "actual_targets": len(unique_groups),
        }
        try:
            success = await self._session_repository.set_broadcast_groups(
                session_id,
                enriched,
                owner_id=owner_id,
                unique_groups=unique_groups,
                stats=stats,
            )
            if not success:
                raise RuntimeError("session_update_failed")
        except Exception:
            logger.exception("Failed to persist updated groups from sheet", extra={"session_id": session_id})
            return

        await self._repository.update_state(
            session_id=session_id,
            owner_id=owner_id,
            content_hash=new_hash,
            last_sync_ts=datetime.utcnow(),
        )

        # Notify user about changes (no logging of success per requirements)
        try:
            await self._bot_client.send_message(owner_id, "Список групп обновлён: обнаружены изменения в таблице")
        except Exception:
            pass
