from __future__ import annotations

import asyncio
import contextlib
import logging
import re
from typing import Dict, Optional

from telethon import events
from telethon.events import CallbackQuery, NewMessage
from telethon.errors import MessageIdInvalidError, MessageNotModifiedError

from src.bot.context import BotContext
from src.bot.keyboards import build_main_menu_keyboard


logger = logging.getLogger(__name__)

_CANCEL_TEXT_PATTERN = re.compile(r"^\s*отмена\s*$", re.IGNORECASE)
_CANCEL_CALLBACK_PATTERN = re.compile(rb"^cancel\b", re.IGNORECASE)
_CANCEL_RESPONSE = "Действие отменено. Возвращаю в главное меню."


async def _maybe_remove_inline_keyboard(client, user_id: int, message_id: Optional[int]) -> None:
    if not message_id:
        return
    try:
        await client.edit_message(user_id, message_id, buttons=None)
    except (MessageNotModifiedError, MessageIdInvalidError):
        return
    except Exception:  # pragma: no cover - defensive logging
        logger.debug(
            "Не удалось убрать inline-клавиатуру",
            exc_info=True,
            extra={"user_id": user_id, "message_id": message_id},
        )


async def _cleanup_auth_state(context: BotContext, client, user_id: int, state) -> None:
    if state is None:
        return
    qr_task = getattr(state, "qr_task", None)
    if qr_task is not None:
        qr_task.cancel()
        state.qr_task = None
    auth_client = getattr(state, "client", None)
    if auth_client is not None:
        with contextlib.suppress(Exception):
            await context.session_manager.close_client(auth_client)
    await _maybe_remove_inline_keyboard(client, user_id, getattr(state, "last_message_id", None))


async def _cleanup_broadcast_flow(client, user_id: int, state) -> None:
    if state is None:
        return
    await _maybe_remove_inline_keyboard(client, user_id, getattr(state, "last_message_id", None))


async def _cleanup_broadcast_run(context: BotContext, client, user_id: int, state) -> None:
    if state is None:
        return
    task = getattr(state, "task", None)
    if task and not task.done():
        task.cancel()
        with contextlib.suppress(asyncio.TimeoutError, asyncio.CancelledError, Exception):
            await asyncio.wait_for(task, timeout=1.0)
    await _maybe_remove_inline_keyboard(client, user_id, getattr(state, "last_message_id", None))
    await _maybe_remove_inline_keyboard(client, user_id, getattr(state, "progress_message_id", None))


async def _cleanup_group_upload(client, user_id: int, state) -> None:
    if state is None:
        return
    await _maybe_remove_inline_keyboard(client, user_id, getattr(state, "last_message_id", None))


async def _cleanup_group_view(client, user_id: int, state) -> None:
    if state is None:
        return
    await _maybe_remove_inline_keyboard(client, user_id, getattr(state, "last_message_id", None))


def _state_snapshot(states: Dict[str, object]) -> Dict[str, object]:
    return {
        "auth_step": getattr(getattr(states.get("auth"), "step", None), "value", None),
        "broadcast_step": getattr(getattr(states.get("broadcast"), "step", None), "value", None),
        "broadcast_run_step": getattr(getattr(states.get("broadcast_run"), "step", None), "value", None),
        "group_upload_step": getattr(getattr(states.get("group_upload"), "step", None), "value", None),
        "group_view_step": getattr(getattr(states.get("group_view"), "step", None), "value", None),
    }


async def _handle_cancel(event, *, context: BotContext, source: str) -> None:
    user_id = event.sender_id

    auth_state = context.auth_manager.clear(user_id)
    broadcast_state = context.broadcast_manager.clear(user_id)
    broadcast_run_state = context.broadcast_run_manager.clear(user_id)
    group_upload_state = context.groups_manager.clear(user_id)
    group_view_state = context.group_view_manager.clear(user_id)

    states = {
        "auth": auth_state,
        "broadcast": broadcast_state,
        "broadcast_run": broadcast_run_state,
        "group_upload": group_upload_state,
        "group_view": group_view_state,
    }

    snapshot = _state_snapshot(states)
    logger.info(
        "Получена глобальная отмена",
        extra={
            "user_id": user_id,
            "cancel_source": source,
            **snapshot,
        },
    )

    client = event.client

    await _cleanup_auth_state(context, client, user_id, auth_state)
    await _cleanup_broadcast_flow(client, user_id, broadcast_state)
    await _cleanup_broadcast_run(context, client, user_id, broadcast_run_state)
    await _cleanup_group_upload(client, user_id, group_upload_state)
    await _cleanup_group_view(client, user_id, group_view_state)

    if isinstance(event, CallbackQuery.Event):
        with contextlib.suppress(Exception):
            await event.answer("Операция отменена.")
        with contextlib.suppress(Exception):
            await event.edit(buttons=None)

    await client.send_message(user_id, _CANCEL_RESPONSE, buttons=build_main_menu_keyboard())

    raise events.StopPropagation


def setup_cancel_command(client, context: BotContext) -> None:
    """Register a global cancellation handler applicable in any state."""

    def _is_cancel_message(event: NewMessage.Event) -> bool:
        if not event.is_private or getattr(event.message, "out", False):
            return False
        text = event.raw_text or ""
        return bool(_CANCEL_TEXT_PATTERN.match(text))

    @client.on(events.NewMessage(func=_is_cancel_message))
    async def handle_cancel_message(event: NewMessage.Event) -> None:
        await _handle_cancel(event, context=context, source="text")

    def _is_cancel_callback(event: CallbackQuery.Event) -> bool:
        if not event.is_private:
            return False
        if event.data is None:
            return False
        return bool(_CANCEL_CALLBACK_PATTERN.match(event.data))

    @client.on(events.CallbackQuery(func=_is_cancel_callback))
    async def handle_cancel_callback(event: CallbackQuery.Event) -> None:
        await _handle_cancel(event, context=context, source="callback")
