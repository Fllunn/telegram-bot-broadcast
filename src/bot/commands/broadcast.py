from __future__ import annotations

import logging
import re
from typing import Iterable

from telethon import Button, events
from telethon.events import NewMessage

from src.bot.context import BotContext
from src.bot.keyboards import ADD_TEXT_LABEL, build_main_menu_keyboard
from src.models.session import TelethonSession
from src.services.broadcast_state import BroadcastStep

logger = logging.getLogger(__name__)

CANCEL_LABEL = "Отмена"
SCOPE_SINGLE = "single"
SCOPE_ALL = "all"
SCOPE_PREFIX = "broadcast_scope"
SELECT_PREFIX = "broadcast_select"
CONFIRM_PREFIX = "broadcast_confirm"
CANCEL_PREFIX = "broadcast_cancel"
ADD_TEXT_PATTERN = rf"^(?:/add_text(?:@\w+)?|{re.escape(ADD_TEXT_LABEL)})$"


def _extract_payload(data: bytes, prefix: str) -> str | None:
    try:
        decoded = data.decode("utf-8")
    except UnicodeDecodeError:
        return None
    if not decoded.startswith(prefix):
        return None
    return decoded.split(":", maxsplit=1)[-1]


def _expect_step(context: BotContext, step: BroadcastStep):
    def predicate(event: NewMessage.Event) -> bool:
        if not event.is_private or getattr(event.message, "out", False):
            return False
        state = context.broadcast_manager.get(event.sender_id)
        if state is None or state.step != step:
            return False
        if state.last_message_id is not None and state.last_message_id == event.id:
            return False
        return True

    return predicate


def _render_session_label(session: TelethonSession) -> str:
    display = session.display_name()
    phone = session.phone
    return f"{display} ({phone})" if phone else display


def _build_scope_buttons() -> list[list[Button]]:
    return [
        [
            Button.inline("Один аккаунт", f"{SCOPE_PREFIX}:{SCOPE_SINGLE}".encode("utf-8")),
            Button.inline("Все аккаунты", f"{SCOPE_PREFIX}:{SCOPE_ALL}".encode("utf-8")),
        ],
        [Button.inline("❌ Отмена", f"{CANCEL_PREFIX}:scope".encode("utf-8"))],
    ]


def _build_accounts_buttons(sessions: Iterable[TelethonSession]) -> list[list[Button]]:
    rows: list[list[Button]] = []
    for session in sessions:
        rows.append(
            [
                Button.inline(
                    _render_session_label(session),
                    f"{SELECT_PREFIX}:{session.session_id}".encode("utf-8"),
                )
            ]
        )
    rows.append([Button.inline("❌ Отмена", f"{CANCEL_PREFIX}:accounts".encode("utf-8"))])
    return rows


def _build_confirmation_buttons() -> list[list[Button]]:
    return [
        [
            Button.inline("✅ Да", f"{CONFIRM_PREFIX}:yes".encode("utf-8")),
            Button.inline("❌ Отмена", f"{CONFIRM_PREFIX}:no".encode("utf-8")),
        ]
    ]


def setup_broadcast_commands(client, context: BotContext) -> None:
    """Register broadcast-related command handlers."""

    @client.on(events.NewMessage(pattern=ADD_TEXT_PATTERN))
    async def handle_add_text_command(event: NewMessage.Event) -> None:
        if not event.is_private:
            return

        user_id = event.sender_id
        if context.broadcast_manager.has_active_flow(user_id):
            await event.respond(
                "Вы уже настраиваете текст для рассылки. Завершите текущий процесс или отправьте «Отмена».",
                buttons=[[Button.text(CANCEL_LABEL, resize=True)]],
            )
            return

        sessions = list(await context.session_manager.get_active_sessions(user_id))
        if not sessions:
            await event.respond(
                "У вас нет подключённых аккаунтов. Подключите аккаунт, чтобы добавить текст для рассылки.",
                buttons=build_main_menu_keyboard(),
            )
            return

        context.broadcast_manager.begin(user_id, step=BroadcastStep.CHOOSING_SCOPE, last_message_id=event.id)
        logger.info("Пользователь %s начал настройку текста рассылки", user_id)
        prompt = (
            "Для каких аккаунтов сохранить текст рассылки?\n"
            "Выберите нужный вариант ниже."
        )
        message = await event.respond(prompt, buttons=_build_scope_buttons())
        context.broadcast_manager.update(user_id, last_message_id=message.id)

    @client.on(events.CallbackQuery(pattern=rf"^{SCOPE_PREFIX}:".encode("utf-8")))
    async def handle_scope_choice(event: events.CallbackQuery.Event) -> None:
        user_id = event.sender_id
        state = context.broadcast_manager.get(user_id)
        if state is None or state.step != BroadcastStep.CHOOSING_SCOPE:
            await event.answer("Эта операция больше неактуальна.", alert=True)
            return

        scope = _extract_payload(event.data, SCOPE_PREFIX)
        if scope is None:
            await event.answer("Некорректный выбор.", alert=True)
            return

        sessions = list(await context.session_manager.get_active_sessions(user_id))
        if not sessions:
            await event.answer("Нет подключённых аккаунтов.", alert=True)
            await event.edit(
                "У вас нет подключённых аккаунтов. Подключите аккаунт, чтобы добавить текст для рассылки.",
                buttons=build_main_menu_keyboard(),
            )
            context.broadcast_manager.clear(user_id)
            return

        if scope == SCOPE_SINGLE:
            context.broadcast_manager.update(user_id, step=BroadcastStep.CHOOSING_ACCOUNT, apply_to_all=False)
            edited = await event.edit(
                "Выберите аккаунт, для которого нужно сохранить текст:",
                buttons=_build_accounts_buttons(sessions),
            )
            context.broadcast_manager.update(user_id, last_message_id=edited.id)
            return

        if scope == SCOPE_ALL:
            session_ids = [session.session_id for session in sessions]
            context.broadcast_manager.update(
                user_id,
                apply_to_all=True,
                target_session_ids=session_ids,
            )
            existing = [s for s in sessions if (s.metadata or {}).get("broadcast_text")]
            if existing:
                context.broadcast_manager.update(user_id, step=BroadcastStep.CONFIRMING_REPLACE)
                warning = (
                    "В некоторых аккаунтах уже есть текст для рассылки.\n"
                    "Вы действительно хотите его заменить для всех аккаунтов?"
                )
                edited = await event.edit(warning, buttons=_build_confirmation_buttons())
                context.broadcast_manager.update(user_id, last_message_id=edited.id)
                return

            context.broadcast_manager.update(user_id, step=BroadcastStep.WAITING_TEXT)
            edited = await event.edit(
                "Отправьте текст, который будем использовать для рассылки по всем аккаунтам.",
                buttons=[[Button.text(CANCEL_LABEL, resize=True)]],
            )
            context.broadcast_manager.update(user_id, last_message_id=edited.id)
            return

        await event.answer("Неизвестный вариант.", alert=True)

    @client.on(events.CallbackQuery(pattern=rf"^{SELECT_PREFIX}:".encode("utf-8")))
    async def handle_account_selection(event: events.CallbackQuery.Event) -> None:
        user_id = event.sender_id
        state = context.broadcast_manager.get(user_id)
        if state is None or state.step != BroadcastStep.CHOOSING_ACCOUNT:
            await event.answer("Эта операция больше неактуальна.", alert=True)
            return

        session_id = _extract_payload(event.data, SELECT_PREFIX)
        if session_id is None:
            await event.answer("Некорректный выбор.", alert=True)
            return

        session = await context.session_repository.get_by_session_id(session_id)
        if session is None or session.owner_id != user_id:
            await event.answer("Сессия не найдена.", alert=True)
            return

        context.broadcast_manager.update(
            user_id,
            target_session_ids=[session.session_id],
            apply_to_all=False,
        )

        if (session.metadata or {}).get("broadcast_text"):
            context.broadcast_manager.update(user_id, step=BroadcastStep.CONFIRMING_REPLACE)
            edited = await event.edit(
                "Для выбранного аккаунта уже есть текст. Заменить его?",
                buttons=_build_confirmation_buttons(),
            )
            context.broadcast_manager.update(user_id, last_message_id=edited.id)
            return

        context.broadcast_manager.update(user_id, step=BroadcastStep.WAITING_TEXT)
        edited = await event.edit(
            "Отправьте текст, который будем использовать для выбранного аккаунта.",
            buttons=[[Button.text(CANCEL_LABEL, resize=True)]],
        )
        context.broadcast_manager.update(user_id, last_message_id=edited.id)

    @client.on(events.CallbackQuery(pattern=rf"^{CONFIRM_PREFIX}:".encode("utf-8")))
    async def handle_confirmation(event: events.CallbackQuery.Event) -> None:
        user_id = event.sender_id
        state = context.broadcast_manager.get(user_id)
        if state is None or state.step != BroadcastStep.CONFIRMING_REPLACE:
            await event.answer("Эта операция больше неактуальна.", alert=True)
            return

        payload = _extract_payload(event.data, CONFIRM_PREFIX)
        if payload == "yes":
            context.broadcast_manager.update(user_id, step=BroadcastStep.WAITING_TEXT)
            edited = await event.edit(
                "Введите новый текст для рассылки.",
                buttons=[[Button.text(CANCEL_LABEL, resize=True)]],
            )
            context.broadcast_manager.update(user_id, last_message_id=edited.id)
            return

        await event.edit(
            "Изменение текста отменено.",
            buttons=build_main_menu_keyboard(),
        )
        context.broadcast_manager.clear(user_id)

    @client.on(events.CallbackQuery(pattern=rf"^{CANCEL_PREFIX}:".encode("utf-8")))
    async def handle_flow_cancel(event: events.CallbackQuery.Event) -> None:
        user_id = event.sender_id
        if not context.broadcast_manager.has_active_flow(user_id):
            await event.answer("Нечего отменять.", alert=True)
            return

        context.broadcast_manager.clear(user_id)
        await event.edit("Настройка рассылки отменена.", buttons=build_main_menu_keyboard())

    @client.on(events.NewMessage(incoming=True, func=_expect_step(context, BroadcastStep.WAITING_TEXT)))
    async def handle_broadcast_text(event: NewMessage.Event) -> None:
        user_id = event.sender_id
        text = (event.raw_text or "").strip()

        if text.lower() == CANCEL_LABEL.lower():
            context.broadcast_manager.clear(user_id)
            await event.respond("Настройка рассылки отменена.", buttons=build_main_menu_keyboard())
            return

        if not text:
            context.broadcast_manager.update(user_id, last_message_id=event.id)
            await event.respond(
                "Текст не может быть пустым. Отправьте сообщение ещё раз или напишите «Отмена».",
                buttons=[[Button.text(CANCEL_LABEL, resize=True)]],
            )
            return

        state = context.broadcast_manager.get(user_id)
        if state is None or not state.target_session_ids:
            logger.warning("Нет целевых сессий для сохранения текста", extra={"user_id": user_id})
            await event.respond(
                "Не удалось определить целевые аккаунты. Попробуйте начать заново командой /add_text.",
                buttons=build_main_menu_keyboard(),
            )
            context.broadcast_manager.clear(user_id)
            return

        try:
            modified = await context.session_repository.set_broadcast_texts(state.target_session_ids, text)
        except Exception:
            logger.exception("Ошибка при сохранении текста рассылки", extra={"user_id": user_id})
            context.broadcast_manager.update(user_id, last_message_id=event.id)
            await event.respond(
                "Не удалось сохранить текст. Попробуйте ещё раз или отправьте «Отмена».",
                buttons=[[Button.text(CANCEL_LABEL, resize=True)]],
            )
            return

        context.broadcast_manager.clear(user_id)
        logger.info(
            "Пользователь %s сохранил текст для %s аккаунтов",
            user_id,
            modified,
        )
        await event.respond(
            "Текст для рассылки сохранён. Вы можете изменить его командой /add_text или продолжить с выбранными аккаунтами.",
            buttons=build_main_menu_keyboard(),
        )
