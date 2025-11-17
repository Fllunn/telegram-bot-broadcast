from __future__ import annotations

import asyncio
import contextlib
import io
import logging
import re
from typing import Any, Awaitable, Callable

import qrcode
from telethon import Button, TelegramClient, events
from telethon.tl.custom.message import Message
from telethon.events import NewMessage
from telethon.errors import (
    MessageNotModifiedError,
    PasswordHashInvalidError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PhoneNumberBannedError,
    PhoneNumberInvalidError,
    SessionPasswordNeededError,
)

from src.bot.context import BotContext
from src.bot.keyboards import ACCOUNTS_LABEL, LOGIN_PHONE_LABEL, LOGIN_QR_LABEL, build_main_menu_keyboard
from src.models.session import SessionOwnerType, TelethonSession
from src.services.auth_state import AuthSession, AuthStep
from src.services.account_status import AccountStatusResult


logger = logging.getLogger(__name__)

CANCEL_LABEL = "Отмена"
QR_REFRESH_LABEL = "Обновить QR"
QR_IMAGE_NAME = "telegram_login_qr.png"
QR_REFRESH_PREFIX = "qr_refresh"
QR_CANCEL_PREFIX = "qr_cancel"
QR_REFRESH_PATTERN = rf"^{QR_REFRESH_PREFIX}:".encode("utf-8")
QR_CANCEL_PATTERN = rf"^{QR_CANCEL_PREFIX}:".encode("utf-8")
LOGIN_PHONE_PATTERN = rf"^(?:/login_phone(?:@\w+)?|{re.escape(LOGIN_PHONE_LABEL)})$"
LOGIN_QR_PATTERN = rf"^(?:/login_qr(?:@\w+)?|{re.escape(LOGIN_QR_LABEL)})$"
ACCOUNTS_PATTERN = rf"^(?:/accounts(?:@\w+)?|{re.escape(ACCOUNTS_LABEL)})$"

SendMessageFn = Callable[[str, Any], Awaitable[object]]


def _extract_callback_payload(data: bytes, prefix: str) -> str | None:
    try:
        payload = data.decode("utf-8")
    except UnicodeDecodeError:
        return None
    if not payload.startswith(prefix):
        return None
    return payload.split(":", maxsplit=1)[-1]


def _encode_callback_data(prefix: str, payload: str) -> bytes:
    return f"{prefix}:{payload}".encode("utf-8")


def _render_account_target(session: TelethonSession) -> str:
    phone = session.phone or "не указан"
    username = (session.metadata or {}).get("username")
    return f"@{username} ({phone})" if username else phone


def _format_session(session: TelethonSession) -> str:
    title = _render_account_target(session)
    status = "активен" if session.is_active else "неактивен"
    return f"• {title} ({status})"


def _format_session_status(
    session: TelethonSession,
    status: AccountStatusResult | None,
    pending: bool = False,
) -> str:
    title = _render_account_target(session)
    if pending and status is None:
        return f"• {title} (проверяем...)"
    if status is None:
        fallback = "активен" if session.is_active else "неактивен"
        return f"• {title} ({fallback})"
    if status.active:
        return f"• {title} (активен)"
    reason = status.reason if status.reason else "требуется повторный вход"
    return f"• {title} (неактивен)"


def _build_single_button(label: str) -> list[list[Button]]:
    return [[Button.text(label, resize=True)]]


def _build_logout_buttons(sessions: list[TelethonSession]) -> list[list[Button]]:
    rows: list[list[Button]] = []
    for session in sessions:
        callback_data = f"logout_req:{session.session_id}".encode("utf-8")
        label = f"Отвязать {_render_account_target(session)}"
        rows.append([Button.inline(label, callback_data)])
    return rows


def _cancel_qr_task(state: AuthSession | None) -> None:
    if state and state.qr_task is not None:
        state.qr_task.cancel()
        state.qr_task = None


def _build_qr_buttons(user_id: int) -> list[list[Button]]:
    return [
        [
            Button.inline(f"🔄 {QR_REFRESH_LABEL}", _encode_callback_data(QR_REFRESH_PREFIX, str(user_id))),
            Button.inline("❌ Отмена", _encode_callback_data(QR_CANCEL_PREFIX, str(user_id))),
        ]
    ]


def _generate_qr_image(url: str) -> io.BytesIO:
    qr = qrcode.QRCode(box_size=8, border=2, error_correction=qrcode.constants.ERROR_CORRECT_M)
    qr.add_data(url)
    qr.make(fit=True)
    image = qr.make_image(fill_color="black", back_color="white")
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    buffer.seek(0)
    buffer.name = QR_IMAGE_NAME
    return buffer


def _build_qr_caption(existing_sessions: list[TelethonSession] | None = None) -> str:
    sections: list[str] = []
    if existing_sessions:
        body = "\n".join(_format_session(session) for session in existing_sessions)
        sections.append(f"У вас уже подключены аккаунты:\n{body}")
    sections.append(
        "Откройте Telegram на другом устройстве → Настройки → Устройства → Подключить устройство и отсканируйте QR-код."
    )
    sections.append(
        "QR-код действует 1 минуту. Используйте «Обновить QR», чтобы выдать новый, или «Отмена», чтобы прервать процесс."
    )
    return "\n\n".join(sections)


async def _send_qr_via_event(
    event: NewMessage.Event,
    user_id: int,
    qr_login,
    existing_sessions: list[TelethonSession] | None,
) -> "Message":
    qr_image = _generate_qr_image(qr_login.url)
    return await event.respond(
        _build_qr_caption(existing_sessions),
        file=qr_image,
        buttons=_build_qr_buttons(user_id),
    )


async def _send_qr_via_client(
    client: TelegramClient,
    user_id: int,
    qr_login,
    existing_sessions: list[TelethonSession] | None,
):
    qr_image = _generate_qr_image(qr_login.url)
    return await client.send_file(
        user_id,
        file=qr_image,
        caption=_build_qr_caption(existing_sessions),
        buttons=_build_qr_buttons(user_id),
    )


async def _cleanup_session(
    context: BotContext,
    user_id: int,
    session_client: TelegramClient | None = None,
) -> None:
    state = context.auth_manager.clear(user_id)
    client_to_close = session_client
    if state:
        _cancel_qr_task(state)
        if client_to_close is None:
            client_to_close = state.client
    if client_to_close is not None:
        try:
            await context.session_manager.close_client(client_to_close)
        except Exception:  # pragma: no cover - defensive logging only
            logger.exception(
                "Не удалось корректно закрыть временный Telethon-клиент",
                extra={"user_id": user_id},
            )


async def _wait_for_qr_authorization(
    bot_client: TelegramClient,
    context: BotContext,
    user_id: int,
) -> None:
    while True:
        state = context.auth_manager.get(user_id)
        if state is None or state.qr_login is None:
            return

        session_client = state.client
        if session_client is None:
            await _cleanup_session(context, user_id)
            await bot_client.send_message(
                user_id,
                "Попробуйте начать авторизацию заново командой /login_qr.",
                buttons=build_main_menu_keyboard(),
            )
            return

        try:
            user = await state.qr_login.wait()
        except asyncio.CancelledError:
            return
        except SessionPasswordNeededError:
            context.auth_manager.update(
                user_id,
                step=AuthStep.WAITING_PASSWORD,
                qr_task=None,
                qr_login=None,
                last_message_id=None,
            )
            await bot_client.send_message(
                user_id,
                "Введите пароль двухфакторной аутентификации:",
                buttons=_build_single_button(CANCEL_LABEL),
            )
            return
        except asyncio.TimeoutError:
            context.auth_manager.update(user_id, qr_task=None, last_message_id=None)
            await bot_client.send_message(
                user_id,
                "⏳ Время действия QR-кода истекло. Нажмите «Обновить QR», чтобы получить новый код.",
                buttons=_build_qr_buttons(user_id),
            )
            return
        except Exception:
            logger.exception("Ошибка при ожидании авторизации по QR", extra={"user_id": user_id})
            await _cleanup_session(context, user_id, session_client=session_client)
            await bot_client.send_message(
                user_id,
                "Не удалось завершить авторизацию по QR. Попробуйте заново или используйте вход по номеру телефона.",
                buttons=build_main_menu_keyboard(),
            )
            return

        phone = getattr(user, "phone", None)
        context.auth_manager.update(user_id, qr_task=None, phone=phone, qr_login=None)
        await _finalize_login(
            context,
            user_id=user_id,
            phone=phone,
            session_client=session_client,
            send_message=lambda text, buttons: bot_client.send_message(user_id, text, buttons=buttons),
        )
        return


def _expect_step(context: BotContext, step: AuthStep):
    def predicate(event: NewMessage.Event) -> bool:
        if not event.is_private or getattr(event.message, "out", False):
            return False
        state = context.auth_manager.get(event.sender_id)
        if state is None or state.step != step:
            return False
        if state.last_message_id is not None and state.last_message_id == event.id:
            return False
        return True

    return predicate


async def _cancel_flow(event: NewMessage.Event, context: BotContext) -> None:
    user_id = event.sender_id
    await _cleanup_session(context, user_id)
    await event.respond("Авторизация отменена.", buttons=build_main_menu_keyboard())


async def _finalize_login(
    context: BotContext,
    *,
    user_id: int,
    phone: str | None,
    session_client: TelegramClient,
    send_message: SendMessageFn,
) -> None:
    try:
        me = await session_client.get_me()
    except Exception:
        logger.exception("Не удалось получить информацию о профиле после авторизации", extra={"user_id": user_id})
        await send_message(
            "Не удалось завершить авторизацию. Попробуйте снова или войдите через номер телефона.",
            build_main_menu_keyboard(),
        )
        await _cleanup_session(context, user_id, session_client=session_client)
        return

    resolved_phone = (phone or getattr(me, "phone", None) or "").strip()
    if not resolved_phone:
        logger.error("Не удалось получить номер телефона авторизованного аккаунта", extra={"user_id": user_id})
        await send_message(
            "Telegram не вернул номер телефона аккаунта. Попробуйте войти через номер телефона.",
            build_main_menu_keyboard(),
        )
        await _cleanup_session(context, user_id, session_client=session_client)
        return

    session_string = session_client.session.save()
    session_id = f"{user_id}:{me.id}"
    existing_session = await context.session_repository.get_by_session_id(session_id)

    session_model = TelethonSession(
        session_id=session_id,
        owner_id=user_id,
        owner_type=SessionOwnerType.USER,
        session_data=session_string,
        client_type="user",
        phone=resolved_phone,
        metadata={
            "username": me.username,
            "first_name": me.first_name,
            "last_name": me.last_name,
            "phone": resolved_phone,
            "telegram_user_id": me.id,
        },
    )

    is_new_account = existing_session is None or not existing_session.is_active

    try:
        await context.session_manager.persist_session(session_model)
    except Exception:
        logger.exception("Не удалось сохранить Telethon-сессию", extra={"user_id": user_id})
        await send_message(
            "Не удалось сохранить сессию. Попробуйте позже или повторите вход.",
            build_main_menu_keyboard(),
        )
        await _cleanup_session(context, user_id, session_client=session_client)
        return

    await _cleanup_session(context, user_id, session_client=session_client)

    name_parts = [part for part in (me.first_name, me.last_name) if part]
    display_name = " ".join(name_parts) if name_parts else me.username or "Пользователь"
    handle = f"@{me.username}" if me.username else "—"
    if is_new_account:
        message = f"✅ Аккаунт успешно подключен: {display_name} ({handle})"
        logger.info(
            "Аккаунт авторизован",
            extra={
                "user_id": user_id,
                "account_id": me.id,
                "account_display": display_name,
            },
        )
    else:
        account_ref = f"@{me.username}" if me.username else display_name
        message = f"Вы уже вошли в аккаунт {account_ref}.\nИспользуйте /accounts для управления."
        logger.debug(
            "Повторная авторизация выполнена",
            extra={
                "user_id": user_id,
                "account_id": me.id,
            },
        )

    await send_message(message, build_main_menu_keyboard())


def setup_account_commands(client, context: BotContext) -> None:
    """Register account management commands."""

    @client.on(events.NewMessage(pattern=ACCOUNTS_PATTERN))
    async def handle_accounts(event: NewMessage.Event) -> None:
        if not event.is_private:
            return

        user_id = event.sender_id
        try:
            sessions_ordered = await context.session_repository.list_sessions_for_owner(user_id)
        except Exception:
            logger.exception("Failed to load sessions for account overview", extra={"user_id": user_id})
            await event.respond(
                "Не удалось загрузить список аккаунтов. Попробуйте позже.",
                buttons=build_main_menu_keyboard(),
            )
            return

        if not sessions_ordered:
            await event.respond(
                "У вас пока нет подключённых аккаунтов. Используйте /login_phone, чтобы подключить первый аккаунт.",
                buttons=build_main_menu_keyboard(),
            )
            return

        cached_statuses, pending_sessions = await context.account_status_service.get_cached_snapshot(sessions_ordered)
        pending_ids = {session.session_id for session in pending_sessions}

        def _render_initial_line(session: TelethonSession) -> str:
            status = cached_statuses.get(session.session_id)
            if status is None:
                pending = session.session_id in pending_ids
            else:
                pending = False
            return _format_session_status(session, cached_statuses.get(session.session_id), pending)

        body = "\n".join(_render_initial_line(session) for session in sessions_ordered)
        pending_note = "\n\nОбновляем статусы аккаунтов..." if pending_ids else ""

        try:
            message = await event.respond(
                (
                    f"Подключённые аккаунты:\n{body}{pending_note}\n\n"
                    "Нажмите кнопку, чтобы отвязать аккаунт."
                ),
                buttons=_build_logout_buttons(sessions_ordered),
            )
        except Exception:
            logger.exception("Failed to send account status message", extra={"user_id": user_id})
            return

        prior_states = {session.session_id: session.is_active for session in sessions_ordered}

        async def refresh_and_update() -> None:
            try:
                results = await context.account_status_service.refresh_sessions(
                    sessions_ordered,
                    verify_dialog_access=True,
                    use_cache=False,
                )
            except Exception:
                logger.exception("Failed to refresh account statuses", extra={"user_id": user_id})
                return

            try:
                lines: list[str] = []
                any_inactive = False
                for session in sessions_ordered:
                    status = results.get(session.session_id)
                    lines.append(_format_session_status(session, status))
                    is_active = bool(status and status.active)
                    previous_active = prior_states.get(session.session_id)
                    if not is_active:
                        any_inactive = True
                    if previous_active == is_active:
                        continue
                    if is_active:
                        await context.auto_broadcast_service.mark_account_active(
                            session.session_id,
                            owner_id=session.owner_id,
                            metadata=session.metadata,
                        )
                    else:
                        await context.auto_broadcast_service.mark_account_inactive(
                            session.session_id,
                            owner_id=session.owner_id,
                            reason=(status.detail if status else "unknown"),
                            metadata=session.metadata,
                        )
            except Exception:
                logger.exception("Failed to persist refreshed account statuses", extra={"user_id": user_id})
                return

            note = ""
            if any_inactive:
                note = "\n\nНеактивные аккаунты требуют повторного входа через /login_phone или /login_qr."

            updated_body = "\n".join(lines)
            text = (
                f"Подключённые аккаунты:\n{updated_body}{note}\n\n"
                "Нажмите кнопку, чтобы отвязать аккаунт."
            )
            try:
                await message.edit(
                    text,
                    buttons=_build_logout_buttons(sessions_ordered),
                )
            except MessageNotModifiedError:
                logger.debug("Account status message unchanged", extra={"user_id": user_id})
            except Exception:
                logger.exception("Failed to edit account status message", extra={"user_id": user_id})

        asyncio.create_task(refresh_and_update())

    @client.on(events.NewMessage(pattern=LOGIN_PHONE_PATTERN))
    async def handle_login_phone(event: NewMessage.Event) -> None:
        if not event.is_private:
            return

        user_id = event.sender_id
        state = context.auth_manager.get(user_id)
        if state and state.step != AuthStep.IDLE:
            await event.respond(
                "Вы уже проходите авторизацию. Пожалуйста, завершите текущий шаг или нажмите «Отмена».",
                buttons=_build_single_button(CANCEL_LABEL),
            )
            return

        existing_sessions = list(
            await context.session_manager.get_active_sessions(user_id, verify_live=True)
        )
        intro = ""
        if existing_sessions:
            body = "\n".join(_format_session(session) for session in existing_sessions)
            intro = f"У вас уже подключены аккаунты:\n{body}\n\n"

        context.auth_manager.begin(user_id, step=AuthStep.WAITING_PHONE, last_message_id=event.id)
        logger.debug("Запущен процесс авторизации по номеру", extra={"user_id": user_id})
        await event.respond(
            f"{intro}Введите ваш номер телефона (в формате +79998887766):",
            buttons=_build_single_button(CANCEL_LABEL),
        )

    @client.on(events.NewMessage(pattern=LOGIN_QR_PATTERN))
    async def handle_login_qr(event: NewMessage.Event) -> None:
        if not event.is_private:
            return

        user_id = event.sender_id
        if context.auth_manager.has_active_flow(user_id):
            await event.respond(
                "Завершите текущую авторизацию или нажмите «Отмена», чтобы начать новую.",
                buttons=_build_single_button(CANCEL_LABEL),
            )
            return

        existing_sessions = list(
            await context.session_manager.get_active_sessions(user_id, verify_live=True)
        )

        temp_client: TelegramClient | None = None
        try:
            temp_client = await context.session_manager.create_temporary_client()
            ignored_ids = [
                metadata_id
                for metadata_id in (
                    (session.metadata or {}).get("telegram_user_id") for session in existing_sessions
                )
                if isinstance(metadata_id, int)
            ]
            qr_login = await temp_client.qr_login(ignored_ids=ignored_ids or None)
        except Exception:
            logger.exception("Не удалось подготовить авторизацию по QR", extra={"user_id": user_id})
            if temp_client is not None:
                with contextlib.suppress(Exception):
                    await context.session_manager.close_client(temp_client)
            await event.respond(
                "Не удалось создать QR-код. Попробуйте позже или используйте вход по номеру телефона.",
                buttons=build_main_menu_keyboard(),
            )
            return

        state = context.auth_manager.begin(user_id, step=AuthStep.WAITING_QR, last_message_id=event.id)
        state.client = temp_client
        state.qr_login = qr_login

        try:
            message = await _send_qr_via_event(event, user_id, qr_login, existing_sessions or None)
        except Exception:
            logger.exception("Не удалось отправить QR-код пользователю", extra={"user_id": user_id})
            await _cleanup_session(context, user_id, session_client=temp_client)
            await event.respond(
                "Не удалось отправить QR-код. Попробуйте снова или используйте вход по номеру телефона.",
                buttons=build_main_menu_keyboard(),
            )
            return

        context.auth_manager.update(user_id, last_message_id=message.id)
        task = asyncio.create_task(_wait_for_qr_authorization(client, context, user_id))
        context.auth_manager.update(user_id, qr_task=task)
        logger.debug("Запущен процесс авторизации по QR", extra={"user_id": user_id})

    @client.on(events.NewMessage(incoming=True, func=_expect_step(context, AuthStep.WAITING_PHONE)))
    async def handle_phone_number(event: NewMessage.Event) -> None:
        raw_text = (event.raw_text or "").strip()
        if raw_text.lower() == CANCEL_LABEL.lower():
            await _cancel_flow(event, context)
            return

        phone = raw_text.replace(" ", "")
        if not phone.startswith("+") or not phone[1:].isdigit():
            context.auth_manager.update(event.sender_id, last_message_id=event.id)
            await event.respond(
                "Неверный формат номера. Отправьте номер в формате +79998887766 или нажмите «Отмена».",
                buttons=_build_single_button(CANCEL_LABEL),
            )
            return

        temp_client: TelegramClient | None = None
        try:
            temp_client = await context.session_manager.create_temporary_client()
            sent_code = await temp_client.send_code_request(phone)
        except PhoneNumberInvalidError:
            logger.warning("Получен некорректный номер при авторизации", extra={"user_id": event.sender_id})
            if temp_client:
                await context.session_manager.close_client(temp_client)
            context.auth_manager.update(event.sender_id, last_message_id=event.id)
            await event.respond(
                "Telegram отклонил номер. Проверьте формат и попробуйте снова.",
                buttons=_build_single_button(CANCEL_LABEL),
            )
            return
        except PhoneNumberBannedError:
            logger.error("Номер заблокирован Telegram", extra={"user_id": event.sender_id})
            if temp_client:
                await context.session_manager.close_client(temp_client)
            context.auth_manager.clear(event.sender_id)
            await event.respond(
                "Этот номер заблокирован Telegram. Попробуйте другой номер или обратитесь в поддержку Telegram.",
                buttons=build_main_menu_keyboard(),
            )
            return
        except Exception:
            logger.exception("Ошибка при отправке кода авторизации", extra={"user_id": event.sender_id})
            if temp_client:
                await context.session_manager.close_client(temp_client)
            context.auth_manager.update(event.sender_id, last_message_id=event.id)
            await event.respond(
                "Не удалось отправить код. Попробуйте позже или нажмите «Отмена».",
                buttons=_build_single_button(CANCEL_LABEL),
            )
            return

        context.auth_manager.update(
            event.sender_id,
            step=AuthStep.WAITING_CODE,
            phone=phone,
            phone_code_hash=sent_code.phone_code_hash,
            client=temp_client,
            last_message_id=event.id,
        )

        await event.respond(
            "Введите код, который пришёл в Telegram:",
            buttons=_build_single_button(CANCEL_LABEL),
        )

    @client.on(events.NewMessage(incoming=True, func=_expect_step(context, AuthStep.WAITING_CODE)))
    async def handle_phone_code(event: NewMessage.Event) -> None:
        raw_text = (event.raw_text or "").strip()
        if raw_text.lower() == CANCEL_LABEL.lower():
            await _cancel_flow(event, context)
            return

        state = context.auth_manager.get(event.sender_id)
        if state is None:
            return

        if state.client is None or state.phone is None:
            logger.error("Состояние авторизации повреждено", extra={"user_id": event.sender_id})
            await event.respond(
                "Текущая попытка авторизации недоступна. Попробуйте начать заново командой /login_phone.",
                buttons=build_main_menu_keyboard(),
            )
            context.auth_manager.clear(event.sender_id)
            return

        code = raw_text.replace(" ", "")
        context.auth_manager.update(event.sender_id, last_message_id=event.id)

        try:
            await state.client.sign_in(
                phone=state.phone,
                code=code,
                phone_code_hash=state.phone_code_hash,
            )
        except PhoneCodeInvalidError:
            logger.warning("Пользователь ввёл неверный код", extra={"user_id": event.sender_id})
            await event.respond(
                "Код неверный. Проверьте сообщение Telegram и введите код ещё раз.",
                buttons=_build_single_button(CANCEL_LABEL),
            )
            return
        except PhoneCodeExpiredError:
            logger.warning("Код авторизации истёк", extra={"user_id": event.sender_id})
            await event.respond(
                "Срок действия кода истёк. Отправьте /login_phone, чтобы получить новый код.",
                buttons=build_main_menu_keyboard(),
            )
            await context.session_manager.close_client(state.client)
            context.auth_manager.clear(event.sender_id)
            return
        except SessionPasswordNeededError:
            context.auth_manager.update(event.sender_id, step=AuthStep.WAITING_PASSWORD, last_message_id=event.id)
            await event.respond(
                "Введите пароль двухфакторной аутентификации:",
                buttons=_build_single_button(CANCEL_LABEL),
            )
            return
        except PhoneNumberBannedError:
            logger.error("Номер заблокирован при подтверждении кода", extra={"user_id": event.sender_id})
            await event.respond(
                "Этот номер заблокирован Telegram. Попробуйте другой номер.",
                buttons=build_main_menu_keyboard(),
            )
            await context.session_manager.close_client(state.client)
            context.auth_manager.clear(event.sender_id)
            return
        except Exception:
            logger.exception("Ошибка при подтверждении кода", extra={"user_id": event.sender_id})
            await event.respond(
                "Не удалось подтвердить код. Попробуйте снова начать авторизацию командой /login_phone.",
                buttons=build_main_menu_keyboard(),
            )
            await context.session_manager.close_client(state.client)
            context.auth_manager.clear(event.sender_id)
            return

        await _finalize_login(
            context,
            user_id=event.sender_id,
            phone=state.phone,
            session_client=state.client,
            send_message=lambda text, buttons: event.respond(text, buttons=buttons),
        )

    @client.on(events.NewMessage(incoming=True, func=_expect_step(context, AuthStep.WAITING_QR)))
    async def handle_qr_text_controls(event: NewMessage.Event) -> None:
        raw_text = (event.raw_text or "").strip()
        if raw_text.lower() == CANCEL_LABEL.lower():
            await _cancel_flow(event, context)
            return

        context.auth_manager.update(event.sender_id, last_message_id=event.id)
        await event.respond(
            "Используйте кнопку «Обновить QR», чтобы выпустить новый код, или отправьте «Отмена», чтобы прервать процесс.",
            buttons=_build_qr_buttons(event.sender_id),
        )

    @client.on(events.NewMessage(incoming=True, func=_expect_step(context, AuthStep.WAITING_PASSWORD)))
    async def handle_password(event: NewMessage.Event) -> None:
        raw_text = (event.raw_text or "").strip()
        if raw_text.lower() == CANCEL_LABEL.lower():
            await _cancel_flow(event, context)
            return

        state = context.auth_manager.get(event.sender_id)
        if state is None:
            return

        if state.client is None or state.phone is None:
            logger.error("Состояние авторизации повреждено (пароль)", extra={"user_id": event.sender_id})
            await event.respond(
                "Не удалось продолжить авторизацию. Начните заново командой /login_phone.",
                buttons=build_main_menu_keyboard(),
            )
            context.auth_manager.clear(event.sender_id)
            return

        context.auth_manager.update(event.sender_id, last_message_id=event.id)

        try:
            await state.client.sign_in(password=raw_text)
        except PasswordHashInvalidError:
            logger.warning("Пользователь ввёл неверный 2FA пароль", extra={"user_id": event.sender_id})
            await event.respond(
                "Пароль двухфакторной аутентификации неверный. Попробуйте ещё раз или нажмите «Отмена».",
                buttons=_build_single_button(CANCEL_LABEL),
            )
            return
        except Exception:
            logger.exception("Ошибка при вводе 2FA пароля", extra={"user_id": event.sender_id})
            await event.respond(
                "Не удалось подтвердить пароль. Начните вход заново командой /login_phone.",
                buttons=build_main_menu_keyboard(),
            )
            await context.session_manager.close_client(state.client)
            context.auth_manager.clear(event.sender_id)
            return

        await _finalize_login(
            context,
            user_id=event.sender_id,
            phone=state.phone,
            session_client=state.client,
            send_message=lambda text, buttons: event.respond(text, buttons=buttons),
        )

    @client.on(events.CallbackQuery(pattern=QR_REFRESH_PATTERN))
    async def handle_qr_refresh(event: events.CallbackQuery.Event) -> None:
        user_id = event.sender_id
        payload = _extract_callback_payload(event.data, QR_REFRESH_PREFIX)
        if payload is None or payload != str(user_id):
            await event.answer("Некорректный запрос.", alert=True)
            return

        state = context.auth_manager.get(user_id)
        if (
            state is None
            or state.step != AuthStep.WAITING_QR
            or state.qr_login is None
            or state.client is None
        ):
            await event.answer("Активная авторизация не найдена.", alert=True)
            return

        _cancel_qr_task(state)
        try:
            await state.qr_login.recreate()
        except Exception:
            logger.exception("Не удалось обновить QR-код", extra={"user_id": user_id})
            await event.answer("Не удалось обновить QR-код. Попробуйте позже.", alert=True)
            return

        try:
            sessions = list(
                await context.session_manager.get_active_sessions(user_id, verify_live=True)
            )
            message = await _send_qr_via_client(client, user_id, state.qr_login, sessions or None)
        except Exception:
            logger.exception("Не удалось отправить новый QR-код", extra={"user_id": user_id})
            await event.answer("Ошибка при отправке QR-кода. Попробуйте позже.", alert=True)
            return

        context.auth_manager.update(user_id, step=AuthStep.WAITING_QR, last_message_id=message.id)
        task = asyncio.create_task(_wait_for_qr_authorization(client, context, user_id))
        context.auth_manager.update(user_id, qr_task=task)

        await event.answer("Новый QR-код отправлен. Сканируйте его в Telegram.")
        with contextlib.suppress(Exception):
            await event.message.delete()

    @client.on(events.CallbackQuery(pattern=QR_CANCEL_PATTERN))
    async def handle_qr_cancel(event: events.CallbackQuery.Event) -> None:
        user_id = event.sender_id
        payload = _extract_callback_payload(event.data, QR_CANCEL_PREFIX)
        if payload is None or payload != str(user_id):
            await event.answer("Некорректный запрос.", alert=True)
            return

        state = context.auth_manager.get(user_id)
        if state is None or state.step != AuthStep.WAITING_QR:
            await event.answer("Активная авторизация не найдена.", alert=True)
            with contextlib.suppress(Exception):
                await event.message.delete()
            return

        await _cleanup_session(context, user_id)
        await event.answer("Авторизация отменена.")
        await client.send_message(user_id, "Авторизация отменена.", buttons=build_main_menu_keyboard())
        with contextlib.suppress(Exception):
            await event.message.delete()

    @client.on(events.CallbackQuery(pattern=b"^logout_req:"))
    async def handle_logout_request(event: events.CallbackQuery.Event) -> None:
        user_id = event.sender_id
        if context.auth_manager.has_active_flow(user_id):
            await event.answer("Сначала завершите текущую авторизацию.", alert=True)
            return
        session_id = _extract_callback_payload(event.data, "logout_req")
        if not session_id:
            await event.answer("Некорректный запрос.", alert=True)
            return
        session = await context.session_repository.get_by_session_id(session_id)
        if session is None or session.owner_id != user_id:
            await event.answer("Сессия не найдена.", alert=True)
            return

        target = _render_account_target(session)

        await event.edit(
            f"Вы действительно хотите отвязать аккаунт {target}?",
            buttons=[
                [
                    Button.inline("✅ Да", f"logout_yes:{session.session_id}".encode("utf-8")),
                    Button.inline("❌ Отмена", f"logout_cancel:{session.session_id}".encode("utf-8")),
                ]
            ],
        )

    @client.on(events.CallbackQuery(pattern=b"^logout_yes:"))
    async def handle_logout_confirm(event: events.CallbackQuery.Event) -> None:
        user_id = event.sender_id
        session_id = _extract_callback_payload(event.data, "logout_yes")
        if not session_id:
            await event.answer("Некорректный запрос.", alert=True)
            return
        session = await context.session_repository.get_by_session_id(session_id)
        if session is None or session.owner_id != user_id:
            await event.answer("Сессия не найдена.", alert=True)
            return

        try:
            removed = await context.session_manager.remove_session(session)
        except Exception:
            logger.exception(
                "Ошибка при удалении пользовательской сессии",
                extra={"user_id": user_id, "session_id": session.session_id},
            )
            await event.answer("Не удалось отвязать аккаунт. Попробуйте позже.", alert=True)
            return

        if not removed:
            await event.answer("Аккаунт уже был отключён.", alert=True)
            # Continue to refresh the list for пользовательского удобства.

        target = _render_account_target(session)

        remaining = list(
            await context.session_manager.get_active_sessions(user_id, verify_live=True)
        )
        if removed:
            await event.answer("Аккаунт отключён.")

        status_header = (
            f"✅ Аккаунт {target} отключён."
            if removed
            else f"Аккаунт {target} уже был отключён ранее."
        )

        if remaining:
            body = "\n".join(_format_session(item) for item in remaining)
            await event.edit(
                (
                    f"{status_header}\n\nПодключённые аккаунты:\n{body}\n\n"
                    "Чтобы отвязать другой аккаунт, выберите его ниже."
                ),
                buttons=_build_logout_buttons(remaining),
            )
        else:
            await event.edit(
                f"{status_header}\n\nПодключите новый аккаунт через /login_phone.",
            )

    @client.on(events.CallbackQuery(pattern=b"^logout_cancel:"))
    async def handle_logout_cancel(event: events.CallbackQuery.Event) -> None:
        user_id = event.sender_id
        session_id = _extract_callback_payload(event.data, "logout_cancel")
        if not session_id:
            await event.answer("Некорректный запрос.", alert=True)
            return
        # Even if session is missing (e.g. removed elsewhere), fall back to fresh list.
        remaining = list(
            await context.session_manager.get_active_sessions(user_id, verify_live=True)
        )
        await event.answer("Удаление отменено.")

        if remaining:
            body = "\n".join(_format_session(item) for item in remaining)
            await event.edit(
                f"Подключённые аккаунты:\n{body}\n\nНажмите кнопку, чтобы отвязать аккаунт.",
                buttons=_build_logout_buttons(remaining),
            )
        else:
            await event.edit(
                "У вас пока нет подключённых аккаунтов. Используйте /login_phone, чтобы подключить первый аккаунт.",
            )
