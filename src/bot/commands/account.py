from __future__ import annotations

import logging

from telethon import Button, TelegramClient, events
from telethon.events import NewMessage
from telethon.errors import (
    PasswordHashInvalidError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PhoneNumberBannedError,
    PhoneNumberInvalidError,
    SessionPasswordNeededError,
)

from src.bot.context import BotContext
from src.models.session import SessionOwnerType, TelethonSession
from src.services.auth_state import AuthStep


logger = logging.getLogger(__name__)

CANCEL_LABEL = "Отмена"
LOGIN_PHONE_LABEL = "Подключить аккаунт 📱"
LOGOUT_LABEL = "Выйти из аккаунта"


def _extract_session_id(data: bytes, prefix: str) -> str | None:
    try:
        payload = data.decode("utf-8")
    except UnicodeDecodeError:
        return None
    if not payload.startswith(prefix):
        return None
    return payload.split(":", maxsplit=1)[-1]


def _render_account_target(session: TelethonSession) -> str:
    phone = session.phone or "не указан"
    username = (session.metadata or {}).get("username")
    return f"@{username} ({phone})" if username else phone


def _format_session(session: TelethonSession) -> str:
    title = _render_account_target(session)
    status = "активен" if session.is_active else "неактивен"
    return f"• {title} ({status})"


def _build_single_button(label: str) -> list[list[Button]]:
    return [[Button.text(label, resize=True)]]


def _build_logout_buttons(sessions: list[TelethonSession]) -> list[list[Button]]:
    rows: list[list[Button]] = []
    for session in sessions:
        callback_data = f"logout_req:{session.session_id}".encode("utf-8")
        label = f"Удалить {_render_account_target(session)}"
        rows.append([Button.inline(label, callback_data)])
    return rows


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
    state = context.auth_manager.clear(user_id)
    if state and state.client is not None:
        try:
            await context.session_manager.close_client(state.client)
        except Exception:  # pragma: no cover - defensive logging only
            logger.exception("Не удалось корректно закрыть временный Telethon-клиент", extra={"user_id": user_id})
    await event.respond("Авторизация отменена.", buttons=Button.clear())


async def _finalize_login(
    event: NewMessage.Event,
    context: BotContext,
    *,
    phone: str,
    session_client: TelegramClient,
) -> None:
    phone = phone.strip()
    user_id = event.sender_id
    if not phone:
        logger.error("Получен пустой номер телефона при финализации авторизации", extra={"user_id": user_id})
        await event.respond(
            "Не удалось сохранить аккаунт: номер телефона не указан. Повторите вход.",
            buttons=Button.clear(),
        )
        await context.session_manager.close_client(session_client)
        context.auth_manager.clear(user_id)
        return
    me = await session_client.get_me()
    session_string = session_client.session.save()

    session_id = f"{user_id}:{me.id}"
    existing_session = await context.session_repository.get_by_session_id(session_id)

    session_model = TelethonSession(
        session_id=session_id,
        owner_id=user_id,
        owner_type=SessionOwnerType.USER,
        session_data=session_string,
        client_type="user",
        phone=phone,
        metadata={
            "username": me.username,
            "first_name": me.first_name,
            "last_name": me.last_name,
            "phone": phone,
            "telegram_user_id": me.id,
        },
    )

    is_new_account = existing_session is None or not existing_session.is_active

    try:
        await context.session_manager.persist_session(session_model)
    except Exception:
        logger.exception("Не удалось сохранить Telethon-сессию", extra={"user_id": user_id})
        await event.respond(
            "Не удалось сохранить сессию. Попробуйте позже или повторите вход.",
            buttons=Button.clear(),
        )
        await context.session_manager.close_client(session_client)
        context.auth_manager.clear(user_id)
        return

    await context.session_manager.close_client(session_client)
    context.auth_manager.clear(user_id)

    name_parts = [part for part in (me.first_name, me.last_name) if part]
    display_name = " ".join(name_parts) if name_parts else me.username or "Пользователь"
    handle = f"@{me.username}" if me.username else "—"
    if is_new_account:
        message = f"✅ Аккаунт успешно подключен: {display_name} ({handle})"
        logger.info(
            "Пользователь %s успешно авторизовал аккаунт", user_id, extra={"owner_id": user_id, "account_id": me.id}
        )
    else:
        if me.username:
            account_ref = f"@{me.username}"
        else:
            account_ref = display_name
        message = f"Вы уже вошли в аккаунт {account_ref}.\nИспользуйте /accounts для управления."
        logger.info(
            "Пользователь %s повторно авторизовал аккаунт", user_id, extra={"owner_id": user_id, "account_id": me.id}
        )
    await event.respond(message, buttons=Button.clear())


async def _prompt_logout_selection(event: NewMessage.Event, context: BotContext) -> None:
    sessions = list(await context.session_manager.get_active_sessions(event.sender_id))
    if not sessions:
        await event.respond(
            "У вас нет активных аккаунтов. Подключите новый через /login_phone.",
            buttons=_build_single_button(LOGIN_PHONE_LABEL),
        )
        return

    body = "\n".join(_format_session(session) for session in sessions)
    await event.respond(
        f"Выберите аккаунт, из которого нужно выйти:\n{body}\n\nНажмите кнопку напротив нужного аккаунта.",
        buttons=_build_logout_buttons(sessions),
    )


def setup_account_commands(client, context: BotContext) -> None:
    """Register account management commands."""

    @client.on(events.NewMessage(pattern=r"^/accounts(?:@\w+)?$"))
    async def handle_accounts(event: NewMessage.Event) -> None:
        if not event.is_private:
            return

        sessions = list(await context.session_manager.get_active_sessions(event.sender_id))
        if not sessions:
            await event.respond(
                "У вас пока нет подключённых аккаунтов. Используйте /login_phone, чтобы подключить первый аккаунт.",
            )
            return

        body = "\n".join(_format_session(session) for session in sessions)
        await event.respond(
            f"Подключённые аккаунты:\n{body}\n\nНажмите кнопку, чтобы отключить аккаунт.",
            buttons=_build_logout_buttons(sessions),
        )

    @client.on(events.NewMessage(pattern=rf"^(?:/logout(?:@\w+)?|{LOGOUT_LABEL})$"))
    async def handle_logout_command(event: NewMessage.Event) -> None:
        if not event.is_private:
            return

        if context.auth_manager.has_active_flow(event.sender_id):
            await event.respond(
                "Сначала завершите текущую авторизацию или нажмите «Отмена».",
                buttons=_build_single_button(CANCEL_LABEL),
            )
            return

        await _prompt_logout_selection(event, context)

    @client.on(events.NewMessage(pattern=rf"^(?:/login_phone(?:@\w+)?|{LOGIN_PHONE_LABEL})$"))
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

        existing_sessions = list(await context.session_manager.get_active_sessions(user_id))
        intro = ""
        if existing_sessions:
            body = "\n".join(_format_session(session) for session in existing_sessions)
            intro = f"У вас уже подключены аккаунты:\n{body}\n\n"

        context.auth_manager.begin(user_id, step=AuthStep.WAITING_PHONE, last_message_id=event.id)
        logger.info("Запущен процесс авторизации по номеру для пользователя %s", user_id)
        await event.respond(
            f"{intro}Введите ваш номер телефона (в формате +79998887766):",
            buttons=_build_single_button(CANCEL_LABEL),
        )

    @client.on(events.NewMessage(pattern=r"^/login_qr(?:@\w+)?$"))
    async def handle_login_qr(event: NewMessage.Event) -> None:
        if not event.is_private:
            return
        await event.respond(
            "Авторизация по QR-коду появится позже. Пока используйте вход по номеру телефона.",
            buttons=_build_single_button(LOGIN_PHONE_LABEL),
        )

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
                buttons=Button.clear(),
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
                buttons=Button.clear(),
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
                buttons=Button.clear(),
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
                buttons=Button.clear(),
            )
            await context.session_manager.close_client(state.client)
            context.auth_manager.clear(event.sender_id)
            return
        except Exception:
            logger.exception("Ошибка при подтверждении кода", extra={"user_id": event.sender_id})
            await event.respond(
                "Не удалось подтвердить код. Попробуйте снова начать авторизацию командой /login_phone.",
                buttons=Button.clear(),
            )
            await context.session_manager.close_client(state.client)
            context.auth_manager.clear(event.sender_id)
            return

        await _finalize_login(event, context, phone=state.phone, session_client=state.client)

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
                buttons=Button.clear(),
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
                buttons=Button.clear(),
            )
            await context.session_manager.close_client(state.client)
            context.auth_manager.clear(event.sender_id)
            return

        await _finalize_login(event, context, phone=state.phone, session_client=state.client)

    @client.on(events.CallbackQuery(pattern=b"^logout_req:"))
    async def handle_logout_request(event: events.CallbackQuery.Event) -> None:
        user_id = event.sender_id
        if context.auth_manager.has_active_flow(user_id):
            await event.answer("Сначала завершите текущую авторизацию.", alert=True)
            return

        session_id = _extract_session_id(event.data, "logout_req")
        if not session_id:
            await event.answer("Некорректный запрос.", alert=True)
            return
        session = await context.session_repository.get_by_session_id(session_id)
        if session is None or session.owner_id != user_id:
            await event.answer("Сессия не найдена.", alert=True)
            return

        target = _render_account_target(session)

        await event.edit(
            f"Вы действительно хотите удалить аккаунт {target}?",
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
        session_id = _extract_session_id(event.data, "logout_yes")
        if not session_id:
            await event.answer("Некорректный запрос.", alert=True)
            return
        session = await context.session_repository.get_by_session_id(session_id)
        if session is None or session.owner_id != user_id:
            await event.answer("Сессия не найдена.", alert=True)
            return

        removed = await context.session_manager.remove_session(session_id)
        if not removed:
            await event.answer("Не удалось отключить аккаунт.", alert=True)
            return

        target = _render_account_target(session)

        remaining = list(await context.session_manager.get_active_sessions(user_id))
        await event.answer("Аккаунт отключён.")

        if remaining:
            body = "\n".join(_format_session(item) for item in remaining)
            await event.edit(
                f"✅ Аккаунт {target} отключён.\n\nПодключённые аккаунты:\n{body}\n\nЧтобы отключить другой аккаунт, выберите его ниже.",
                buttons=_build_logout_buttons(remaining),
            )
        else:
            await event.edit(
                f"✅ Аккаунт {target} отключён.\n\nПодключите новый аккаунт через /login_phone.",
            )

    @client.on(events.CallbackQuery(pattern=b"^logout_cancel:"))
    async def handle_logout_cancel(event: events.CallbackQuery.Event) -> None:
        user_id = event.sender_id
        session_id = _extract_session_id(event.data, "logout_cancel")
        if not session_id:
            await event.answer("Некорректный запрос.", alert=True)
            return
        # Even if session is missing (e.g. removed elsewhere), fall back to fresh list.
        remaining = list(await context.session_manager.get_active_sessions(user_id))
        await event.answer("Удаление отменено.")

        if remaining:
            body = "\n".join(_format_session(item) for item in remaining)
            await event.edit(
                f"Подключённые аккаунты:\n{body}\n\nНажмите кнопку, чтобы отключить аккаунт.",
                buttons=_build_logout_buttons(remaining),
            )
        else:
            await event.edit(
                "У вас пока нет подключённых аккаунтов. Используйте /login_phone, чтобы подключить первый аккаунт.",
            )
