from __future__ import annotations

from telethon import events
from telethon.events import NewMessage

from src.bot.context import BotContext
from src.models.user import User
from src.bot.keyboards import build_main_menu_keyboard


def setup_start_command(client, context: BotContext) -> None:
    """Register the /start command handler."""

    @client.on(events.NewMessage(pattern=r"^/start(?:@\w+)?$"))
    async def handle_start(event: NewMessage.Event) -> None:
        sender = await event.get_sender()
        if sender is not None and not getattr(sender, "bot", False):
            user = User(
                telegram_id=sender.id,
                username=sender.username,
                first_name=sender.first_name,
                last_name=sender.last_name,
                language_code=getattr(sender, "lang_code", None),
            )
            await context.user_repository.upsert_user(user)

        await event.respond(
            (
                "Привет! Я помогу подключить и управлять несколькими аккаунтами."
                "\nИспользуйте кнопки ниже или команды /help, /login_phone, /login_qr, /accounts, /add_text."
            ),
            buttons=build_main_menu_keyboard(),
        )
