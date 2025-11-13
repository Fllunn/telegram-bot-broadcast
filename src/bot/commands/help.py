from __future__ import annotations

from telethon import events
from telethon.events import NewMessage

from src.bot.context import BotContext


def setup_help_command(client, context: BotContext) -> None:
    """Register the /help command handler."""

    @client.on(events.NewMessage(pattern=r"^/help(?:@\w+)?$"))
    async def handle_help(event: NewMessage.Event) -> None:
        await event.respond(
            (
                "Я помогу подключить несколько Telegram-аккаунтов.\n"
                "Доступные действия:\n"
                "1. /login_phone — войти по номеру телефона.\n"
                "2. /login_qr — войти по QR-коду (при включённой 2FA запросим пароль).\n"
                "3. /add_text — добавить или обновить текст для будущей рассылки.\n"
                "4. /add_image — добавить или обновить картинку для будущей рассылки.\n"
                "Команда /accounts покажет подключённые аккаунты."
            )
        )
