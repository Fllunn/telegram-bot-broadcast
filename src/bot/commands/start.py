from __future__ import annotations

from telethon import events
from telethon.events import NewMessage

from src.bot.context import BotContext
from src.models.user import User
from src.bot.keyboards import AUTO_STATUS_LABEL, build_main_menu_keyboard


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
                "Привет! Я помогу вам управлять рассылками и авторассылками.\n\n"
                "Команды:\n"
                "• /login_phone — вход в аккаунт по номеру телефона\n"
                "• /login_qr — вход в аккаунт через QR-код (работает только если выключена двухфакторная аутентификация)\n"
                "• /accounts — список аккаунтов\n"
                "• /add_text, /add_image — добавление текста или изображения\n"
                "• /view_broadcast — просмотр текущей рассылки\n"
                "• /upload_groups — загрузка списков групп\n"
                "• /broadcast — запуск рассылки\n"
                "• /view_groups — сохранённые списки групп\n"
                "• /help — справка по командам\n\n"
                "Для авторассылок используйте кнопки:\n"
                "Авторассылка • Остановить авторассылку • Статус авторассылки"
            ),
            buttons=build_main_menu_keyboard(),
        )
