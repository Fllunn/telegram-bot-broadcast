from __future__ import annotations

from telethon import events
from telethon.events import NewMessage

from src.bot.context import BotContext
from src.bot.keyboards import AUTO_STATUS_LABEL


def setup_help_command(client, context: BotContext) -> None:
    """Register the /help command handler."""

    @client.on(events.NewMessage(pattern=r"^/help(?:@\w+)?$"))
    async def handle_help(event: NewMessage.Event) -> None:
        await event.respond(
          (
              "Я помогу вам управлять рассылками и авторассылками.\n\n"
              "Что можно сделать:\n"
              "• /login_phone — войти в аккаунт по номеру телефона\n"
              "• /login_qr — войти через QR-код (работает только если выключена двухфакторная аутентификация)\n"
              "• /accounts — посмотреть ваши подключённые аккаунты\n"
              "• /add_text — добавить или обновить текст для рассылки\n"
              "• /add_image — добавить или обновить изображение для рассылки\n"
              "• /view_broadcast — увидеть текущий текст и изображение для рассылки\n"
              "• /upload_groups — загрузить список групп (.xlsx/.xls)\n"
              "• /view_groups — посмотреть сохранённые списки групп\n"
              "• /broadcast — запустить рассылку вручную\n\n"
              "Авторассылки:\n"
              "• Используйте кнопку «Авторассылка», чтобы создать периодическую рассылку\n"
              "• Кнопка «Остановить авторассылку» позволяет быстро остановить задачу без ввода ID\n"
              "• Кнопка «Статус авторассылки» показывает активные задачи и их состояние\n\n"
              "Подсказка:\n"
              "• Формат времени для интервалов — ЧЧ:ММ:СС (например, 00:05:00 = 5 минут)\n"
              "• Если аккаунт стал неактивным, бот уведомит и предложит войти снова\n"
              "• Если в загружаемом файле несколько групп с одинаковым именем на аккаунте, рассылка будет произведена во все такие группы\n\n"
              "Если нужна помощь — используйте /start или /help ещё раз."
          )
        )
