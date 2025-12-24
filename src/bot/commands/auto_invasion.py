from __future__ import annotations

from telethon import events
from telethon.events import NewMessage

from src.bot.context import BotContext
from src.services.auto_invasion.worker import AutoInvasionWorker


_worker_instance: AutoInvasionWorker = None


def setup_auto_invasion_commands(client, context: BotContext) -> None:
    global _worker_instance

    @client.on(events.NewMessage(pattern=r"^/auto_invasion(?:@\w+)?$"))
    async def handle_auto_invasion(event: NewMessage.Event) -> None:
        global _worker_instance
        
        if _worker_instance is None:
            await event.respond("Сервис автовступления не инициализирован.")
            return
        
        sender = await event.get_sender()
        if not sender or getattr(sender, "bot", False):
            await event.respond("❌ Ошибка: не удалось определить пользователя.")
            return
        
        user_id = sender.id
        is_active = await context.invasion_repository.is_active(user_id)
        
        if not is_active:
            await _worker_instance.activate(user_id)
            await event.respond("✅ Автовступление активировано.")
        else:
            await event.respond(
                "⚠️ Автовступление уже активно.\n\n"
                "Хотите отключить его? Отправьте /auto_invasion_stop"
            )

    @client.on(events.NewMessage(pattern=r"^/auto_invasion_stop(?:@\w+)?$"))
    async def handle_auto_invasion_stop(event: NewMessage.Event) -> None:
        global _worker_instance
        
        if _worker_instance is None:
            await event.respond("Сервис автовступления не инициализирован.")
            return
        
        sender = await event.get_sender()
        if not sender or getattr(sender, "bot", False):
            await event.respond("❌ Ошибка: не удалось определить пользователя.")
            return
        
        user_id = sender.id
        await _worker_instance.deactivate(user_id)
        await event.respond("🛑 Автовступление остановлено.")

    @client.on(events.NewMessage(pattern=r"^/auto_invasion_status(?:@\w+)?$"))
    async def handle_auto_invasion_status(event: NewMessage.Event) -> None:
        if context.invasion_repository is None:
            await event.respond("❌ Сервис автовступления не инициализирован.")
            return
        
        sender = await event.get_sender()
        if not sender or getattr(sender, "bot", False):
            await event.respond("❌ Ошибка: не удалось определить пользователя.")
            return
        
        user_id = sender.id
        is_active = await context.invasion_repository.is_active(user_id)
        stats = await context.invasion_repository.count_groups(user_id)
        
        status_text = "🟢 Активно" if is_active else "🔴 Остановлено"
        
        await event.respond(
            f"Статус: {status_text}\n"
            f"Всего групп: {stats['total']} | Вступлено: {stats['joined']} | Осталось: {stats['pending']}"
        )


def set_worker_instance(worker: AutoInvasionWorker) -> None:
    global _worker_instance
    _worker_instance = worker
