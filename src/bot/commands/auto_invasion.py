from __future__ import annotations

import io

from telethon import Button, events
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
        
        buttons = [
            [
                Button.inline(f"📥 Вступлено ({stats['joined']})", data=f"joined_{user_id}".encode()),
                Button.inline(f"⏳ Осталось ({stats['pending']})", data=f"pending_{user_id}".encode()),
            ]
        ]
        
        await event.respond(
            f"Статус: {status_text}\n"
            f"Всего групп: {stats['total']} | Вступлено: {stats['joined']} | Осталось: {stats['pending']}",
            buttons=buttons,
        )

    @client.on(events.CallbackQuery(data=lambda data: data.startswith(b"joined_")))
    async def handle_joined_groups(event: events.CallbackQuery.Event) -> None:
        if context.invasion_repository is None:
            await event.answer("❌ Ошибка: сервис не инициализирован.")
            return
        
        try:
            user_id = int(event.data.decode().split("_")[1])
            sender = await event.get_sender()
            
            if sender.id != user_id:
                await event.answer("❌ Доступ запрещен.")
                return
            
            groups = await context.invasion_repository.get_joined_groups(user_id)
            
            if not groups:
                await event.answer("📭 Нет вступленных групп.")
                return
            
            # Generate txt file content
            content = "Список успешно вступленных групп:\n\n"
            for idx, group in enumerate(groups, 1):
                content += f"{idx}. {group}\n"
            
            # Send as file
            file_bytes = io.BytesIO(content.encode("utf-8"))
            await event.client.send_file(
                event.chat_id,
                file_bytes,
                filename="joined_groups.txt",
                caption="Список вступленных групп",
            )
            await event.answer("✅ Файл отправлен.")
        except Exception as e:
            await event.answer(f"❌ Ошибка: {str(e)}")

    @client.on(events.CallbackQuery(data=lambda data: data.startswith(b"pending_")))
    async def handle_pending_groups(event: events.CallbackQuery.Event) -> None:
        if context.invasion_repository is None:
            await event.answer("❌ Ошибка: сервис не инициализирован.")
            return
        
        try:
            user_id = int(event.data.decode().split("_")[1])
            sender = await event.get_sender()
            
            if sender.id != user_id:
                await event.answer("❌ Доступ запрещен.")
                return
            
            groups = await context.invasion_repository.get_pending_groups(user_id)
            
            if not groups:
                await event.answer("🎉 Во все вступили")
                return
            
            # Generate txt file content
            content = "Список групп, в которые не удалось вступить:\n\n"
            for idx, group in enumerate(groups, 1):
                content += f"{idx}. {group}\n"
            
            # Send as file
            file_bytes = io.BytesIO(content.encode("utf-8"))
            await event.client.send_file(
                event.chat_id,
                file_bytes,
                filename="pending_groups.txt",
                caption="Список групп для повтора",
            )
            await event.answer("✅ Файл отправлен.")
        except Exception as e:
            await event.answer(f"❌ Ошибка: {str(e)}")


def set_worker_instance(worker: AutoInvasionWorker) -> None:
    global _worker_instance
    _worker_instance = worker
