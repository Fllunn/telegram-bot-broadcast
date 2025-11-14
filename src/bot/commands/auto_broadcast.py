from __future__ import annotations

import contextlib
import logging
import re
from typing import Dict, List, Optional

from telethon import Button, events
from telethon.events import CallbackQuery, NewMessage

from src.bot.context import BotContext
from src.bot.keyboards import build_main_menu_keyboard
from src.models.auto_broadcast import AccountMode, GroupTarget, TaskStatus
from src.services.auto_broadcast.state_manager import (
    AutoTaskSetupState,
    AutoTaskSetupStep,
)


logger = logging.getLogger(__name__)

AUTO_SCHEDULE_PATTERN = r"^(?:/auto_schedule(?:@\w+)?|Автозадача)$"
AUTO_STATUS_PATTERN = r"^/auto_status(?:@\w+)?$"
AUTO_PAUSE_PATTERN = r"^/auto_pause(?:@\w+)?(\s+\S+)?$"
AUTO_RESUME_PATTERN = r"^/auto_resume(?:@\w+)?(\s+\S+)?$"
AUTO_STOP_PATTERN = r"^/auto_stop(?:@\w+)?(\s+\S+)?$"
AUTO_NOTIFY_ON_PATTERN = r"^/auto_notify_on(?:@\w+)?(\s+\S+)?$"
AUTO_NOTIFY_OFF_PATTERN = r"^/auto_notify_off(?:@\w+)?(\s+\S+)?$"

MODE_CALLBACK = "auto_mode"
SELECT_CALLBACK = "auto_select"
CONFIRM_CALLBACK = "auto_confirm"
NOTIFY_CALLBACK = "auto_notify"
CANCEL_CALLBACK = "auto_cancel"

INTERVAL_HELP = (
    "Укажите интервал между циклами рассылки. Можно вводить в секундах или в формате ЧЧ:ММ:СС.\n"
    "Интервал должен быть больше рассчётного минимума, чтобы сообщения не перекрывались."
)


def setup_auto_broadcast_commands(client, context: BotContext) -> None:
    service = context.auto_broadcast_service
    state_manager = service.state_manager

    async def _render_mode_prompt(event: NewMessage.Event, sessions) -> None:
        counts: Dict[str, int] = {}
        account_groups: Dict[str, List[GroupTarget]] = {}
        account_labels: Dict[str, str] = {}
        for session in sessions:
            metadata = session.metadata or {}
            raw_groups = metadata.get("broadcast_groups") if isinstance(metadata, dict) else []
            targets = [target for target in service.build_group_targets(raw_groups) if service.is_valid_group(target)]
            counts[session.session_id] = len(targets)
            for target in targets:
                target.source_session_id = session.session_id
            account_groups[session.session_id] = targets
            account_labels[session.session_id] = session.display_name()
        total_groups = sum(counts.values())
        if total_groups == 0:
            await event.respond(
                "Нет групп для рассылки ни в одном аккаунте. Загрузите группы и попробуйте снова.",
                buttons=build_main_menu_keyboard(),
            )
            state_manager.clear(event.sender_id)
            return
        state = state_manager.begin(
            event.sender_id,
            step=AutoTaskSetupStep.CHOOSING_MODE,
            available_account_ids=[session.session_id for session in sessions],
            per_account_group_counts=counts,
            account_labels=account_labels,
            account_groups=account_groups,
            total_groups=total_groups,
        )
        message = await event.respond(
            "Выберите режим автозадачи:\n"
            "• Один аккаунт — рассылка всегда от выбранного аккаунта.\n"
            "• Все аккаунты — перед каждым циклом порядок аккаунтов будет перемешан.",
            buttons=[
                [
                    Button.inline("Один аккаунт", f"{MODE_CALLBACK}:{AccountMode.SINGLE.value}".encode("utf-8")),
                    Button.inline("Все аккаунты", f"{MODE_CALLBACK}:{AccountMode.ALL.value}".encode("utf-8")),
                ],
                [Button.inline("Отмена", f"{CANCEL_CALLBACK}:mode".encode("utf-8"))],
            ],
        )
        state_manager.update(event.sender_id, last_message_id=message.id)

    def _parse_interval_seconds(text: str) -> Optional[float]:
        normalized = text.strip()
        if not normalized:
            return None
        if ":" in normalized:
            parts = normalized.split(":")
            if len(parts) == 3:
                hours, minutes, seconds = parts
            elif len(parts) == 2:
                hours = "0"
                minutes, seconds = parts
            else:
                return None
            try:
                total = int(hours) * 3600 + int(minutes) * 60 + int(seconds)
                return float(total)
            except ValueError:
                return None
        else:
            try:
                value = float(normalized)
                return value if value >= 0 else None
            except ValueError:
                return None

    def _format_task_summary(task) -> str:
        interval = service.humanize_interval(task.user_interval_seconds)
        status_map = {
            TaskStatus.RUNNING: "запущена",
            TaskStatus.PAUSED: "на паузе",
            TaskStatus.STOPPED: "остановлена",
            TaskStatus.ERROR: "ошибка",
        }
        next_run = task.next_run_ts.strftime("%d.%m %H:%M:%S") if task.next_run_ts else "не запланирован"
        return (
            f"Task ID: {task.task_id}\n"
            f"Режим: {'один аккаунт' if task.account_mode == AccountMode.SINGLE else 'все аккаунты'}\n"
            f"Статус: {status_map.get(task.status, task.status.value)}\n"
            f"Интервал: {interval}\n"
            f"Следующий запуск: {next_run}\n"
            f"Отправлено: {task.total_sent}, ошибок: {task.total_failed}"
        )

    def _minimum_seconds_for_state(user_id: int, state: AutoTaskSetupState) -> float:
        account_ids = state.available_account_ids if state.account_mode == AccountMode.ALL else [state.selected_account_id]
        if not account_ids:
            return 0.0
        groups_map = {
            account_id: [GroupTarget.model_validate(group) if isinstance(group, dict) else group for group in state.account_groups.get(account_id, [])]
            for account_id in account_ids
        }
        return service.minimum_interval_seconds(groups_map)

    async def _finalize_creation(event, state: AutoTaskSetupState) -> None:
        account_ids = state.available_account_ids if state.account_mode == AccountMode.ALL else [state.selected_account_id]
        try:
            task = await service.create_task(
                user_id=event.sender_id,
                account_mode=state.account_mode or AccountMode.ALL,
                session_ids=account_ids,
                user_interval_seconds=state.user_interval_seconds or 0,
                notify_each_cycle=state.notify_each_cycle,
                batch_size=state.batch_size,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.exception("Не удалось создать автозадачу", exc_info=exc, extra={"user_id": event.sender_id})
            await event.respond(
                "Не удалось создать автозадачу: {0}".format(exc),
                buttons=build_main_menu_keyboard(),
            )
            return
        state_manager.clear(event.sender_id)
        await event.respond(
            "Автозадача создана и запущена.\n{0}".format(_format_task_summary(task)),
            buttons=build_main_menu_keyboard(),
        )

    def _state_ready_for_confirmation(state: AutoTaskSetupState) -> bool:
        if state.account_mode == AccountMode.SINGLE and not state.selected_account_id:
            return False
        if state.user_interval_seconds is None:
            return False
        return True

    def _render_confirmation_text(state: AutoTaskSetupState) -> str:
        if state.account_mode == AccountMode.SINGLE:
            account_count = 1
            account_line = state.account_labels.get(state.selected_account_id or "", "не выбран")
        else:
            account_count = len(state.available_account_ids)
            account_line = f"{account_count} аккаунтов"
        notify_line = "Включены" if state.notify_each_cycle else "Выключены"
        interval = service.humanize_interval(state.user_interval_seconds or 0)
        return (
            "Проверьте параметры автозадачи:\n"
            f"Режим: {'все аккаунты' if state.account_mode == AccountMode.ALL else 'один аккаунт'}\n"
            f"Аккаунты: {account_line}\n"
            f"Интервал между циклами: {interval}\n"
            f"Уведомления: {notify_line}\n\n"
            "Нажмите 'Создать', чтобы запустить автозадачу."
        )

    async def _update_confirmation_message(event: CallbackQuery.Event, state: AutoTaskSetupState) -> None:
        text = _render_confirmation_text(state)
        buttons = [
            [Button.inline("✅ Создать", f"{CONFIRM_CALLBACK}:create".encode("utf-8"))],
            [Button.inline(
                f"🔔 Уведомления: {'ON' if state.notify_each_cycle else 'OFF'}",
                f"{NOTIFY_CALLBACK}:toggle".encode("utf-8"),
            )],
            [Button.inline("Отмена", f"{CANCEL_CALLBACK}:confirm".encode("utf-8"))],
        ]
        message = await event.edit(text, buttons=buttons)
        state_manager.update(event.sender_id, last_message_id=message.id)

    def _extract_task_id(message: str) -> Optional[str]:
        parts = message.strip().split()
        if len(parts) < 2:
            return None
        return parts[1].strip()

    @client.on(events.NewMessage(pattern=AUTO_SCHEDULE_PATTERN))
    async def handle_auto_schedule(event: NewMessage.Event) -> None:
        if not event.is_private:
            return
        state_manager.clear(event.sender_id)
        sessions = await service.load_active_sessions(event.sender_id)
        if not sessions:
            await event.respond(
                "Нет активных аккаунтов для настройки автозадачи. Подключите аккаунт и повторите.",
                buttons=build_main_menu_keyboard(),
            )
            return
        await _render_mode_prompt(event, sessions)

    @client.on(events.CallbackQuery(pattern=rf"^{MODE_CALLBACK}:".encode("utf-8")))
    async def handle_mode_selection(event: CallbackQuery.Event) -> None:
        state = state_manager.get(event.sender_id)
        if state is None or state.step != AutoTaskSetupStep.CHOOSING_MODE:
            await event.answer("Эта операция больше неактуальна.", alert=True)
            return
        payload = event.data.decode("utf-8", errors="ignore").split(":", maxsplit=1)[-1]
        try:
            mode = AccountMode(payload)
        except ValueError:
            await event.answer("Неизвестный режим.", alert=True)
            return
        state_manager.update(event.sender_id, account_mode=mode)
        if mode == AccountMode.SINGLE:
            buttons = []
            for account_id in state.available_account_ids:
                count = state.per_account_group_counts.get(account_id, 0)
                label_name = state.account_labels.get(account_id, account_id)
                label = f"{label_name} ({count} групп)"
                buttons.append([Button.inline(label, f"{SELECT_CALLBACK}:{account_id}".encode("utf-8"))])
            buttons.append([Button.inline("Отмена", f"{CANCEL_CALLBACK}:accounts".encode("utf-8"))])
            message = await event.edit("Выберите аккаунт для автозадачи:", buttons=buttons)
            state_manager.update(event.sender_id, step=AutoTaskSetupStep.CHOOSING_ACCOUNT, last_message_id=message.id)
        else:
            minimum = _minimum_seconds_for_state(event.sender_id, state)
            text = (
                "Вы выбрали режим для всех аккаунтов.\n"
                f"Всего аккаунтов: {len(state.available_account_ids)}\n"
                f"Минимальный интервал: {service.humanize_interval(minimum)}\n\n"
                f"{INTERVAL_HELP}"
            )
            message = await event.edit(text, buttons=[[Button.inline("Отмена", f"{CANCEL_CALLBACK}:interval".encode("utf-8"))]])
            state_manager.update(event.sender_id, step=AutoTaskSetupStep.ENTERING_INTERVAL, last_message_id=message.id)

    @client.on(events.CallbackQuery(pattern=rf"^{SELECT_CALLBACK}:".encode("utf-8")))
    async def handle_account_selection(event: CallbackQuery.Event) -> None:
        state = state_manager.get(event.sender_id)
        if state is None or state.step != AutoTaskSetupStep.CHOOSING_ACCOUNT:
            await event.answer("Эта операция больше неактуальна.", alert=True)
            return
        session_id = event.data.decode("utf-8", errors="ignore").split(":", maxsplit=1)[-1]
        if session_id not in state.available_account_ids:
            await event.answer("Некорректный выбор.", alert=True)
            return
        if state.per_account_group_counts.get(session_id, 0) == 0:
            await event.answer("Для аккаунта нет групп для рассылки.", alert=True)
            return
        state_manager.update(event.sender_id, selected_account_id=session_id)
        minimum = _minimum_seconds_for_state(event.sender_id, state_manager.get(event.sender_id))
        label_name = state.account_labels.get(session_id, session_id)
        text = (
            f"Выбран аккаунт {label_name}.\n"
            f"Минимальный интервал: {service.humanize_interval(minimum)}\n\n"
            f"{INTERVAL_HELP}"
        )
        message = await event.edit(text, buttons=[[Button.inline("Отмена", f"{CANCEL_CALLBACK}:interval".encode("utf-8"))]])
        state_manager.update(event.sender_id, step=AutoTaskSetupStep.ENTERING_INTERVAL, last_message_id=message.id)

    def _should_capture_interval(event: NewMessage.Event) -> bool:
        if not event.is_private or getattr(event.message, "out", False):
            return False
        state = state_manager.get(event.sender_id)
        return bool(state and state.step == AutoTaskSetupStep.ENTERING_INTERVAL)

    @client.on(events.NewMessage(func=_should_capture_interval))
    async def handle_interval_input(event: NewMessage.Event) -> None:
        state = state_manager.get(event.sender_id)
        if state is None:
            return
        seconds = _parse_interval_seconds(event.raw_text or "")
        if seconds is None or seconds <= 0:
            await event.respond("Некорректное значение. Укажите положительный интервал.")
            return
        minimum = _minimum_seconds_for_state(event.sender_id, state)
        if seconds <= minimum:
            await event.respond(
                "Минимально допустимое время — {0}. Укажите больше.".format(service.humanize_interval(minimum))
            )
            return
        state_manager.update(event.sender_id, user_interval_seconds=seconds, step=AutoTaskSetupStep.CONFIRMATION)
        notify_state = state_manager.get(event.sender_id)
        buttons = [
            [Button.inline("✅ Создать", f"{CONFIRM_CALLBACK}:create".encode("utf-8"))],
            [Button.inline(
                f"🔔 Уведомления: {'ON' if notify_state.notify_each_cycle else 'OFF'}",
                f"{NOTIFY_CALLBACK}:toggle".encode("utf-8"),
            )],
            [Button.inline("Отмена", f"{CANCEL_CALLBACK}:confirm".encode("utf-8"))],
        ]
        summary = _render_confirmation_text(notify_state)
        message = await event.respond(summary, buttons=buttons)
        state_manager.update(event.sender_id, last_message_id=message.id)

    @client.on(events.CallbackQuery(pattern=rf"^{NOTIFY_CALLBACK}:".encode("utf-8")))
    async def handle_notify_toggle(event: CallbackQuery.Event) -> None:
        state = state_manager.get(event.sender_id)
        if state is None or state.step != AutoTaskSetupStep.CONFIRMATION:
            await event.answer("Эта операция больше неактуальна.", alert=True)
            return
        state_manager.update(event.sender_id, notify_each_cycle=not state.notify_each_cycle)
        await event.answer("Готово.")
        await _update_confirmation_message(event, state_manager.get(event.sender_id))

    @client.on(events.CallbackQuery(pattern=rf"^{CONFIRM_CALLBACK}:".encode("utf-8")))
    async def handle_confirmation(event: CallbackQuery.Event) -> None:
        state = state_manager.get(event.sender_id)
        if state is None or state.step != AutoTaskSetupStep.CONFIRMATION:
            await event.answer("Эта операция больше неактуальна.", alert=True)
            return
        if not _state_ready_for_confirmation(state):
            await event.answer("Заполните все шаги.", alert=True)
            return
        await event.answer("Создаю задачу...")
        await _finalize_creation(event, state)

    @client.on(events.CallbackQuery(pattern=rf"^{CANCEL_CALLBACK}:".encode("utf-8")))
    async def handle_auto_cancel(event: CallbackQuery.Event) -> None:
        state = state_manager.clear(event.sender_id)
        await event.answer("Отменено.")
        with contextlib.suppress(Exception):
            await event.edit("Автозадача отменена.", buttons=build_main_menu_keyboard())

    async def _handle_task_command(event: NewMessage.Event, action: str) -> None:
        if not event.is_private:
            return
        task_id = _extract_task_id(event.raw_text or "")
        if not task_id:
            await event.respond("Укажите Task ID. Например: /{0} <task_id>".format(action), buttons=build_main_menu_keyboard())
            return
        if action == "auto_pause":
            task = await service.pause_task(task_id)
            message = "Задача поставлена на паузу." if task else "Задача не найдена."
        elif action == "auto_resume":
            task = await service.resume_task(task_id)
            message = "Задача возобновлена." if task else "Задача не найдена."
        elif action == "auto_stop":
            task = await service.stop_task(task_id)
            message = "Задача остановлена." if task else "Задача не найдена."
        elif action == "auto_notify_on":
            task = await service.toggle_notifications(task_id, True)
            message = "Уведомления включены." if task else "Задача не найдена."
        else:
            task = await service.toggle_notifications(task_id, False)
            message = "Уведомления отключены." if task else "Задача не найдена."
        await event.respond(message, buttons=build_main_menu_keyboard())

    @client.on(events.NewMessage(pattern=AUTO_STATUS_PATTERN))
    async def handle_status(event: NewMessage.Event) -> None:
        if not event.is_private:
            return
        tasks = await service.list_tasks_for_user(event.sender_id)
        if not tasks:
            await event.respond("Активные автозадачи не найдены.", buttons=build_main_menu_keyboard())
            return
        lines = ["Текущие автозадачи:"]
        for task in tasks:
            lines.append(_format_task_summary(task))
            lines.append("---")
        await event.respond("\n".join(lines), buttons=build_main_menu_keyboard())

    @client.on(events.NewMessage(pattern=AUTO_PAUSE_PATTERN))
    async def handle_pause(event: NewMessage.Event) -> None:
        await _handle_task_command(event, "auto_pause")

    @client.on(events.NewMessage(pattern=AUTO_RESUME_PATTERN))
    async def handle_resume(event: NewMessage.Event) -> None:
        await _handle_task_command(event, "auto_resume")

    @client.on(events.NewMessage(pattern=AUTO_STOP_PATTERN))
    async def handle_stop(event: NewMessage.Event) -> None:
        await _handle_task_command(event, "auto_stop")

    @client.on(events.NewMessage(pattern=AUTO_NOTIFY_ON_PATTERN))
    async def handle_notify_on(event: NewMessage.Event) -> None:
        await _handle_task_command(event, "auto_notify_on")

    @client.on(events.NewMessage(pattern=AUTO_NOTIFY_OFF_PATTERN))
    async def handle_notify_off(event: NewMessage.Event) -> None:
        await _handle_task_command(event, "auto_notify_off")
