from __future__ import annotations

import contextlib
import logging
import math
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Mapping

from telethon import Button, events
from telethon.events import CallbackQuery, NewMessage

from src.bot.context import BotContext
from src.bot.keyboards import build_main_menu_keyboard
from src.models.auto_broadcast import AccountMode, AutoBroadcastTask, GroupTarget, TaskStatus
from src.services.auto_broadcast.engine import InvalidIntervalError
from src.services.auto_broadcast.payloads import extract_image_metadata
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
TASK_ACTION_CALLBACK = "auto_task_action"


@dataclass(frozen=True)
class TaskActionMeta:
    prompt: str
    empty_text: str
    success_text: str


TASK_ACTIONS: Dict[str, TaskActionMeta] = {
    "pause": TaskActionMeta(
        prompt="Выберите задачу, которую нужно поставить на паузу:",
        empty_text="Нет задач, которые можно поставить на паузу.",
        success_text="Задача поставлена на паузу.",
    ),
    "resume": TaskActionMeta(
        prompt="Выберите задачу, которую нужно возобновить:",
        empty_text="Нет задач, доступных для возобновления.",
        success_text="Задача возобновлена.",
    ),
    "stop": TaskActionMeta(
        prompt="Выберите задачу, которую нужно остановить:",
        empty_text="Нет задач, доступных для остановки.",
        success_text="Задача остановлена.",
    ),
    "notify_on": TaskActionMeta(
        prompt="Выберите задачу, для которой включить уведомления:",
        empty_text="Нет задач с отключёнными уведомлениями.",
        success_text="Уведомления включены.",
    ),
    "notify_off": TaskActionMeta(
        prompt="Выберите задачу, для которой отключить уведомления:",
        empty_text="Нет задач с включёнными уведомлениями.",
        success_text="Уведомления отключены.",
    ),
}

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
        fsm_step = AutoTaskSetupStep.CHOOSING_MODE
        has_materials = False

        for session in sessions:
            metadata = session.metadata or {}
            metadata_mapping: Mapping[str, object] = metadata if isinstance(metadata, Mapping) else {}
            raw_groups = metadata_mapping.get("broadcast_groups") if metadata_mapping else None

            if not raw_groups:
                logger.warning(
                    "Auto-task session metadata does not contain groups",
                    extra={
                        "user_id": event.sender_id,
                        "session_id": session.session_id,
                        "fsm_step": fsm_step.value,
                    },
                )

            raw_text = metadata_mapping.get("broadcast_text") if metadata_mapping else None
            session_has_materials = False
            if isinstance(raw_text, str) and raw_text.strip():
                has_materials = True
                session_has_materials = True
            elif extract_image_metadata(metadata_mapping):
                has_materials = True
                session_has_materials = True

            if not session_has_materials:
                logger.info(
                    "Auto-task session skipped due to missing materials",
                    extra={
                        "user_id": event.sender_id,
                        "session_id": session.session_id,
                    },
                )

            valid_targets = service.build_group_targets(raw_groups)
            targets: List[GroupTarget] = []
            for candidate in valid_targets:
                if isinstance(candidate.metadata, Mapping) and candidate.metadata.get("is_member") is False:
                    logger.warning(
                        "Skipping group for auto-task setup: no membership",
                        extra={
                            "user_id": event.sender_id,
                            "session_id": session.session_id,
                            "group_metadata": candidate.metadata,
                            "group_username": candidate.username,
                            "group_chat_id": candidate.chat_id,
                            "group_link": candidate.link,
                        },
                    )
                    continue
                if service.is_valid_group(candidate):
                    targets.append(candidate)
            if raw_groups and not targets:
                raw_count = None
                if isinstance(raw_groups, (list, tuple)):
                    raw_count = len(raw_groups)
                logger.warning(
                    "Auto-task session has raw groups but none passed validation",
                    extra={
                        "user_id": event.sender_id,
                        "session_id": session.session_id,
                        "raw_count": raw_count,
                    },
                )
            usable_targets = targets if session_has_materials else []
            counts[session.session_id] = len(usable_targets)
            for target in usable_targets:
                target.source_session_id = session.session_id
            account_groups[session.session_id] = usable_targets
            account_labels[session.session_id] = session.display_name()
        if not has_materials:
            logger.warning(
                "Auto-task setup aborted: no broadcast materials",
                extra={"user_id": event.sender_id, "fsm_step": fsm_step.value},
            )
            await event.respond(
                "Нет сохранённого текста или изображения для автозадачи. Добавьте материалы и попробуйте снова.",
                buttons=build_main_menu_keyboard(),
            )
            state_manager.clear(event.sender_id)
            return

        total_groups = sum(counts.values())
        if total_groups == 0:
            logger.warning(
                "Auto-task setup aborted due to empty group list",
                extra={"user_id": event.sender_id, "fsm_step": fsm_step.value},
            )
            await event.respond(
                "Нет доступных групп для автозадачи. Добавьте группы и попробуйте снова.",
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
                if total <= 0:
                    return None
                return float(total)
            except ValueError:
                return None
        else:
            try:
                candidate = normalized.replace(",", ".")
                value = float(candidate)
                if not math.isfinite(value) or value <= 0:
                    return None
                return value
            except ValueError:
                return None

    def _status_descriptor(status: TaskStatus) -> tuple[str, str]:
        mapping = {
            TaskStatus.RUNNING: ("▶️", "Запущена"),
            TaskStatus.PAUSED: ("⏸️", "На паузе"),
            TaskStatus.STOPPED: ("⏹️", "Остановлена"),
            TaskStatus.ERROR: ("⚠️", "Ошибка"),
        }
        return mapping.get(status, ("❓", status.value))

    def _short_account_id(account_id: Optional[str]) -> str:
        if not account_id:
            return "—"
        trimmed = account_id.strip()
        if len(trimmed) <= 6:
            return trimmed
        return f"ID {trimmed[:3]}…{trimmed[-2:]}"

    async def _build_account_label_map(
        user_id: int,
        tasks: Optional[List[AutoBroadcastTask]] = None,
    ) -> Dict[str, str]:
        sessions = await service.load_active_sessions(user_id, ensure_fresh_metadata=True)
        labels: Dict[str, str] = {session.session_id: session.display_name() for session in sessions}
        if tasks:
            for task in tasks:
                for account_id in task.account_ids or []:
                    labels.setdefault(account_id, _short_account_id(account_id))
                if task.account_id:
                    labels.setdefault(task.account_id, _short_account_id(task.account_id))
                if task.current_account_id:
                    labels.setdefault(task.current_account_id, _short_account_id(task.current_account_id))
        return labels

    def _format_account_list(task: AutoBroadcastTask, labels: Mapping[str, str]) -> str:
        if task.account_mode == AccountMode.SINGLE and task.account_id:
            account_ids = [task.account_id]
        else:
            account_ids = task.account_ids or []
        if not account_ids:
            return "—"
        names = [labels.get(account_id, _short_account_id(account_id)) for account_id in account_ids]
        if len(names) > 3:
            remaining = len(names) - 3
            base = ", ".join(names[:3])
            return f"{base} +{remaining}"
        return ", ".join(names)

    def _primary_account_label(task: AutoBroadcastTask, labels: Mapping[str, str]) -> str:
        if task.account_mode == AccountMode.SINGLE and task.account_id:
            return labels.get(task.account_id, _short_account_id(task.account_id))
        account_ids = task.account_ids or []
        if len(account_ids) == 1:
            account_id = account_ids[0]
            return labels.get(account_id, _short_account_id(account_id))
        if account_ids:
            return f"{len(account_ids)} акк."
        return "—"

    def _humanize_seconds(seconds: float) -> str:
        total = int(max(0, round(seconds)))
        if total <= 0:
            return "< 1 сек"
        if total < 60:
            return f"{total} сек"
        minutes, secs = divmod(total, 60)
        if minutes < 60:
            if secs and minutes < 10:
                return f"{minutes} мин {secs} сек"
            return f"{minutes} мин"
        hours, minutes = divmod(minutes, 60)
        if hours < 24:
            if minutes:
                return f"{hours} ч {minutes} мин"
            return f"{hours} ч"
        days, hours = divmod(hours, 24)
        if hours:
            return f"{days} дн {hours} ч"
        return f"{days} дн"

    def _humanize_next_run(next_run: Optional[datetime], *, with_exact: bool = True) -> str:
        if next_run is None:
            return "не запланирован" if with_exact else "—"
        now = datetime.utcnow()
        delta_seconds = (next_run - now).total_seconds()
        relative = "сейчас" if delta_seconds <= 0 else _humanize_seconds(delta_seconds)
        if not with_exact:
            return relative
        return f"{relative} ({next_run:%d.%m %H:%M})"

    def _format_task_progress(task: AutoBroadcastTask, labels: Mapping[str, str]) -> str:
        account_id = task.current_account_id
        if not account_id:
            return "ожидает запуска"
        account_label = labels.get(account_id, _short_account_id(account_id))
        groups = task.per_account_groups.get(account_id) or task.groups
        total_groups = len(groups)
        if total_groups <= 0:
            return f"{account_label}: нет групп"
        current_group = min(max(1, task.current_group_index + 1), total_groups)
        batch_size = max(1, task.batch_size)
        total_batches = max(1, math.ceil(total_groups / batch_size))
        current_batch = min(max(1, task.current_batch_index + 1), total_batches)
        return f"{account_label}: группа {current_group}/{total_groups}, батч {current_batch}/{total_batches}"

    def _format_task_summary(task: AutoBroadcastTask, labels: Mapping[str, str]) -> str:
        icon, status_text = _status_descriptor(task.status)
        mode_text = "все аккаунты" if task.account_mode == AccountMode.ALL else "один аккаунт"
        accounts_text = _format_account_list(task, labels)
        interval_text = service.humanize_interval(task.user_interval_seconds)
        next_run_text = _humanize_next_run(task.next_run_ts)
        progress_text = _format_task_progress(task, labels)
        notify_text = "включены" if task.notify_each_cycle else "выключены"
        stats_text = f"✅ {task.total_sent} • ⚠️ {task.total_failed}"
        return "\n".join(
            [
                f"{icon} {status_text} • {mode_text}",
                f"Аккаунты: {accounts_text}",
                f"Интервал: {interval_text}",
                f"Следующий запуск: {next_run_text}",
                f"Прогресс: {progress_text}",
                f"Уведомления: {notify_text}",
                f"Статистика: {stats_text}",
                f"ID: `{task.task_id}`",
            ]
        )

    def _format_task_preview(task: AutoBroadcastTask, labels: Mapping[str, str]) -> str:
        icon, status_text = _status_descriptor(task.status)
        mode_text = "Все аккаунты" if task.account_mode == AccountMode.ALL else "Один аккаунт"
        accounts_text = _format_account_list(task, labels)
        interval_text = service.humanize_interval(task.user_interval_seconds)
        next_run_text = _humanize_next_run(task.next_run_ts, with_exact=False)
        notify_icon = "🔔" if task.notify_each_cycle else "🔕"
        return (
            f"{icon} {mode_text} • {accounts_text}\n"
            f"   Интервал: {interval_text} • Следующий запуск: {next_run_text} • {notify_icon}"
        )

    def _build_task_button_label(task: AutoBroadcastTask, labels: Mapping[str, str]) -> str:
        icon, _ = _status_descriptor(task.status)
        primary = _primary_account_label(task, labels)
        next_run = _humanize_next_run(task.next_run_ts, with_exact=False)
        return f"{icon} {primary} • {next_run}"

    def _is_task_applicable(task: AutoBroadcastTask, action: str) -> bool:
        if action == "pause":
            return task.status == TaskStatus.RUNNING and task.enabled
        if action == "resume":
            return task.status in {TaskStatus.PAUSED, TaskStatus.ERROR}
        if action == "stop":
            return task.status != TaskStatus.STOPPED
        if action == "notify_on":
            return not task.notify_each_cycle
        if action == "notify_off":
            return task.notify_each_cycle
        return False

    async def _execute_task_action(user_id: int, action: str, task_id: str) -> Optional[AutoBroadcastTask]:
        current = await service.get_task(task_id)
        if current is None or current.user_id != user_id:
            return None
        if action == "pause":
            updated = await service.pause_task(task_id)
        elif action == "resume":
            updated = await service.resume_task(task_id)
        elif action == "stop":
            updated = await service.stop_task(task_id)
        elif action == "notify_on":
            updated = await service.toggle_notifications(task_id, True)
        elif action == "notify_off":
            updated = await service.toggle_notifications(task_id, False)
        else:
            return None
        return updated or await service.get_task(task_id)

    async def _show_task_action_menu(event: NewMessage.Event, action: str) -> None:
        meta = TASK_ACTIONS.get(action)
        if meta is None:
            return
        tasks = await service.list_tasks_for_user(event.sender_id)
        if not tasks:
            await event.respond("Активных автозадач не найдено.", buttons=build_main_menu_keyboard())
            return
        applicable = [task for task in tasks if _is_task_applicable(task, action)]
        if not applicable:
            await event.respond(meta.empty_text, buttons=build_main_menu_keyboard())
            return
        labels = await _build_account_label_map(event.sender_id, applicable)
        lines = [meta.prompt, "Нажмите на кнопку ниже, чтобы выбрать задачу:"]
        for idx, task in enumerate(applicable, start=1):
            lines.append(f"{idx}. {_format_task_preview(task, labels)}")
        buttons = [
            [
                Button.inline(
                    _build_task_button_label(task, labels),
                    f"{TASK_ACTION_CALLBACK}:{action}:{task.task_id}".encode("utf-8"),
                )
            ]
            for task in applicable
        ]
        buttons.append([Button.inline("Отмена", f"{TASK_ACTION_CALLBACK}:cancel".encode("utf-8"))])
        await event.respond("\n\n".join(lines), buttons=buttons)

    def _minimum_seconds_for_state(user_id: int, state: AutoTaskSetupState) -> float:
        account_ids = state.available_account_ids if state.account_mode == AccountMode.ALL else [state.selected_account_id]
        if not account_ids:
            return 0.0
        groups_map = {
            account_id: [GroupTarget.model_validate(group) if isinstance(group, dict) else group for group in state.account_groups.get(account_id, [])]
            for account_id in account_ids
        }
        batch_size = state.batch_size or service.default_batch_size
        return service.minimum_interval_seconds(groups_map, batch_size)

    async def _finalize_creation(event, state: AutoTaskSetupState) -> None:
        account_ids = state.available_account_ids if state.account_mode == AccountMode.ALL else [state.selected_account_id]
        try:
            task = await service.create_task(
                user_id=event.sender_id,
                account_mode=state.account_mode or AccountMode.ALL,
                session_ids=account_ids,
                user_interval_seconds=state.user_interval_seconds or 0,
                notify_each_cycle=state.notify_each_cycle,
                batch_size=state.batch_size or service.default_batch_size,
            )
        except InvalidIntervalError as exc:
            minimum = service.humanize_interval(exc.minimum_seconds)
            await event.respond(
                "Интервал слишком маленький. Минимально допустимое время — {0}.".format(minimum),
                buttons=build_main_menu_keyboard(),
            )
            return
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.exception("Не удалось создать автозадачу", exc_info=exc, extra={"user_id": event.sender_id})
            await event.respond(
                "Не удалось создать автозадачу: {0}".format(exc),
                buttons=build_main_menu_keyboard(),
            )
            return
        state_manager.clear(event.sender_id)
        labels = await _build_account_label_map(event.sender_id, [task])
        await event.respond(
            "Автозадача создана и запущена.\n{0}".format(_format_task_summary(task, labels)),
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
        sessions = await service.load_active_sessions(event.sender_id, ensure_fresh_metadata=True)
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

    @client.on(events.CallbackQuery(pattern=rf"^{TASK_ACTION_CALLBACK}:".encode("utf-8")))
    async def handle_task_action_callback(event: CallbackQuery.Event) -> None:
        payload = event.data.decode("utf-8", errors="ignore")
        parts = payload.split(":", maxsplit=2)
        if len(parts) < 2:
            await event.answer("Некорректный запрос.", alert=True)
            return
        action = parts[1]
        if action == "cancel":
            await event.answer("Отменено.")
            with contextlib.suppress(Exception):
                await event.edit("Действие отменено.", buttons=build_main_menu_keyboard())
            return
        meta = TASK_ACTIONS.get(action)
        if meta is None:
            await event.answer("Неизвестное действие.", alert=True)
            return
        if len(parts) < 3 or not parts[2]:
            await event.answer("Некорректный запрос.", alert=True)
            return
        task_id = parts[2]
        task = await _execute_task_action(event.sender_id, action, task_id)
        if task is None:
            await event.answer("Задача не найдена или недоступна.", alert=True)
            with contextlib.suppress(Exception):
                await event.edit("Задача не найдена или недоступна.", buttons=build_main_menu_keyboard())
            return
        await event.answer("Готово.")
        labels = await _build_account_label_map(event.sender_id, [task])
        summary = _format_task_summary(task, labels)
        with contextlib.suppress(Exception):
            await event.edit(f"{meta.success_text}\n\n{summary}", buttons=build_main_menu_keyboard())

    async def _handle_task_command(event: NewMessage.Event, action: str) -> None:
        if not event.is_private:
            return
        meta = TASK_ACTIONS.get(action)
        if meta is None:
            return
        task_id = _extract_task_id(event.raw_text or "")
        if task_id:
            task = await _execute_task_action(event.sender_id, action, task_id)
            if task is None:
                await event.respond("Задача не найдена или недоступна.", buttons=build_main_menu_keyboard())
                return
            labels = await _build_account_label_map(event.sender_id, [task])
            summary = _format_task_summary(task, labels)
            await event.respond(f"{meta.success_text}\n\n{summary}", buttons=build_main_menu_keyboard())
            return
        await _show_task_action_menu(event, action)

    @client.on(events.NewMessage(pattern=AUTO_STATUS_PATTERN))
    async def handle_status(event: NewMessage.Event) -> None:
        if not event.is_private:
            return
        tasks = await service.list_tasks_for_user(event.sender_id)
        if not tasks:
            await event.respond("Активные автозадачи не найдены.", buttons=build_main_menu_keyboard())
            return
        labels = await _build_account_label_map(event.sender_id, tasks)
        lines = ["Текущие автозадачи:"]
        for idx, task in enumerate(tasks, start=1):
            lines.append(f"{idx}.\n{_format_task_summary(task, labels)}")
        await event.respond("\n\n".join(lines), buttons=build_main_menu_keyboard())

    @client.on(events.NewMessage(pattern=AUTO_PAUSE_PATTERN))
    async def handle_pause(event: NewMessage.Event) -> None:
        await _handle_task_command(event, "pause")

    @client.on(events.NewMessage(pattern=AUTO_RESUME_PATTERN))
    async def handle_resume(event: NewMessage.Event) -> None:
        await _handle_task_command(event, "resume")

    @client.on(events.NewMessage(pattern=AUTO_STOP_PATTERN))
    async def handle_stop(event: NewMessage.Event) -> None:
        await _handle_task_command(event, "stop")

    @client.on(events.NewMessage(pattern=AUTO_NOTIFY_ON_PATTERN))
    async def handle_notify_on(event: NewMessage.Event) -> None:
        await _handle_task_command(event, "notify_on")

    @client.on(events.NewMessage(pattern=AUTO_NOTIFY_OFF_PATTERN))
    async def handle_notify_off(event: NewMessage.Event) -> None:
        await _handle_task_command(event, "notify_off")
