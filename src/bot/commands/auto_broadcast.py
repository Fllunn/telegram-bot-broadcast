from __future__ import annotations

import contextlib
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Mapping, Sequence

from telethon import Button, events
from telethon.events import CallbackQuery, NewMessage

from src.bot.context import BotContext
from src.bot.keyboards import AUTO_STATUS_LABEL, STOP_AUTO_LABEL, build_main_menu_keyboard
from src.config.broadcast_settings import (
    BROADCAST_BATCH_PAUSE_SECONDS,
    BROADCAST_DELAY_MAX_SECONDS,
    BROADCAST_DELAY_MIN_SECONDS,
)
from src.models.auto_broadcast import AccountMode, AutoBroadcastTask, GroupTarget, TaskStatus
from src.services.auto_broadcast.engine import AccountInUseError, InvalidIntervalError
from src.services.auto_broadcast.payloads import extract_image_metadata
from src.services.auto_broadcast.intervals import (
    MAX_INTERVAL_SECONDS,
    NORMALIZED_MAX_INTERVAL,
    IntervalValidationError,
    format_interval_hms,
    parse_interval_input,
)
from src.services.auto_broadcast.state_manager import (
    AutoTaskSetupState,
    AutoTaskSetupStep,
)
from src.services.broadcast_shared import (
    DialogsFetchError,
    collect_unique_target_peer_keys,
    deduplicate_broadcast_groups,
    describe_content_payload,
)
from src.utils.timezone import format_moscow_time


logger = logging.getLogger(__name__)

AUTO_SCHEDULE_PATTERN = r"^(?:/auto_schedule(?:@\w+)?|Авторассылка)$"
AUTO_STATUS_PATTERN = rf"^(?:/auto_status(?:@\w+)?|{re.escape(AUTO_STATUS_LABEL)})$"
AUTO_PAUSE_PATTERN = r"^/auto_pause(?:@\w+)?(\s+\S+)?$"
AUTO_RESUME_PATTERN = r"^/auto_resume(?:@\w+)?(\s+\S+)?$"
AUTO_STOP_PATTERN = r"^/auto_stop(?:@\w+)?(\s+\S+)?$"
AUTO_NOTIFY_ON_PATTERN = r"^/auto_notify_on(?:@\w+)?(\s+\S+)?$"
AUTO_NOTIFY_OFF_PATTERN = r"^/auto_notify_off(?:@\w+)?(\s+\S+)?$"
STOP_AUTO_PATTERN = rf"^(?:{re.escape(STOP_AUTO_LABEL)})$"

MODE_CALLBACK = "auto_mode"
SELECT_CALLBACK = "auto_select"
CONFIRM_CALLBACK = "auto_confirm"
NOTIFY_CALLBACK = "auto_notify"
CANCEL_CALLBACK = "auto_cancel"
TASK_ACTION_CALLBACK = "auto_task_action"
STOP_MENU_CALLBACK = "auto_stop_menu"
STOP_SELECT_CALLBACK = "auto_stop_select"

STOP_SINGLE_OPTION = "single"
STOP_ALL_OPTION = "all"

STOP_MENU_PROMPT = "Выберите, что остановить:\n• Только для этого аккаунта\n• Все авторассылки"
STOP_SINGLE_LABEL = "Только для этого аккаунта"
STOP_ALL_LABEL = "Все авторассылки"


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
    "Укажите интервал между циклами рассылки в формате ЧЧ:ММ:СС (например, 01:30:00).\n"
    f"Максимум — {NORMALIZED_MAX_INTERVAL} (7 дней). Интервал должен быть больше расчётного минимума, чтобы сообщения не перекрывались."
)


def setup_auto_broadcast_commands(client, context: BotContext) -> None:
    service = context.auto_broadcast_service
    state_manager = service.state_manager

    def _coerce_positive_int(value: object, *, default: int = 0) -> int:
        if value is None or isinstance(value, bool):
            return default
        try:
            number = int(value)
        except (TypeError, ValueError):
            return default
        return number if number > 0 else default

    def _extract_metadata_groups(metadata: Mapping[str, object]) -> List[dict[str, object]]:
        if not isinstance(metadata, Mapping):
            return []
        unique_source = metadata.get("broadcast_groups_unique")
        if isinstance(unique_source, list) and unique_source:
            prepared: List[dict[str, object]] = []
            for entry in unique_source:
                if isinstance(entry, Mapping):
                    prepared.append(dict(entry))
            if prepared:
                return prepared
        raw_source = metadata.get("broadcast_groups")
        raw_groups: List[Mapping[str, object]] = []
        if isinstance(raw_source, list) and raw_source:
            for entry in raw_source:
                if isinstance(entry, Mapping):
                    raw_groups.append(entry)
        if not raw_groups:
            return []
        deduplicated = deduplicate_broadcast_groups(raw_groups)
        return [dict(entry) for entry in deduplicated]

    def _build_stats_lines(actual: int, rows: int, unique: int) -> List[str]:
        lines: List[str] = [f"Будет отправлено в {actual} уникальные группы."]
        if rows:
            lines.append(f"Строк в файлах: {rows}.")
        if unique and unique != actual:
            lines.append(f"Уникальных записей в списке: {unique}.")
        return lines

    def _describe_materials_line(has_text: bool, has_image: bool) -> str:
        text_label = "есть" if has_text else "нет"
        image_label = "есть" if has_image else "нет"
        return f"Материалы: текст — {text_label}, картинка — {image_label}."

    def _format_duration(seconds: float) -> str:
        rounded = int(max(0, round(seconds)))
        if rounded <= 0:
            return "< 1 сек"
        hours, remainder = divmod(rounded, 3600)
        minutes, secs = divmod(remainder, 60)
        parts: List[str] = []
        if hours:
            parts.append(f"{hours} ч")
        if minutes:
            parts.append(f"{minutes} мин")
        if secs or not parts:
            parts.append(f"{secs} сек")
        return " ".join(parts)

    def _estimate_total_seconds(groups_count: int, batch_size: Optional[int] = None) -> float:
        if groups_count <= 0:
            return 0.0
        average_delay = (BROADCAST_DELAY_MIN_SECONDS + BROADCAST_DELAY_MAX_SECONDS) / 2
        total = groups_count * average_delay
        if groups_count > 0:
            effective_batch = max(1, int(batch_size or service.default_batch_size))
            batches = max(0, (groups_count - 1) // effective_batch)
            total += batches * BROADCAST_BATCH_PAUSE_SECONDS
        return total

    def _aggregate_account_stats(
        account_ids: Sequence[str],
        stats_map: Mapping[str, Mapping[str, object]],
    ) -> dict[str, object]:
        total_actual = 0
        total_rows = 0
        total_unique = 0
        any_text = False
        any_image = False
        seen: set[str] = set()
        for account_id in account_ids:
            if not account_id or account_id in seen:
                continue
            seen.add(account_id)
            stats = stats_map.get(account_id, {}) if isinstance(stats_map, Mapping) else {}
            actual_value = _coerce_positive_int(
                stats.get("actual_target_groups")
                or stats.get("actual_targets"),
                default=0,
            )
            rows_value = _coerce_positive_int(
                stats.get("rows_in_file")
                or stats.get("file_rows"),
                default=0,
            )
            unique_value = _coerce_positive_int(
                stats.get("unique_entries_in_file")
                or stats.get("unique_groups"),
                default=0,
            )
            total_actual += actual_value
            total_rows += rows_value
            total_unique += unique_value
            if bool(stats.get("has_text")):
                any_text = True
            if bool(stats.get("has_image")):
                any_image = True
        return {
            "actual": max(0, total_actual),
            "rows": max(0, total_rows),
            "unique": max(0, total_unique),
            "has_text": any_text,
            "has_image": any_image,
        }

    async def _calculate_actual_targets_for_session(
        session,
        groups: Sequence[Mapping[str, object]],
        *,
        user_id: int,
        account_label: str,
        fallback: int,
        content_type: Optional[str],
    ) -> int:
        if not groups:
            return 0
        session_client = None
        try:
            session_client = await context.session_manager.build_client_from_session(session)
            peer_keys = await collect_unique_target_peer_keys(
                session_client,
                groups,
                user_id=user_id,
                account_label=account_label,
                account_session_id=session.session_id,
                content_type=content_type,
            )
            return len(peer_keys)
        except DialogsFetchError as exc:
            logger.warning(
                "Не удалось проверить список чатов при подготовке авторассылки",
                extra={
                    "user_id": user_id,
                    "session_id": session.session_id,
                    "reason": exc.error_type,
                },
            )
            return fallback
        except Exception:
            logger.exception(
                "Не удалось рассчитать фактические целевые чаты для авторассылки",
                extra={"session_id": session.session_id, "user_id": user_id},
            )
            return fallback
        finally:
            if session_client is not None:
                try:
                    await context.session_manager.close_client(session_client)
                except Exception:
                    logger.exception(
                        "Не удалось закрыть клиент Telethon после расчёта целевых групп",
                        extra={"session_id": session.session_id},
                    )

    def _stop_menu_buttons() -> List[List[Button]]:
        return [
            [Button.inline(STOP_SINGLE_LABEL, f"{STOP_MENU_CALLBACK}:{STOP_SINGLE_OPTION}".encode("utf-8"))],
            [Button.inline(STOP_ALL_LABEL, f"{STOP_MENU_CALLBACK}:{STOP_ALL_OPTION}".encode("utf-8"))],
            [Button.inline("Отмена", f"{STOP_MENU_CALLBACK}:cancel".encode("utf-8"))],
        ]

    async def _render_mode_prompt(event: NewMessage.Event, sessions) -> None:
        counts: Dict[str, int] = {}
        account_groups: Dict[str, List[GroupTarget]] = {}
        account_labels: Dict[str, str] = {}
        account_stats: Dict[str, Dict[str, object]] = {}
        fsm_step = AutoTaskSetupStep.CHOOSING_MODE
        has_materials = False

        for session in sessions:
            metadata = session.metadata or {}
            metadata_mapping: Mapping[str, object] = metadata if isinstance(metadata, Mapping) else {}
            prepared_groups = _extract_metadata_groups(metadata_mapping)

            if not prepared_groups:
                logger.warning(
                    "Auto-task session metadata does not contain groups",
                    extra={
                        "user_id": event.sender_id,
                        "session_id": session.session_id,
                        "fsm_step": fsm_step.value,
                    },
                )

            raw_text = metadata_mapping.get("broadcast_text") if metadata_mapping else None
            text_value = None
            if isinstance(raw_text, str):
                text_value = raw_text.strip()
            elif raw_text is not None:
                text_value = str(raw_text).strip()
            has_text_value = bool(text_value)

            image_meta = extract_image_metadata(metadata_mapping)
            has_image_value = bool(image_meta)
            if image_meta and image_meta.get("legacy_file_id"):
                logger.warning(
                    "Сохранённая картинка устарела и будет пропущена",
                    extra={
                        "user_id": event.sender_id,
                        "session_id": session.session_id,
                    },
                )
                image_meta = None
                has_image_value = False

            session_has_materials = bool(has_text_value or has_image_value)
            if session_has_materials:
                has_materials = True
            else:
                logger.info(
                    "Auto-task session skipped due to missing materials",
                    extra={
                        "user_id": event.sender_id,
                        "session_id": session.session_id,
                    },
                )

            targets: List[GroupTarget] = []
            usable_group_entries: List[dict[str, object]] = []
            for entry in prepared_groups:
                normalized = service.build_group_targets([entry])
                if not normalized:
                    continue
                candidate = normalized[0]
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
                if not service.is_valid_group(candidate):
                    continue
                candidate.source_session_id = session.session_id
                targets.append(candidate)
                entry_copy = dict(entry)
                metadata_section = entry.get("metadata")
                if isinstance(metadata_section, Mapping):
                    entry_copy["metadata"] = dict(metadata_section)
                usable_group_entries.append(entry_copy)

            if prepared_groups and not targets:
                raw_count = len(prepared_groups)
                logger.warning(
                    "Auto-task session has raw groups but none passed validation",
                    extra={
                        "user_id": event.sender_id,
                        "session_id": session.session_id,
                        "raw_count": raw_count,
                    },
                )

            stats_payload = metadata_mapping.get("broadcast_groups_stats") if isinstance(metadata_mapping, Mapping) else None
            rows_from_stats = _coerce_positive_int(stats_payload.get("file_rows"), default=0) if isinstance(stats_payload, Mapping) else 0
            unique_from_stats = _coerce_positive_int(stats_payload.get("unique_groups"), default=0) if isinstance(stats_payload, Mapping) else 0
            actual_from_stats = _coerce_positive_int(stats_payload.get("actual_targets"), default=0) if isinstance(stats_payload, Mapping) else 0

            rows_from_occurrences = 0
            for entry in usable_group_entries:
                source_occurrences = _coerce_positive_int(entry.get("source_occurrences"), default=1)
                rows_from_occurrences += source_occurrences

            unique_for_account = len(usable_group_entries)
            rows_for_account = rows_from_stats or rows_from_occurrences or unique_for_account
            unique_entries = unique_from_stats or unique_for_account

            account_label_value = _session_account_label(session)
            content_type = describe_content_payload(has_text_value, has_image_value)
            actual_target_count = 0
            if session_has_materials and usable_group_entries:
                fallback_actual = actual_from_stats or unique_for_account
                actual_target_count = await _calculate_actual_targets_for_session(
                    session,
                    usable_group_entries,
                    user_id=event.sender_id,
                    account_label=account_label_value,
                    fallback=fallback_actual or unique_for_account,
                    content_type=content_type,
                )
                if fallback_actual:
                    actual_target_count = max(actual_target_count, fallback_actual)

            if unique_for_account and actual_target_count <= 0 and session_has_materials:
                actual_target_count = unique_for_account
            if session_has_materials:
                actual_target_count = max(actual_target_count, unique_for_account)

            usable_targets = targets if session_has_materials else []
            account_groups[session.session_id] = usable_targets if session_has_materials else []
            account_labels[session.session_id] = account_label_value
            account_stats[session.session_id] = {
                "rows_in_file": rows_for_account,
                "unique_entries_in_file": unique_entries,
                "actual_target_groups": actual_target_count,
                "has_text": has_text_value,
                "has_image": has_image_value,
                "file_rows": rows_for_account,
                "unique_groups": unique_for_account,
                "actual_targets": actual_target_count,
            }
            counts[session.session_id] = int(max(0, actual_target_count))
        if not has_materials:
            logger.warning(
                "Auto-task setup aborted: no broadcast materials",
                extra={"user_id": event.sender_id, "fsm_step": fsm_step.value},
            )
            await event.respond(
                "Нет сохранённого текста или изображения для авторассылки. Добавьте материалы и попробуйте снова.",
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
                "Нет доступных групп для авторассылки. Добавьте группы и попробуйте снова.",
                buttons=build_main_menu_keyboard(),
            )
            state_manager.clear(event.sender_id)
            return

        ordered_account_ids: List[str] = []
        for session in sessions:
            if counts.get(session.session_id, 0) > 0:
                ordered_account_ids.append(session.session_id)

        if not ordered_account_ids:
            logger.warning(
                "Auto-task setup aborted: no accounts with available targets",
                extra={"user_id": event.sender_id, "fsm_step": fsm_step.value},
            )
            await event.respond(
                "Нет доступных аккаунтов для авторассылки. Попробуйте обновить списки групп и материалы.",
                buttons=build_main_menu_keyboard(),
            )
            state_manager.clear(event.sender_id)
            return

        filtered_counts = {account_id: counts[account_id] for account_id in ordered_account_ids}
        filtered_labels = {account_id: account_labels[account_id] for account_id in ordered_account_ids}
        filtered_groups = {account_id: account_groups[account_id] for account_id in ordered_account_ids}
        filtered_stats = {account_id: account_stats[account_id] for account_id in ordered_account_ids}
        total_groups = sum(filtered_counts.values())

        state = state_manager.begin(
            event.sender_id,
            step=AutoTaskSetupStep.CHOOSING_MODE,
            available_account_ids=ordered_account_ids,
            per_account_group_counts=filtered_counts,
            account_labels=filtered_labels,
            account_groups=filtered_groups,
            account_group_stats=filtered_stats,
            total_groups=total_groups,
        )
        message = await event.respond(
            "Выберите режим авторассылки:\n"
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

    def _normalize_username_label(username: Optional[str]) -> Optional[str]:
        if not username:
            return None
        value = str(username).strip().lstrip("@")
        if not value:
            return None
        return f"@{value}"

    def _normalize_phone_label(phone: Optional[str]) -> Optional[str]:
        if not phone:
            return None
        digits = "".join(ch for ch in str(phone).strip() if ch not in {" ", "-", "(" , ")"})
        if not digits:
            return None
        normalized = digits if digits.startswith("+") else f"+{digits.lstrip('+')}"
        if len(normalized) < 4:
            return None
        return normalized

    def _session_account_label(session) -> str:
        metadata = session.metadata or {}
        username = metadata.get("username") if isinstance(metadata, Mapping) else None
        label = _normalize_username_label(username)
        if label:
            return f"{label} (неактивен)" if not getattr(session, "is_active", True) else label
        phone_source = getattr(session, "phone", None)
        if not phone_source and isinstance(metadata, Mapping):
            phone_source = metadata.get("phone")
        phone_label = _normalize_phone_label(phone_source)
        base_label = phone_label or "Аккаунт без данных"
        if not getattr(session, "is_active", True):
            return f"{base_label} (неактивен)"
        return base_label

    def _deduplicate_preserve_order(values: Sequence[str]) -> List[str]:
        seen: set[str] = set()
        result: List[str] = []
        for value in values:
            if not value or value in seen:
                continue
            seen.add(value)
            result.append(value)
        return result

    def _collect_task_account_ids(task: AutoBroadcastTask) -> List[str]:
        candidates: List[str] = []
        if task.account_mode == AccountMode.SINGLE and task.account_id:
            candidates.append(task.account_id)
        candidates.extend(task.account_ids or [])
        if task.current_account_id:
            candidates.append(task.current_account_id)
        return _deduplicate_preserve_order(candidates)

    def _build_stop_result_message(stopped: int, requested: int) -> Optional[str]:
        if stopped <= 0:
            return None
        if requested <= 1:
            return "Авторассылка остановлена."
        if stopped == requested:
            return "Авторассылка для всех аккаунтов остановлена."
        return f"Авторассылка остановлена для {stopped} из {requested} задач."

    async def _finalize_stop_callback(
        event: CallbackQuery.Event,
        *,
        message: str,
        edit_text: Optional[str] = "Готово.",
    ) -> None:
        if edit_text is not None:
            with contextlib.suppress(Exception):
                await event.edit(edit_text, buttons=None)
        await event.respond(message, buttons=build_main_menu_keyboard())

    async def _build_account_label_map(
        user_id: int,
        tasks: Optional[List[AutoBroadcastTask]] = None,
    ) -> Dict[str, str]:
        sessions = await service.load_active_sessions(user_id, ensure_fresh_metadata=True)
        labels: Dict[str, str] = {session.session_id: _session_account_label(session) for session in sessions}
        if tasks:
            required_ids: List[str] = []
            for task in tasks:
                required_ids.extend(_collect_task_account_ids(task))
            required_ids = _deduplicate_preserve_order(required_ids)
            missing_ids = [account_id for account_id in required_ids if account_id not in labels]
            if missing_ids:
                extra_sessions = await context.session_repository.get_by_session_ids(missing_ids)
                for session in extra_sessions:
                    if session.owner_id != user_id:
                        continue
                    labels[session.session_id] = _session_account_label(session)
            for account_id in required_ids:
                labels.setdefault(account_id, "Аккаунт недоступен")
        return labels

    def _format_account_list(task: AutoBroadcastTask, labels: Mapping[str, str]) -> str:
        account_ids = _collect_task_account_ids(task)
        if not account_ids:
            return "—"
        names = [labels.get(account_id, "Аккаунт недоступен") for account_id in account_ids]
        if len(names) > 3:
            remaining = len(names) - 3
            base = ", ".join(names[:3])
            return f"{base} +{remaining}"
        return ", ".join(names)

    def _primary_account_label(task: AutoBroadcastTask, labels: Mapping[str, str]) -> str:
        account_ids = _collect_task_account_ids(task)
        if not account_ids:
            return "—"
        primary_label = labels.get(account_ids[0], "Аккаунт недоступен")
        if len(account_ids) == 1:
            return primary_label
        return f"{primary_label} +{len(account_ids) - 1}"

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
        return format_moscow_time(next_run)

    def _format_next_run_compact(next_run: Optional[datetime]) -> str:
        return format_moscow_time(next_run)

    def _format_task_summary(task: AutoBroadcastTask, labels: Mapping[str, str]) -> str:
        icon, status_text = _status_descriptor(task.status)
        interval_hms = format_interval_hms(task.user_interval_seconds)
        interval_text = service.humanize_interval(task.user_interval_seconds)
        interval_label = f"Интервал: {interval_hms} ({interval_text})" if interval_hms != "—" else f"Интервал: {interval_text}"
        next_run_text = _format_next_run_compact(task.next_run_ts)
        account_ids = _collect_task_account_ids(task)
        account_labels = [labels.get(account_id, "Аккаунт недоступен") for account_id in account_ids]
        if not account_labels:
            account_line = "Аккаунт: недоступен"
        elif len(account_labels) == 1:
            account_line = f"Аккаунт: {account_labels[0]}"
        else:
            display = ", ".join(account_labels[:3])
            remaining = len(account_labels) - 3
            if remaining > 0:
                display = f"{display} +{remaining}"
            account_line = f"Аккаунты: {display}"
        stats_line = f"Статистика: {task.total_sent} отправлено, {task.total_failed} ошибок"
        return "\n".join(
            [
                f"{icon} {status_text}",
                account_line,
                interval_label,
                f"Следующий запуск: {next_run_text}",
                stats_line,
            ]
        )

    def _format_task_preview(task: AutoBroadcastTask, labels: Mapping[str, str]) -> str:
        icon, status_text = _status_descriptor(task.status)
        mode_text = "Все аккаунты" if task.account_mode == AccountMode.ALL else "Один аккаунт"
        accounts_text = _format_account_list(task, labels)
        interval_hms = format_interval_hms(task.user_interval_seconds)
        interval_text = service.humanize_interval(task.user_interval_seconds)
        interval_display = f"{interval_hms} ({interval_text})" if interval_hms != "—" else interval_text
        next_run_text = _humanize_next_run(task.next_run_ts, with_exact=False)
        notify_icon = "🔔" if task.notify_each_cycle else "🔕"
        return (
            f"{icon} {mode_text} • {accounts_text}\n"
            f"   Интервал: {interval_display} • Следующий запуск: {next_run_text} • {notify_icon}"
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
        if action == "notify_on":
            return not task.notify_each_cycle
        if action == "notify_off":
            return task.notify_each_cycle
        return False

    async def _execute_task_action(user_id: int, action: str, task_id: str) -> Optional[AutoBroadcastTask]:
        current = await service.get_task(task_id)
        if current is None or current.user_id != user_id:
            return None
        try:
            if action == "pause":
                updated = await service.pause_task(task_id)
            elif action == "resume":
                updated = await service.resume_task(task_id)
            elif action == "notify_on":
                updated = await service.toggle_notifications(task_id, True)
            elif action == "notify_off":
                updated = await service.toggle_notifications(task_id, False)
            else:
                return None
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.exception(
                "Не удалось обновить состояние авторассылки",
                extra={"task_id": task_id, "user_id": user_id, "action": action, "error": str(exc)},
            )
            return None
        return updated or await service.get_task(task_id)

    async def _show_task_action_menu(event: NewMessage.Event, action: str) -> None:
        meta = TASK_ACTIONS.get(action)
        if meta is None:
            return
        tasks = await service.list_tasks_for_user(event.sender_id)
        if not tasks:
            await event.respond("Нет активных авторассылок.", buttons=build_main_menu_keyboard())
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
        except AccountInUseError as exc:
            await event.respond(str(exc), buttons=build_main_menu_keyboard())
            return
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.exception("Не удалось создать авторассылку", exc_info=exc, extra={"user_id": event.sender_id})
            await event.respond(
                "Не удалось создать авторассылку: {0}".format(exc),
                buttons=build_main_menu_keyboard(),
            )
            return
        state_manager.clear(event.sender_id)
        labels = await _build_account_label_map(event.sender_id, [task])
        await event.respond(
            "Авторассылка создана и запущена.\n{0}".format(_format_task_summary(task, labels)),
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
            account_ids = [state.selected_account_id] if state.selected_account_id else []
        else:
            account_ids = list(state.available_account_ids)

        stats_map = state.account_group_stats if isinstance(state.account_group_stats, Mapping) else {}
        aggregated = _aggregate_account_stats(account_ids, stats_map)

        stats_lines = _build_stats_lines(
            int(aggregated.get("actual", 0)),
            int(aggregated.get("rows", 0)),
            int(aggregated.get("unique", 0)),
        )
        lines = list(stats_lines)

        if state.account_mode == AccountMode.SINGLE and account_ids:
            account_label = state.account_labels.get(account_ids[0] or "", "не выбран")
            lines.append(f"Выбранный аккаунт: {account_label}.")
        else:
            lines.append(f"Выбрано аккаунтов: {len(account_ids)}.")

        lines.append(
            _describe_materials_line(bool(aggregated.get("has_text")), bool(aggregated.get("has_image")))
        )
        lines.append("Рассылка будет проходить постепенно, с паузами для безопасности.")
        estimated = _format_duration(
            _estimate_total_seconds(
                int(aggregated.get("actual", 0)),
                batch_size=state.batch_size or service.default_batch_size,
            )
        )
        lines.append(f"Оценочное время: ≈ {estimated}.")
        lines.append(f"Режим: {'все аккаунты' if state.account_mode == AccountMode.ALL else 'один аккаунт'}.")

        interval_seconds = state.user_interval_seconds or 0
        normalized_interval = state.user_interval_text or format_interval_hms(interval_seconds)
        if interval_seconds > 0:
            humanized_interval = service.humanize_interval(interval_seconds)
            lines.append(f"Интервал между циклами: {normalized_interval} ({humanized_interval}).")
        else:
            lines.append(f"Интервал между циклами: {normalized_interval}.")

        notify_line = "Включены" if state.notify_each_cycle else "Выключены"
        lines.append(f"Уведомления: {notify_line}.")
        lines.extend(["", "Готовы начать?", "Нажмите 'Создать', чтобы запустить авторассылку."])
        return "\n".join(lines)

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

    @client.on(events.NewMessage(pattern=AUTO_SCHEDULE_PATTERN))
    async def handle_auto_schedule(event: NewMessage.Event) -> None:
        if not event.is_private:
            return
        state_manager.clear(event.sender_id)
        sessions = await service.load_active_sessions(event.sender_id, ensure_fresh_metadata=True)
        if not sessions:
            await event.respond(
                "Нет активных аккаунтов для настройки авторассылки. Подключите аккаунт и повторите.",
                buttons=build_main_menu_keyboard(),
            )
            return
        await _render_mode_prompt(event, sessions)

    @client.on(events.NewMessage(pattern=STOP_AUTO_PATTERN))
    async def handle_stop_autobroadcast(event: NewMessage.Event) -> None:
        if not event.is_private:
            return
        tasks = await service.list_tasks_for_user(event.sender_id, active_only=True)
        if not tasks:
            await event.respond("Нет активных авторассылок.", buttons=build_main_menu_keyboard())
            return
        if len(tasks) == 1:
            stopped, requested = await service.stop_tasks(
                user_id=event.sender_id,
                task_ids=[tasks[0].task_id],
            )
            if stopped <= 0:
                await event.respond("Нет активных авторассылок.", buttons=build_main_menu_keyboard())
                return
            message = _build_stop_result_message(stopped, requested) or "Авторассылка остановлена."
            await event.respond(message, buttons=build_main_menu_keyboard())
            return
        await event.respond(STOP_MENU_PROMPT, buttons=_stop_menu_buttons())

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
                label_name = state.account_labels.get(account_id, account_id)
                stats = state.account_group_stats.get(account_id, {}) if isinstance(state.account_group_stats, Mapping) else {}
                actual_count = _coerce_positive_int(stats.get("actual_target_groups") or stats.get("actual_targets"), default=0)
                rows_total = _coerce_positive_int(stats.get("rows_in_file") or stats.get("file_rows"), default=0)
                unique_total = _coerce_positive_int(stats.get("unique_entries_in_file") or stats.get("unique_groups"), default=0)
                label_parts = [f"{actual_count} уникальных чатов"]
                if rows_total:
                    label_parts.append(f"{rows_total} строк")
                elif unique_total:
                    label_parts.append(f"{unique_total} уникальных записей")
                label_stats = ", ".join(label_parts)
                label = f"{label_name} ({label_stats})"
                buttons.append([Button.inline(label, f"{SELECT_CALLBACK}:{account_id}".encode("utf-8"))])
            buttons.append([Button.inline("Отмена", f"{CANCEL_CALLBACK}:accounts".encode("utf-8"))])
            message = await event.edit("Выберите аккаунт для авторассылки:", buttons=buttons)
            state_manager.update(event.sender_id, step=AutoTaskSetupStep.CHOOSING_ACCOUNT, last_message_id=message.id)
        else:
            minimum = _minimum_seconds_for_state(event.sender_id, state)
            stats_map = state.account_group_stats if isinstance(state.account_group_stats, Mapping) else {}
            aggregated = _aggregate_account_stats(state.available_account_ids, stats_map)
            stats_lines = _build_stats_lines(
                int(aggregated.get("actual", 0)),
                int(aggregated.get("rows", 0)),
                int(aggregated.get("unique", 0)),
            )
            text_lines = list(stats_lines)
            text_lines.append(f"Выбрано аккаунтов: {len(state.available_account_ids)}.")
            text_lines.append(
                _describe_materials_line(bool(aggregated.get("has_text")), bool(aggregated.get("has_image")))
            )
            text_lines.append(f"Минимальный интервал: {service.humanize_interval(minimum)}")
            text_lines.extend(["", INTERVAL_HELP])
            text = "\n".join(text_lines)
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
        stats_map = state.account_group_stats if isinstance(state.account_group_stats, Mapping) else {}
        aggregated = _aggregate_account_stats([session_id], stats_map)
        stats_lines = _build_stats_lines(
            int(aggregated.get("actual", 0)),
            int(aggregated.get("rows", 0)),
            int(aggregated.get("unique", 0)),
        )
        summary_lines = list(stats_lines)
        summary_lines.append(f"Выбранный аккаунт: {label_name}.")
        summary_lines.append(
            _describe_materials_line(bool(aggregated.get("has_text")), bool(aggregated.get("has_image")))
        )
        summary_lines.append(f"Минимальный интервал: {service.humanize_interval(minimum)}")
        summary_lines.extend(["", INTERVAL_HELP])
        text = "\n".join(summary_lines)
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
        try:
            parsed = parse_interval_input(event.raw_text or "")
        except IntervalValidationError as exc:
            await event.respond(exc.user_message)
            return
        seconds = float(parsed.total_seconds)
        minimum = _minimum_seconds_for_state(event.sender_id, state)
        if minimum > MAX_INTERVAL_SECONDS:
            await event.respond(
                f"Минимальный безопасный интервал для выбранных параметров превышает лимит {NORMALIZED_MAX_INTERVAL}. "
                "Уменьшите количество групп или разделите рассылку на несколько задач."
            )
            return
        if seconds <= minimum:
            await event.respond(
                "Минимально допустимое время — {0}. Укажите больше.".format(service.humanize_interval(minimum))
            )
            return
        state_manager.update(
            event.sender_id,
            user_interval_seconds=seconds,
            user_interval_text=parsed.normalized_text,
            step=AutoTaskSetupStep.CONFIRMATION,
        )
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
        await event.answer("Создаю авторассылку...")
        await _finalize_creation(event, state)

    @client.on(events.CallbackQuery(pattern=rf"^{CANCEL_CALLBACK}:".encode("utf-8")))
    async def handle_auto_cancel(event: CallbackQuery.Event) -> None:
        state = state_manager.clear(event.sender_id)
        await event.answer("Отменено.")
        with contextlib.suppress(Exception):
            await event.edit("Авторассылка отменена.")
        await event.respond("Возвращаюсь в главное меню.", buttons=build_main_menu_keyboard())

    @client.on(events.CallbackQuery(pattern=rf"^{STOP_MENU_CALLBACK}:".encode("utf-8")))
    async def handle_stop_menu_callback(event: CallbackQuery.Event) -> None:
        payload = event.data.decode("utf-8", errors="ignore")
        parts = payload.split(":", maxsplit=2)
        option = parts[1] if len(parts) > 1 else ""
        if option == "cancel":
            await event.answer("Отменено.")
            with contextlib.suppress(Exception):
                await event.edit("Действие отменено.")
            await event.respond("Возвращаюсь в главное меню.", buttons=build_main_menu_keyboard())
            return
        if option == STOP_SINGLE_OPTION:
            tasks = await service.list_tasks_for_user(event.sender_id, active_only=True)
            if not tasks:
                await event.answer("Нет активных авторассылок.", alert=True)
                await _finalize_stop_callback(
                    event,
                    message="Нет активных авторассылок.",
                    edit_text="Нет активных авторассылок.",
                )
                return
            if len(tasks) == 1:
                await event.answer("Останавливаю…")
                stopped, requested = await service.stop_tasks(
                    user_id=event.sender_id,
                    task_ids=[tasks[0].task_id],
                )
                if stopped <= 0:
                    await _finalize_stop_callback(
                        event,
                        message="Нет активных авторассылок.",
                        edit_text="Нет активных авторассылок.",
                    )
                    return
                message = _build_stop_result_message(stopped, requested) or "Авторассылка остановлена."
                await _finalize_stop_callback(event, message=message)
                return
            labels = await _build_account_label_map(event.sender_id, tasks)
            lines = ["Выберите аккаунт для остановки:"]
            buttons = [
                [
                    Button.inline(
                        _format_account_list(task, labels),
                        f"{STOP_SELECT_CALLBACK}:{task.task_id}".encode("utf-8"),
                    )
                ]
                for task in tasks
            ]
            buttons.append([Button.inline("Отмена", f"{STOP_MENU_CALLBACK}:cancel".encode("utf-8"))])
            with contextlib.suppress(Exception):
                await event.edit("\n".join(lines), buttons=buttons)
            return
        if option == STOP_ALL_OPTION:
            tasks = await service.list_tasks_for_user(event.sender_id, active_only=True)
            if not tasks:
                await event.answer("Нет активных авторассылок.", alert=True)
                await _finalize_stop_callback(
                    event,
                    message="Нет активных авторассылок.",
                    edit_text="Нет активных авторассылок.",
                )
                return
            await event.answer("Останавливаю…")
            stopped, requested = await service.stop_tasks(
                user_id=event.sender_id,
                task_ids=[task.task_id for task in tasks],
            )
            if stopped <= 0:
                await _finalize_stop_callback(
                    event,
                    message="Нет активных авторассылок.",
                    edit_text="Нет активных авторассылок.",
                )
                return
            message = _build_stop_result_message(stopped, requested) or "Авторассылка остановлена."
            await _finalize_stop_callback(event, message=message)
            return
        await event.answer("Неизвестный выбор.", alert=True)

    @client.on(events.CallbackQuery(pattern=rf"^{STOP_SELECT_CALLBACK}:".encode("utf-8")))
    async def handle_stop_select_callback(event: CallbackQuery.Event) -> None:
        payload = event.data.decode("utf-8", errors="ignore")
        parts = payload.split(":", maxsplit=1)
        task_id = parts[1] if len(parts) > 1 else ""
        if not task_id:
            await event.answer("Некорректный выбор.", alert=True)
            return
        stopped, requested = await service.stop_tasks(user_id=event.sender_id, task_ids=[task_id])
        if stopped <= 0:
            await event.answer("Задача не найдена или недоступна.", alert=True)
            await _finalize_stop_callback(
                event,
                message="Задача не найдена или недоступна.",
                edit_text="Задача не найдена или недоступна.",
            )
            return
        message = _build_stop_result_message(stopped, requested) or "Авторассылка остановлена."
        await event.answer("Готово.")
        await _finalize_stop_callback(event, message=message)

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
                await event.edit("Действие отменено.")
            await event.respond("Возвращаюсь в главное меню.", buttons=build_main_menu_keyboard())
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
                await event.edit("Задача не найдена или недоступна.")
            await event.respond(
                f"Не удалось найти выбранную автозадачу. Попробуйте обновить список через {AUTO_STATUS_LABEL}.",
                buttons=build_main_menu_keyboard(),
            )
            return
        await event.answer("Готово.")
        labels = await _build_account_label_map(event.sender_id, [task])
        summary = _format_task_summary(task, labels)
        with contextlib.suppress(Exception):
            await event.edit("Готово ✅")
        await event.respond(f"{meta.success_text}\n\n{summary}", buttons=build_main_menu_keyboard())

    async def _handle_task_command(event: NewMessage.Event, action: str) -> None:
        if not event.is_private:
            return
        if action == "stop":
            await handle_stop_autobroadcast(event)
            return
        meta = TASK_ACTIONS.get(action)
        if meta is None:
            return
        await _show_task_action_menu(event, action)

    @client.on(events.NewMessage(pattern=AUTO_STATUS_PATTERN))
    async def handle_status(event: NewMessage.Event) -> None:
        if not event.is_private:
            return
        tasks = await service.list_tasks_for_user(event.sender_id, active_only=True)
        if not tasks:
            await event.respond("Нет активных авторассылок.", buttons=build_main_menu_keyboard())
            return
        labels = await _build_account_label_map(event.sender_id, tasks)
        blocks = [f"{idx}.\n{_format_task_summary(task, labels)}" for idx, task in enumerate(tasks, start=1)]
        body = "\n\n".join(blocks)
        await event.respond(f"Авторассылки:\n\n{body}", buttons=build_main_menu_keyboard())

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
