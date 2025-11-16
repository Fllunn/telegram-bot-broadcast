from __future__ import annotations

import asyncio
import base64
import binascii
import logging
import random
import re
import time
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional, Sequence, Set

from telethon import Button, events
from telethon.events import NewMessage
from telethon.tl import types as tl_types
from telethon.errors import MessageNotModifiedError
from telethon.errors.rpcerrorlist import (
	AuthKeyUnregisteredError,
	SessionRevokedError,
	UserDeactivatedError,
	UserDeactivatedBanError,
)

from src.bot.context import BotContext
from src.bot.keyboards import (
	ADD_IMAGE_LABEL,
	ADD_TEXT_LABEL,
	BROADCAST_LABEL,
	LOGIN_PHONE_LABEL,
	LOGIN_QR_LABEL,
	VIEW_BROADCAST_LABEL,
	build_main_menu_keyboard,
)

from src.models.session import TelethonSession
from src.services.broadcast_state import (
	BroadcastFlow,
	BroadcastRunScope,
	BroadcastRunStep,
	BroadcastStep,
)
from src.services.broadcast_shared import (
	BroadcastImageData as SharedBroadcastImageData,
	DialogsFetchError,
	ResolvedGroupTarget as SharedResolvedGroupTarget,
	deduplicate_broadcast_groups,
	collect_unique_target_peer_keys,
	describe_content_payload,
	extract_group_log_context,
	extract_identifier_from_link_value,
	log_broadcast_event,
	render_group_label,
	resolve_group_targets,
	send_payload_to_group,
	sanitize_username_value,
)


CANCEL_LABEL = "Отмена"

ACCOUNT_HEALTH_CHECK_INTERVAL = 30.0

SCOPE_PREFIX = "scope"
SCOPE_SINGLE = "single"
SCOPE_ALL = "all"
SELECT_PREFIX = "select"
CONFIRM_PREFIX = "confirm"
CANCEL_PREFIX = "cancel"

VIEW_SCOPE_PREFIX = "view_scope"
VIEW_SELECT_PREFIX = "view_select"
VIEW_CANCEL_PREFIX = "view_cancel"

RUN_SCOPE_PREFIX = "run_scope"
RUN_SELECT_PREFIX = "run_select"
RUN_CONFIRM_PREFIX = "run_confirm"
RUN_CANCEL_PREFIX = "run_cancel"
RUN_STOP_PREFIX = "run_stop"

ADD_TEXT_PATTERN = rf"^(?:/add_text(?:@\w+)?|{re.escape(ADD_TEXT_LABEL)})$"
ADD_IMAGE_PATTERN = rf"^(?:/add_image(?:@\w+)?|{re.escape(ADD_IMAGE_LABEL)})$"
VIEW_BROADCAST_PATTERN = rf"^(?:/view_broadcast(?:@\w+)?|{re.escape(VIEW_BROADCAST_LABEL)})$"
BROADCAST_PATTERN = rf"^(?:/broadcast(?:@\w+)?|{re.escape(BROADCAST_LABEL)})$"

BROADCAST_DELAY_MIN_SECONDS = 2
BROADCAST_DELAY_MAX_SECONDS = 5
BROADCAST_BATCH_SIZE = 5
BROADCAST_BATCH_PAUSE_SECONDS = 10


logger = logging.getLogger(__name__)

AUTH_ERROR_TYPES = (
	AuthKeyUnregisteredError,
	SessionRevokedError,
	UserDeactivatedError,
	UserDeactivatedBanError,
)
AUTH_ERROR_NAMES = {error.__name__ for error in AUTH_ERROR_TYPES}


def _log_broadcast(level: int, message: str, **details: Any) -> None:
	log_broadcast_event(level, message, **details)


@dataclass(frozen=True)
class FlowConfig:
	start_prompt: str
	no_sessions: str
	select_prompt: str
	wait_prompt_all: str
	wait_prompt_single: str
	replace_warning_all: str
	replace_warning_single: str
	confirm_prompt: str
	replace_cancelled: str
	success_message: str
	invalid_input: str
	save_error: str
	restart_hint: str
	metadata_key: str
	log_started_subject: str
	log_saved_subject: str


FLOW_CONFIG = {
	BroadcastFlow.TEXT: FlowConfig(
		start_prompt="Для каких аккаунтов сохранить текст рассылки?\nВыберите нужный вариант ниже.",
		no_sessions="У вас нет подключённых аккаунтов. Подключите аккаунт, чтобы добавить текст для рассылки.",
		select_prompt="Выберите аккаунт, для которого нужно сохранить текст:",
		wait_prompt_all="Отправьте текст, который будем использовать для рассылки по всем аккаунтам.",
		wait_prompt_single="Отправьте текст, который будем использовать для выбранного аккаунта.",
		replace_warning_all=(
			"В некоторых аккаунтах уже есть текст для рассылки.\n"
			"Вы действительно хотите его заменить для всех аккаунтов?"
		),
		replace_warning_single="Для выбранного аккаунта уже есть текст. Заменить его?",
		confirm_prompt="Введите новый текст для рассылки.",
		replace_cancelled="Изменение текста отменено.",
		success_message=(
			"Текст для рассылки сохранён. Вы можете изменить его командой /add_text или продолжить с выбранными аккаунтами."
		),
		invalid_input="Текст не может быть пустым. Отправьте сообщение ещё раз или напишите «Отмена».",
		save_error="Не удалось сохранить текст. Попробуйте ещё раз или отправьте «Отмена».",
		restart_hint="/add_text",
		metadata_key="broadcast_text",
		log_started_subject="текста",
		log_saved_subject="текст",
	),
	BroadcastFlow.IMAGE: FlowConfig(
		start_prompt="Для каких аккаунтов сохранить картинку для рассылки?\nВыберите нужный вариант ниже.",
		no_sessions="У вас нет подключённых аккаунтов. Подключите аккаунт, чтобы добавить картинку для рассылки.",
		select_prompt="Выберите аккаунт, для которого нужно сохранить картинку:",
		wait_prompt_all="Отправьте картинку, которую будем использовать для рассылки по всем аккаунтам.",
		wait_prompt_single="Отправьте картинку, которую будем использовать для выбранного аккаунта.",
		replace_warning_all=(
			"В некоторых аккаунтах уже есть картинка для рассылки.\n"
			"Вы действительно хотите её заменить для всех аккаунтов?"
		),
		replace_warning_single="Для выбранного аккаунта уже есть картинка. Заменить её?",
		confirm_prompt="Отправьте новую картинку для рассылки.",
		replace_cancelled="Изменение картинки отменено.",
		success_message=(
			"Картинка для рассылки сохранена. Вы можете изменить её командой /add_image или продолжить с выбранными аккаунтами."
		),
		invalid_input="Пожалуйста, отправьте фотографию или напишите «Отмена».",
		save_error="Не удалось сохранить картинку. Попробуйте ещё раз или отправьте «Отмена».",
		restart_hint="/add_image",
		metadata_key="broadcast_image",
		log_started_subject="картинки",
		log_saved_subject="картинку",
	),
}


WAITING_STEP = {
	BroadcastFlow.TEXT: BroadcastStep.WAITING_TEXT,
	BroadcastFlow.IMAGE: BroadcastStep.WAITING_IMAGE,
}


@dataclass(slots=True)
class SessionBroadcastPlan:
	"""Prepared broadcast payload for a specific account."""

	session: TelethonSession
	groups: list[Mapping[str, object]]
	text: Optional[str] = None
	image_meta: Optional[Mapping[str, object]] = None
	rows_total: int = 0
	actual_target_count: int = 0

	def has_text(self) -> bool:
		return bool(self.text and self.text.strip())

	def has_image(self) -> bool:
		if not self.image_meta:
			return False
		if isinstance(self.image_meta, Mapping) and self.image_meta.get("legacy_file_id"):
			return False
		return True


@dataclass(slots=True)
class BroadcastPlan:
	"""Aggregated data for executing a broadcast run."""

	sessions: list[SessionBroadcastPlan]
	total_groups: int
	unique_groups_total: int
	rows_total: int

	def has_text(self) -> bool:
		return any(entry.has_text() for entry in self.sessions)

	def has_image(self) -> bool:
		return any(entry.has_image() for entry in self.sessions)

	def session_labels(self) -> list[str]:
		return [_render_session_label(entry.session) for entry in self.sessions]


BroadcastImageData = SharedBroadcastImageData
ResolvedGroupTarget = SharedResolvedGroupTarget


def _flow_config(flow: BroadcastFlow) -> FlowConfig:
	return FLOW_CONFIG[flow]


def _extract_payload(data: bytes, prefix: str) -> str | None:
	try:
		decoded = data.decode("utf-8")
	except UnicodeDecodeError:
		return None
	if not decoded.startswith(prefix):
		return None
	return decoded.split(":", maxsplit=1)[-1]


def _coerce_positive_int(value: object, *, default: int = 0) -> int:
	if value is None or isinstance(value, bool):
		return default
	try:
		number = int(value)
	except (TypeError, ValueError):
		return default
	return number if number > 0 else default


async def _calculate_actual_target_count(
	context: BotContext,
	session: TelethonSession,
	groups: Sequence[Mapping[str, object]],
	*,
	user_id: int,
	account_label: str,
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
	finally:
		if session_client is not None:
			try:
				await context.session_manager.close_client(session_client)
			except Exception:
				logger.exception(
					"Не удалось закрыть клиент Telethon после расчёта целевых групп",
					extra={"session_id": session.session_id},
				)


def _expect_step(context: BotContext, step: BroadcastStep):
	def predicate(event: NewMessage.Event) -> bool:
		if not event.is_private or getattr(event.message, "out", False):
			return False
		state = context.broadcast_manager.get(event.sender_id)
		if state is None or state.step != step:
			return False
		if state.last_message_id is not None and state.last_message_id == event.id:
			return False
		return True

	return predicate


def _render_session_label(session: TelethonSession) -> str:
	display = session.display_name()
	phone = session.phone
	return f"{display} ({phone})" if phone else display


def _build_scope_buttons() -> list[list[Button]]:
	return [
		[
			Button.inline("Один аккаунт", f"{SCOPE_PREFIX}:{SCOPE_SINGLE}".encode("utf-8")),
			Button.inline("Все аккаунты", f"{SCOPE_PREFIX}:{SCOPE_ALL}".encode("utf-8")),
		],
		[Button.inline("❌ Отмена", f"{CANCEL_PREFIX}:scope".encode("utf-8"))],
	]


def _build_accounts_buttons(sessions: Iterable[TelethonSession]) -> list[list[Button]]:
	rows: list[list[Button]] = []
	for session in sessions:
		rows.append(
			[
				Button.inline(
					_render_session_label(session),
					f"{SELECT_PREFIX}:{session.session_id}".encode("utf-8"),
				)
			]
		)
	rows.append([Button.inline("❌ Отмена", f"{CANCEL_PREFIX}:accounts".encode("utf-8"))])
	return rows


def _build_confirmation_buttons() -> list[list[Button]]:
	return [
		[
			Button.inline("✅ Да", f"{CONFIRM_PREFIX}:yes".encode("utf-8")),
			Button.inline("❌ Отмена", f"{CONFIRM_PREFIX}:no".encode("utf-8")),
		]
	]


def _has_existing_content(session: TelethonSession, flow: BroadcastFlow) -> bool:
	metadata = session.metadata or {}
	if flow == BroadcastFlow.IMAGE:
		return bool(metadata.get("broadcast_image") or metadata.get("broadcast_image_file_id"))
	return bool(metadata.get(_flow_config(flow).metadata_key))


def _waiting_prompt(flow: BroadcastFlow, apply_to_all: bool) -> str:
	config = _flow_config(flow)
	return config.wait_prompt_all if apply_to_all else config.wait_prompt_single


def _build_view_scope_buttons() -> list[list[Button]]:
	return [
		[
			Button.inline("Один аккаунт", f"{VIEW_SCOPE_PREFIX}:{SCOPE_SINGLE}".encode("utf-8")),
			Button.inline("Все аккаунты", f"{VIEW_SCOPE_PREFIX}:{SCOPE_ALL}".encode("utf-8")),
		],
		[Button.inline("❌ Отмена", f"{VIEW_CANCEL_PREFIX}:scope".encode("utf-8"))],
	]


def _build_view_accounts_buttons(sessions: Iterable[TelethonSession]) -> list[list[Button]]:
	rows: list[list[Button]] = []
	for session in sessions:
		rows.append(
			[
				Button.inline(
					_render_session_label(session),
					f"{VIEW_SELECT_PREFIX}:{session.session_id}".encode("utf-8"),
				)
			]
		)
	rows.append([Button.inline("❌ Отмена", f"{VIEW_CANCEL_PREFIX}:accounts".encode("utf-8"))])
	return rows


def _build_broadcast_scope_buttons() -> list[list[Button]]:
	return [
		[
			Button.inline("Один аккаунт", f"{RUN_SCOPE_PREFIX}:single".encode("utf-8")),
			Button.inline("Все аккаунты", f"{RUN_SCOPE_PREFIX}:all".encode("utf-8")),
		],
		[Button.inline("❌ Отмена", f"{RUN_CANCEL_PREFIX}:scope".encode("utf-8"))],
	]


def _build_broadcast_account_buttons(sessions: Iterable[TelethonSession]) -> list[list[Button]]:
	rows: list[list[Button]] = []
	for session in sessions:
		rows.append(
			[
				Button.inline(
					_render_session_label(session),
					f"{RUN_SELECT_PREFIX}:{session.session_id}".encode("utf-8"),
				)
			]
		)
	rows.append([Button.inline("❌ Отмена", f"{RUN_CANCEL_PREFIX}:accounts".encode("utf-8"))])
	return rows


def _build_broadcast_confirmation_buttons() -> list[list[Button]]:
	return [
		[
			Button.inline("✅ Начать", f"{RUN_CONFIRM_PREFIX}:start".encode("utf-8")),
			Button.inline("❌ Отмена", f"{RUN_CONFIRM_PREFIX}:cancel".encode("utf-8")),
		]
	]


def _build_missing_content_keyboard() -> list[list[Button]]:
	return [
		[Button.text(ADD_TEXT_LABEL, resize=True)],
		[Button.text(ADD_IMAGE_LABEL, resize=True)],
		[Button.text(CANCEL_LABEL, resize=True)],
	]


def _build_connect_account_keyboard() -> list[list[Button]]:
	return [
		[Button.text(LOGIN_PHONE_LABEL, resize=True)],
		[Button.text(LOGIN_QR_LABEL, resize=True)],
		[Button.text(CANCEL_LABEL, resize=True)],
	]


def _build_progress_buttons(cancel_requested: bool) -> list[list[Button]] | None:
	if cancel_requested:
		return None
	return [[Button.inline("❌ Отмена рассылки", f"{RUN_STOP_PREFIX}:now".encode("utf-8"))]]


def _collect_session_materials_snapshot(sessions: Iterable[TelethonSession]) -> list[dict[str, object]]:
	snapshot: list[dict[str, object]] = []
	for session in sessions:
		metadata = session.metadata or {}
		raw_text = metadata.get("broadcast_text")
		text_value = str(raw_text).strip() if isinstance(raw_text, str) else None
		has_text = bool(text_value)
		image_meta = _extract_image_metadata(metadata)
		has_image = bool(image_meta and not image_meta.get("legacy_file_id"))
		snapshot.append(
			{
				"session_id": session.session_id,
				"label": _render_session_label(session),
				"has_text": has_text,
				"has_image": has_image,
			}
		)
	return snapshot


def _describe_broadcast_flow_state(state) -> dict[str, object]:
	if state is None:
		return {
			"flow": None,
			"step": BroadcastStep.IDLE.value,
			"apply_to_all": False,
			"targets": [],
		}
	return {
		"flow": state.flow.value,
		"step": state.step.value,
		"apply_to_all": state.apply_to_all,
		"targets": list(state.target_session_ids or []),
	}


def _extract_broadcast_groups(metadata: Optional[Mapping[str, object]]) -> list[Mapping[str, object]]:
	if not metadata:
		return []
	groups = metadata.get("broadcast_groups") if isinstance(metadata, Mapping) else None
	if not isinstance(groups, list):
		return []
	prepared: list[Mapping[str, object]] = []
	for entry in groups:
		if isinstance(entry, Mapping):
			prepared.append(dict(entry))
	return prepared


def _estimate_total_seconds(groups_count: int) -> float:
	if groups_count <= 0:
		return 0.0
	average_delay = (BROADCAST_DELAY_MIN_SECONDS + BROADCAST_DELAY_MAX_SECONDS) / 2
	total = groups_count * average_delay
	if groups_count > 0:
		batches = max(0, (groups_count - 1) // BROADCAST_BATCH_SIZE)
		total += batches * BROADCAST_BATCH_PAUSE_SECONDS
	return total


def _estimate_remaining_seconds(groups_left: int) -> float:
	return _estimate_total_seconds(groups_left)


def _format_duration(seconds: float) -> str:
	rounded = int(max(0, round(seconds)))
	if rounded <= 0:
		return "< 1 сек"
	hours, remainder = divmod(rounded, 3600)
	minutes, secs = divmod(remainder, 60)
	parts: list[str] = []
	if hours:
		parts.append(f"{hours} ч")
	if minutes:
		parts.append(f"{minutes} мин")
	if secs or not parts:
		parts.append(f"{secs} сек")
	return " ".join(parts)


def _build_confirmation_text(plan: BroadcastPlan) -> str:
	lines = [f"Будет отправлено в {plan.total_groups} уникальные группы."]
	if plan.rows_total:
		lines.append(f"Строк в файлах: {plan.rows_total}.")
	if plan.unique_groups_total and plan.unique_groups_total != plan.total_groups:
		lines.append(f"Уникальных записей в списке: {plan.unique_groups_total}.")
	if len(plan.sessions) == 1:
		lines.append(f"Выбранный аккаунт: {plan.session_labels()[0]}.")
	else:
		lines.append(f"Выбрано аккаунтов: {len(plan.sessions)}.")
	materials: list[str] = []
	materials.append("текст — есть" if plan.has_text() else "текст — нет")
	materials.append("картинка — есть" if plan.has_image() else "картинка — нет")
	lines.append(f"Материалы: {', '.join(materials)}.")
	lines.append("Рассылка будет проходить постепенно, с паузами для безопасности.")
	estimated = _format_duration(_estimate_total_seconds(plan.total_groups))
	lines.append(f"Оценочное время: ≈ {estimated}.")
	lines.append("Готовы начать?")
	return "\n".join(lines)


def _build_progress_text(
	status: str,
	total: int,
	processed: int,
	success: int,
	failed: int,
	current_account: Optional[str],
	current_chat: Optional[str],
	remaining_seconds: float,
) -> str:
	lines = [status]
	lines.append(f"Отправлено: {processed} / {total}")
	lines.append(f"Успешно: {success}")
	lines.append(f"Неудачно: {failed}")
	lines.append(f"Текущий аккаунт: {current_account or '—'}")
	lines.append(f"Текущий чат: {current_chat or '—'}")
	lines.append(f"Ожидаемое время: ≈ {_format_duration(remaining_seconds)}")
	return "\n".join(lines)


def _render_group_label(group: Mapping[str, object]) -> str:
	return render_group_label(group)


def _sanitize_username_value(value: object) -> Optional[str]:
	return sanitize_username_value(value)


def _describe_content_payload(has_text: bool, has_image: bool) -> str:
	return describe_content_payload(has_text, has_image)


def _extract_group_log_context(group: Mapping[str, object]) -> dict[str, Any]:
	return extract_group_log_context(group)


def _extract_identifier_from_link_value(value: object) -> Optional[str]:
	return extract_identifier_from_link_value(value)


def _is_broadcast_trigger(text: str) -> bool:
	if not text:
		return False
	normalized = text.strip()
	if not normalized:
		return False
	if normalized == BROADCAST_LABEL:
		return True
	if normalized.lower().startswith("/broadcast"):
		return True
	return False


async def _resolve_group_targets(
	client,
	group: Mapping[str, object],
	*,
	user_id: int,
	account_label: str,
	account_session_id: str,
	content_type: Optional[str] = None,
	dialogs_cache: Optional[dict[str, list[object]]] = None,
) -> tuple[list[ResolvedGroupTarget], Optional[str]]:
	return await resolve_group_targets(
		client,
		group,
		user_id=user_id,
		account_label=account_label,
		account_session_id=account_session_id,
		content_type=content_type,
		dialogs_cache=dialogs_cache,
	)


async def _prepare_image_data(plan_entry: SessionBroadcastPlan) -> BroadcastImageData | None:
	image_meta = plan_entry.image_meta
	if not image_meta or not isinstance(image_meta, Mapping):
		return None
	raw_bytes: Optional[bytes] = None
	encoded = image_meta.get("data_b64")
	if isinstance(encoded, str) and encoded:
		try:
			raw_bytes = base64.b64decode(encoded.encode("ascii"))
		except (ValueError, binascii.Error):
			raw_bytes = None
	file_name = image_meta.get("file_name") if isinstance(image_meta, Mapping) else None
	if file_name is not None:
		file_name = str(file_name) or None
	mime_type = image_meta.get("mime_type") if isinstance(image_meta, Mapping) else None
	if mime_type is not None:
		mime_type = str(mime_type) or None
	media, is_legacy = _build_input_media(image_meta)
	force_document = isinstance(media, tl_types.InputDocument) or image_meta.get("type") == "document"
	if media is None or is_legacy:
		if raw_bytes is None:
			return None
		return BroadcastImageData(
			media=None,
			force_document=force_document,
			raw_bytes=raw_bytes,
			file_name=file_name,
			mime_type=mime_type,
		)
	return BroadcastImageData(
		media=media,
		force_document=force_document,
		raw_bytes=raw_bytes,
		file_name=file_name,
		mime_type=mime_type,
	)


async def _build_broadcast_plan(
	context: BotContext,
	user_id: int,
	session_ids: Sequence[str],
	stored_sessions: dict[str, TelethonSession],
) -> tuple[BroadcastPlan | None, list[str]]:
	plans: list[SessionBroadcastPlan] = []
	errors: list[str] = []
	unique_groups_total = 0
	rows_total = 0
	actual_groups_total = 0
	seen_session_ids: set[str] = set()
	session_candidates: list[TelethonSession] = []
	session_labels: dict[str, str] = {}
	for session_id in session_ids:
		if not session_id or session_id in seen_session_ids:
			continue
		seen_session_ids.add(session_id)
		session = stored_sessions.get(session_id)
		if session is None:
			try:
				session = await context.session_repository.get_by_session_id(session_id)
			except Exception:
				logger.exception(
					"Не удалось загрузить данные аккаунта для рассылки",
					extra={"session_id": session_id, "user_id": user_id},
				)
				errors.append("Не удалось получить данные выбранного аккаунта. Попробуйте позже.")
				continue
			if session is not None:
				stored_sessions[session.session_id] = session
		if session is None or session.owner_id != user_id:
			errors.append("Выбранный аккаунт недоступен или был удалён.")
			continue
		session_candidates.append(session)
		session_labels[session.session_id] = _render_session_label(session)

	if session_candidates:
		status_results = await context.account_status_service.refresh_sessions(
			session_candidates,
			verify_dialog_access=True,
			use_cache=False,
		)
	else:
		status_results = {}

	for session in list(session_candidates):
		status = status_results.get(session.session_id)
		label = session_labels.get(session.session_id, _render_session_label(session))
		if status is None or not status.active:
			await context.session_manager.deactivate_session(session.session_id)
			await context.auto_broadcast_service.mark_account_inactive(
				session.session_id,
				owner_id=session.owner_id,
				reason=(status.detail if status else "session_validation_failed"),
				metadata=session.metadata,
			)
			errors.append(
				f"Аккаунт {label} стал неактивным"
				+ (f": {status.reason}" if status and status.reason else ".")
			)
			stored_sessions.pop(session.session_id, None)
			continue
		await context.auto_broadcast_service.mark_account_active(
			session.session_id,
			owner_id=session.owner_id,
			metadata=session.metadata,
		)
		session.is_active = True
		account_label = _render_session_label(session)

		metadata = session.metadata or {}
		all_groups = _extract_broadcast_groups(metadata)
		_log_broadcast(
			logging.INFO,
			f"Загружено {len(all_groups)} групп для аккаунта",
			user_id=user_id,
			account_label=account_label,
			account_session_id=session.session_id,
		)
		valid_groups: list[Mapping[str, object]] = []
		skipped_group_labels: list[str] = []
		for group in all_groups:
			member_flag = group.get("is_member") if isinstance(group, Mapping) else None
			if member_flag is False:
				label = _render_group_label(group)
				skipped_group_labels.append(label)
				_log_broadcast(
					logging.DEBUG,
					"Пропускаем чат: нет доступа",
					user_id=user_id,
					account_label=_render_session_label(session),
					account_session_id=session.session_id,
					**_extract_group_log_context(group),
				)
				continue
			valid_groups.append(dict(group))
		unique_groups = deduplicate_broadcast_groups(valid_groups)
		stats_payload = metadata.get("broadcast_groups_stats") if isinstance(metadata, Mapping) else None
		rows_from_stats = _coerce_positive_int(stats_payload.get("file_rows"), default=0) if isinstance(stats_payload, Mapping) else 0
		unique_from_stats = _coerce_positive_int(stats_payload.get("unique_groups"), default=0) if isinstance(stats_payload, Mapping) else 0
		rows_from_occurrences = 0
		for unique_entry in unique_groups:
			source_occurrences = _coerce_positive_int(unique_entry.get("source_occurrences"), default=1)
			if source_occurrences <= 0:
				source_occurrences = 1
			unique_entry["source_occurrences"] = source_occurrences
			rows_from_occurrences += source_occurrences
		rows_for_account = rows_from_stats or rows_from_occurrences or len(valid_groups)
		unique_for_account = unique_from_stats or len(unique_groups)
		raw_text = metadata.get("broadcast_text") if isinstance(metadata, Mapping) else None
		text = None
		if isinstance(raw_text, str):
			text = raw_text.strip()
		elif raw_text is not None:
			text = str(raw_text).strip()
		image_meta = _extract_image_metadata(metadata)
		if image_meta and image_meta.get("legacy_file_id"):
			_log_broadcast(
				logging.WARNING,
				"Сохранённая картинка устарела и будет пропущена",
				user_id=user_id,
				account_label=account_label,
				account_session_id=session.session_id,
			)
			image_meta = None
		content_type = _describe_content_payload(bool(text), bool(image_meta))

		session_errors: list[str] = []
		if not unique_groups:
			session_errors.append(f"Для аккаунта {account_label} не найден доступный список групп.")
		if not (text or image_meta):
			session_errors.append(
				f"Для аккаунта {account_label} нет текста или картинки для рассылки. Добавьте материалы через /add_text или /add_image."
			)
		stats_actual_default = _coerce_positive_int(
			stats_payload.get("actual_targets") if isinstance(stats_payload, Mapping) else None,
			default=len(unique_groups),
		)
		actual_target_count = stats_actual_default
		if not session_errors and unique_groups:
			try:
				actual_target_count = await _calculate_actual_target_count(
					context,
					session,
					unique_groups,
					user_id=user_id,
					account_label=account_label,
					content_type=content_type,
				)
			except DialogsFetchError as exc:
				session_errors.append(
					f"Не удалось проверить список чатов для аккаунта {account_label}. Попробуйте позже."
				)
				_log_broadcast(
					logging.ERROR,
					"Не удалось проверить список чатов для аккаунта",
					user_id=user_id,
					account_label=account_label,
					account_session_id=session.session_id,
					reason=exc.error_type,
				)
			except Exception:
				logger.exception(
					"Не удалось рассчитать фактическое количество целевых чатов",
					extra={"session_id": session.session_id, "user_id": user_id},
				)
				actual_target_count = max(actual_target_count, len(unique_groups))
		if session_errors:
			errors.extend(session_errors)
			if skipped_group_labels:
				errors.append(
					"Пропущены группы без доступа: " + ", ".join(skipped_group_labels[:5]) + (" …" if len(skipped_group_labels) > 5 else "")
				)
			_log_broadcast(
				logging.WARNING,
				"Аккаунт пропущен из-за ошибок подготовки рассылки",
				user_id=user_id,
				account_label=account_label,
				account_session_id=session.session_id,
				issues=session_errors,
				skipped_groups=skipped_group_labels,
			)
			continue

		_log_broadcast(
			logging.INFO,
			f"Подготовлено {actual_target_count} целевых чатов для аккаунта",
			user_id=user_id,
			account_label=account_label,
			account_session_id=session.session_id,
			groups_total=len(all_groups),
			groups_available=len(valid_groups),
			groups_unique=len(unique_groups),
			groups_actual=actual_target_count,
			file_rows=rows_for_account,
			file_unique=unique_for_account,
			rows_available=rows_from_occurrences,
		)

		plan_entry = SessionBroadcastPlan(
			session=session,
			groups=unique_groups,
			text=text,
			image_meta=image_meta,
			rows_total=rows_for_account,
			actual_target_count=actual_target_count,
		)
		plans.append(plan_entry)
		unique_groups_total += len(unique_groups)
		rows_total += rows_for_account
		actual_groups_total += actual_target_count

	if plans and actual_groups_total <= 0:
		errors.append("Не удалось определить группы для рассылки. Загрузите их через /upload_groups.")
	plan = (
		BroadcastPlan(
			sessions=plans,
			total_groups=actual_groups_total,
			unique_groups_total=unique_groups_total,
			rows_total=rows_total,
		)
		if plans and actual_groups_total > 0
		else None
	)
	return plan, errors


async def _safe_edit_message(client, user_id: int, message_id: int, text: str, *, buttons) -> None:
	try:
		await client.edit_message(user_id, message_id, text, buttons=buttons)
	except MessageNotModifiedError:
		return
	except Exception:
		logger.exception(
			"Не удалось обновить сообщение прогресса",
			extra={"user_id": user_id, "message_id": message_id},
		)


async def _send_payload_to_group(
	session_client,
	entity,
	text: Optional[str],
	image_data: BroadcastImageData | None,
	*,
	user_id: int,
	account_label: str,
	account_session_id: str,
	group: Mapping[str, object],
	group_label: str,
	content_type: str,
	extra_log_context: Optional[Mapping[str, Any]] = None,
) -> tuple[bool, str | None]:
	result, reason = await send_payload_to_group(
		session_client,
		entity,
		text,
		image_data,
		user_id=user_id,
		account_label=account_label,
		account_session_id=account_session_id,
		group=group,
		group_label=group_label,
		content_type=content_type,
		extra_log_context=extra_log_context,
	)
	return result, reason


async def _execute_broadcast_plan(
	context: BotContext,
	user_id: int,
	plan: BroadcastPlan,
	progress_message_id: int,
	*,
	bot_client,
) -> None:
	manager = context.broadcast_run_manager
	processed = 0
	success = 0
	failed = 0
	current_account_label: Optional[str] = None
	current_chat_label: Optional[str] = None
	image_cache: dict[str, BroadcastImageData | None] = {}
	dialogs_cache: dict[str, list[object]] = {}
	status_message = "Рассылка запущена"
	inactive_notified: Set[str] = set()

	_log_broadcast(
		logging.INFO,
		"Рассылка запущена",
		user_id=user_id,
		total_groups=plan.total_groups,
		unique_groups=plan.unique_groups_total,
		file_rows=plan.rows_total,
		accounts=len(plan.sessions),
	)

	def _is_cancelled() -> bool:
		state = manager.get(user_id)
		return bool(state and state.cancel_requested)

	async def _update_progress(status: str) -> None:
		remaining = max(0, plan.total_groups - processed)
		text = _build_progress_text(
			status,
			total=plan.total_groups,
			processed=processed,
			success=success,
			failed=failed,
			current_account=current_account_label,
			current_chat=current_chat_label,
			remaining_seconds=_estimate_remaining_seconds(remaining),
		)
		await _safe_edit_message(
			bot_client,
			user_id,
			progress_message_id,
			text,
			buttons=_build_progress_buttons(_is_cancelled()),
		)

	async def _handle_session_inactive(session: TelethonSession, detail: str) -> str:
		session_id = session.session_id
		session_label = _render_session_label(session)
		display_label = session.display_name() or session_label
		if session_id in inactive_notified:
			return display_label
		inactive_notified.add(session_id)
		try:
			await context.session_manager.deactivate_session(session_id)
		except Exception:
			logger.exception(
				"Не удалось деактивировать аккаунт",
				extra={"session_id": session_id, "owner_id": session.owner_id},
			)
		try:
			await context.auto_broadcast_service.mark_account_inactive(
				session_id,
				owner_id=session.owner_id,
				reason=detail,
				metadata=session.metadata,
			)
		except Exception:
			logger.exception(
				"Не удалось обновить состояние аккаунта в базе",
				extra={"session_id": session_id, "owner_id": session.owner_id},
			)
		session.is_active = False
		_log_broadcast(
			logging.WARNING,
			"Аккаунт стал неактивен во время рассылки",
			user_id=user_id,
			account_label=display_label,
			account_session_id=session_id,
			detail=detail,
		)
		try:
			await bot_client.send_message(user_id, f"Аккаунт {display_label} стал неактивным, войдите снова.")
		except Exception:
			logger.exception(
				"Не удалось уведомить пользователя о неактивном аккаунте",
				extra={"session_id": session_id, "owner_id": session.owner_id},
			)
		return display_label

	try:
		await _update_progress(status_message)

		for entry in plan.sessions:
			if _is_cancelled():
				break

			session_inactive = False
			current_account_label = _render_session_label(entry.session)
			session_client = None
			_log_broadcast(
				logging.INFO,
				"Начинаем отправку по аккаунту",
				user_id=user_id,
				account_label=current_account_label,
				account_session_id=entry.session.session_id,
				groups_total=entry.actual_target_count,
				groups_unique=len(entry.groups),
				file_rows=entry.rows_total,
				content_type=_describe_content_payload(bool(entry.text), entry.has_image()),
			)

			try:
				session_client = await context.session_manager.build_client_from_session(entry.session)
			except Exception as exc:
				if isinstance(exc, AUTH_ERROR_TYPES):
					label = await _handle_session_inactive(entry.session, f"build_client:{exc.__class__.__name__}")
					await _update_progress(f"Аккаунт {label} стал неактивным, пропускаем")
					session_inactive = True
				logger.exception(
					"Не удалось восстановить Telethon-клиент для аккаунта",
					extra={"session_id": entry.session.session_id, "owner_id": entry.session.owner_id},
				)
				_log_broadcast(
					logging.ERROR,
					"Не удалось восстановить клиент аккаунта, пропускаем",
					user_id=user_id,
					account_label=current_account_label,
					account_session_id=entry.session.session_id,
				)
				for group in entry.groups:
					processed += 1
					failed += 1
					current_chat_label = _render_group_label(group)
					_log_broadcast(
						logging.ERROR,
						"Пропускаем чат из-за ошибки подключения аккаунта",
						user_id=user_id,
						account_label=current_account_label,
						account_session_id=entry.session.session_id,
						**_extract_group_log_context(group),
					)
					await _update_progress("Не удалось подключиться к аккаунту, пропускаем")
				if _is_cancelled():
					break
				continue

			health_last_checked = 0.0

			async def _ensure_account_active(force: bool = False) -> bool:
				nonlocal health_last_checked, session_inactive
				now = time.monotonic()
				if not force and now - health_last_checked < ACCOUNT_HEALTH_CHECK_INTERVAL:
					return True
				health_last_checked = now
				try:
					status = await context.account_status_service.refresh_session(
						entry.session,
						verify_dialog_access=False,
						use_cache=False,
					)
				except Exception:
					logger.exception(
						"Failed to refresh account health during broadcast",
						extra={"user_id": user_id, "session_id": entry.session.session_id},
					)
					status = None
				if status and status.active:
					return True
				reason = status.detail if status and status.detail else "health_check_failed"
				label = await _handle_session_inactive(entry.session, reason)
				session_inactive = True
				await _update_progress(f"Аккаунт {label} стал неактивным, пропускаем")
				return False

			try:
				session_key = entry.session.session_id
				session_image = image_cache.get(session_key)
				if entry.has_image() and session_image is None:
					prepared = await _prepare_image_data(entry)
					image_cache[session_key] = prepared
					session_image = prepared
				else:
					image_cache.setdefault(session_key, session_image)

				if entry.has_image() and session_image is None and not entry.has_text():
					_log_broadcast(
						logging.ERROR,
						"Материалы для рассылки недоступны (нет текста и недоступна картинка)",
						user_id=user_id,
						account_label=current_account_label,
						account_session_id=entry.session.session_id,
					)
					for group in entry.groups:
						processed += 1
						failed += 1
						current_chat_label = _render_group_label(group)
						_log_broadcast(
							logging.ERROR,
							"Пропускаем чат: отсутствуют материалы для отправки",
							user_id=user_id,
							account_label=current_account_label,
							account_session_id=entry.session.session_id,
							**_extract_group_log_context(group),
						)
						await _update_progress("Материалы недоступны, пропускаем")
					if _is_cancelled():
						break
					continue

				if not await _ensure_account_active(force=True):
					continue

				for group in entry.groups:
					if session_inactive:
						break
					if _is_cancelled() or session_inactive:
						break
					if not await _ensure_account_active():
						break

					current_chat_label = _render_group_label(group)
					content_type = _describe_content_payload(bool(entry.text), session_image is not None)
					try:
						targets, duplicates_message = await _resolve_group_targets(
							session_client,
							group,
							user_id=user_id,
							account_label=current_account_label,
							account_session_id=entry.session.session_id,
							content_type=content_type,
							dialogs_cache=dialogs_cache,
						)
					except DialogsFetchError as exc:
						failed += 1
						processed += 1
						_log_broadcast(
							logging.ERROR,
							"Аккаунт утратил доступ к списку чатов",
							user_id=user_id,
							account_label=current_account_label,
							account_session_id=entry.session.session_id,
							reason=exc.error_type,
							**_extract_group_log_context(group),
						)
						label = await _handle_session_inactive(entry.session, f"dialogs:{exc.error_type}")
						session_inactive = True
						await _update_progress(f"Аккаунт {label} стал неактивным, пропускаем")
						break
					if not targets:
						failed += 1
						processed += 1
						_log_broadcast(
							logging.WARNING,
							"Не удалось определить чат для рассылки",
							user_id=user_id,
							account_label=current_account_label,
							account_session_id=entry.session.session_id,
							**_extract_group_log_context(group),
						)
						await _update_progress("Не удалось определить чат, пропускаем")
						continue

					duplicate_status_sent = False

					for target in targets:
						if session_inactive:
							break
						if _is_cancelled():
							break
						if not await _ensure_account_active():
							break

						current_chat_label = target.label
						result, reason = await _send_payload_to_group(
							session_client,
							target.entity,
							entry.text,
							session_image,
							user_id=user_id,
							account_label=current_account_label,
							account_session_id=entry.session.session_id,
							group=target.group,
							group_label=target.label,
							content_type=content_type,
							extra_log_context=target.log_context,
						)
						processed += 1
						if result:
							success += 1
							local_status = (
								status_message
								if not _is_cancelled()
								else "Рассылка будет остановлена после текущей отправки"
							)
						else:
							failed += 1
							_log_broadcast(
								logging.ERROR,
								"Ошибка при отправке сообщения в чат",
								user_id=user_id,
								account_label=current_account_label,
								account_session_id=entry.session.session_id,
								reason=reason,
								**_extract_group_log_context(target.group),
							)
							if reason and reason in AUTH_ERROR_NAMES:
								label = await _handle_session_inactive(entry.session, f"send:{reason}")
								session_inactive = True
								await _update_progress(f"Аккаунт {label} стал неактивным, пропускаем")
								break
							local_status = f"Ошибка: {reason or 'неизвестная ошибка'}"

						status_for_progress = local_status
						if duplicates_message and not duplicate_status_sent:
							status_for_progress = (
								duplicates_message
								if result
								else f"{duplicates_message}\n{local_status}"
							)
							duplicate_status_sent = True

						await _update_progress(status_for_progress)

						if _is_cancelled() or session_inactive:
							break

						if processed < plan.total_groups and not (_is_cancelled() or session_inactive):
							if processed % BROADCAST_BATCH_SIZE == 0:
								await asyncio.sleep(BROADCAST_BATCH_PAUSE_SECONDS)
							else:
								await asyncio.sleep(
									random.randint(BROADCAST_DELAY_MIN_SECONDS, BROADCAST_DELAY_MAX_SECONDS)
								)

					if _is_cancelled():
						break

			finally:
				if session_client is not None:
					try:
						await context.session_manager.close_client(session_client)
					except Exception:
						logger.exception(
							"Не удалось закрыть клиент Telethon после рассылки",
							extra={"session_id": entry.session.session_id},
						)

		final_status = "Рассылка остановлена пользователем" if _is_cancelled() else "Рассылка завершена"
		summary_lines = [final_status, f"Успешно: {success}"]
		if failed:
			summary_lines.append(f"С ошибками: {failed}")
		summary_lines.append(f"Целевых чатов: {plan.total_groups}")
		if plan.unique_groups_total and plan.unique_groups_total != plan.total_groups:
			summary_lines.append(f"Уникальных записей в списке: {plan.unique_groups_total}")
		if plan.rows_total:
			summary_lines.append(f"Строк в файлах: {plan.rows_total}")
		_log_broadcast(
			logging.INFO,
			"Рассылка завершена",
			user_id=user_id,
			total_groups=plan.total_groups,
			unique_groups=plan.unique_groups_total,
			file_rows=plan.rows_total,
			success=success,
			failed=failed,
			cancelled=_is_cancelled(),
		)

		await _safe_edit_message(
			bot_client,
			user_id,
			progress_message_id,
			"\n".join(summary_lines),
			buttons=None,
		)

	except Exception as critical_err:
		logger.exception(
			"Критическая ошибка при выполнении рассылки",
			extra={"user_id": user_id},
		)
		_log_broadcast(
			logging.ERROR,
			"Критическая ошибка при выполнении рассылки",
			user_id=user_id,
			error=str(critical_err),
			error_type=critical_err.__class__.__name__,
		)
		error_text = _build_progress_text(
			"Рассылка прервана из-за ошибки",
			plan.total_groups,
			processed,
			success,
			failed,
			current_account_label,
			current_chat_label,
			0,
		)
		await _safe_edit_message(bot_client, user_id, progress_message_id, error_text, buttons=None)
		await bot_client.send_message(
			user_id,
			"Рассылка прервана из-за внутренней ошибки. Попробуйте позже.",
			buttons=build_main_menu_keyboard(),
		)
	finally:
		manager.update(
			user_id,
			step=BroadcastRunStep.IDLE,
			task=None,
			cancel_requested=False,
			progress_message_id=None,
			plan=None,
			target_session_ids=[],
			last_trigger_message_id=None,
		)


async def _send_broadcast_overview(client, user_id: int, sessions: Iterable[TelethonSession]) -> None:
	any_content = False

	for session in sessions:
		label = _render_session_label(session)
		metadata = session.metadata or {}
		text = metadata.get("broadcast_text")
		image_meta = _extract_image_metadata(metadata)

		if text:
			await client.send_message(
				user_id,
				f"Аккаунт {label}\n\nТекущий текст для рассылки:\n{text}",
			)
			any_content = True

		if image_meta:
			try:
				media, is_legacy = _build_input_media(image_meta)
				if media is None:
					raise ValueError("invalid media payload")
				reply = await client.send_file(
					user_id,
					media,
					caption=f"Аккаунт {label}\nКартинка для рассылки",
				)
				del reply
			except Exception:
				logger.exception(
					"Не удалось отправить сохранённую картинку для аккаунта",
					extra={"user_id": user_id, "session_id": session.session_id},
				)
				await client.send_message(
					user_id,
					(
						f"Не удалось отправить картинку для аккаунта {label}. Загрузите её заново через /add_image."
						if not image_meta.get("legacy_file_id")
						else f"Картинка для аккаунта {label} устарела. Загрузите её заново через /add_image."
					),
				)
			else:
				any_content = True

		if not text and not image_meta:
			await client.send_message(
				user_id,
				(
					f"Для аккаунта {label} нет сохранённого текста или картинки.\n"
					"Используйте /add_text или /add_image, чтобы добавить материалы."
				),
			)

	if not any_content:
		await client.send_message(
			user_id,
			"Текущий текст или картинка для рассылки отсутствуют.\nИспользуйте /add_text или /add_image для добавления.",
			buttons=build_main_menu_keyboard(),
		)
	else:
		await client.send_message(
			user_id,
			"Готово. Чтобы обновить материалы, используйте /add_text или /add_image.",
			buttons=build_main_menu_keyboard(),
		)


def _resolve_image_entity(event: NewMessage.Event) -> tl_types.TypeFileLike | None:
	"""Extract a Telethon media entity suitable for pack_bot_file_id."""

	media = getattr(event.message, "media", None)
	if isinstance(media, tl_types.MessageMediaPhoto) and isinstance(media.photo, tl_types.Photo):
		return media.photo

	if isinstance(media, tl_types.MessageMediaDocument) and isinstance(media.document, tl_types.Document):
		mime = getattr(media.document, "mime_type", "") or ""
		if mime.startswith("image/"):
			return media.document
		for attribute in media.document.attributes:
			if isinstance(attribute, tl_types.DocumentAttributeImageSize):
				return media.document

	return None


def _serialize_image_entity(entity: tl_types.TypeFileLike) -> Mapping[str, object]:
	if isinstance(entity, tl_types.Photo):
		file_reference = base64.b64encode(entity.file_reference or b"").decode("ascii")
		return {
			"type": "photo",
			"id": entity.id,
			"access_hash": entity.access_hash,
			"file_reference": file_reference,
		}
	if isinstance(entity, tl_types.Document):
		file_reference = base64.b64encode(entity.file_reference or b"").decode("ascii")
		return {
			"type": "document",
			"id": entity.id,
			"access_hash": entity.access_hash,
			"file_reference": file_reference,
			"mime_type": getattr(entity, "mime_type", "image/jpeg") or "image/jpeg",
		}
	raise ValueError("Unsupported media type for рассылка")


def _extract_image_metadata(metadata: Mapping[str, object]) -> Optional[Mapping[str, object]]:
	if not metadata:
		return None
	image_meta = metadata.get("broadcast_image")
	if isinstance(image_meta, Mapping):
		return dict(image_meta)
	legacy = metadata.get("broadcast_image_file_id")
	if isinstance(legacy, str) and legacy:
		return {"legacy_file_id": legacy}
	return None


def _decode_file_reference(value: object) -> Optional[bytes]:
	if not value:
		return b""
	if isinstance(value, (bytes, bytearray)):
		return bytes(value)
	if isinstance(value, str):
		try:
			return base64.b64decode(value.encode("ascii"))
		except (ValueError, binascii.Error):
			return None
	return None


def _build_input_media(image_meta: Mapping[str, object]) -> tuple[object | None, bool]:
	if "legacy_file_id" in image_meta:
		return image_meta.get("legacy_file_id"), True

	media_type = image_meta.get("type")
	media_id = image_meta.get("id")
	access_hash = image_meta.get("access_hash")
	file_reference = _decode_file_reference(image_meta.get("file_reference"))
	if not isinstance(media_id, int) or not isinstance(access_hash, int) or file_reference is None:
		return None, False

	if media_type == "photo":
		return tl_types.InputPhoto(media_id, access_hash, file_reference), False
	if media_type == "document":
		return tl_types.InputDocument(media_id, access_hash, file_reference), False
	return None, False


def setup_broadcast_commands(client, context: BotContext) -> None:
	"""Register broadcast-related command handlers."""

	run_manager = context.broadcast_run_manager

	async def _start_flow(event: NewMessage.Event, flow: BroadcastFlow) -> None:
		if not event.is_private:
			return

		user_id = event.sender_id
		config = _flow_config(flow)

		previous_state = context.broadcast_manager.clear(user_id)
		if previous_state and previous_state.step != BroadcastStep.IDLE:
			logger.info(
				"Прерван незавершённый поток настройки рассылки",
				extra={"user_id": user_id, "flow": previous_state.flow.value, "step": previous_state.step.value},
			)

		sessions = list(
			await context.session_manager.get_active_sessions(user_id, verify_live=True)
		)
		if not sessions:
			await event.respond(config.no_sessions, buttons=build_main_menu_keyboard())
			return

		context.broadcast_manager.begin(
			user_id,
			flow=flow,
			step=BroadcastStep.CHOOSING_SCOPE,
			last_message_id=event.id,
		)
		logger.info(
			"Пользователь %s начал настройку %s для рассылки",
			user_id,
			config.log_started_subject,
		)
		message = await event.respond(config.start_prompt, buttons=_build_scope_buttons())
		context.broadcast_manager.update(user_id, last_message_id=message.id)

	@client.on(events.NewMessage(pattern=BROADCAST_PATTERN))
	async def handle_broadcast_run_command(event: NewMessage.Event) -> None:
		if not event.is_private:
			return

		user_id = event.sender_id
		previous_setup = context.broadcast_manager.get(user_id)
		state_snapshot = _describe_broadcast_flow_state(previous_setup)
		if previous_setup and previous_setup.step != BroadcastStep.IDLE:
			_log_broadcast(
				logging.INFO,
				"Запуск рассылки заблокирован активным сценарием настройки материалов",
				user_id=user_id,
				current_state=state_snapshot,
			)
			await event.respond(
				"Вы сейчас настраиваете материалы для рассылки. Завершите текущее действие или нажмите «Отмена», чтобы его прервать.",
				buttons=[[Button.text(CANCEL_LABEL, resize=True)]],
			)
			return

		run_state = run_manager.get(user_id)
		if run_state:
			if run_state.task and not run_state.task.done():
				_log_broadcast(
					logging.INFO,
					"Запрос запуска отклонён: рассылка уже выполняется",
					user_id=user_id,
					run_state_step=run_state.step.value,
					cancel_requested=run_state.cancel_requested,
				)
				await event.respond(
					"Рассылка уже выполняется. Используйте кнопку «Отмена рассылки» в сообщении прогресса.",
					buttons=build_main_menu_keyboard(),
				)
				return
			if run_state.last_trigger_message_id == event.id:
				_log_broadcast(
					logging.DEBUG,
					"Повторный запуск рассылки проигнорирован",
					user_id=user_id,
					message_id=event.id,
				)
				return
			run_manager.clear(user_id)

		try:
			sessions_iter = await context.session_manager.get_active_sessions(
				user_id,
				verify_live=True,
			)
		except Exception:
			logger.exception("Не удалось получить список аккаунтов для рассылки", extra={"user_id": user_id})
			await event.respond(
				"Не удалось получить список аккаунтов. Попробуйте позже.",
				buttons=build_main_menu_keyboard(),
			)
			return

		sessions = list(sessions_iter)
		if not sessions:
			_log_broadcast(
				logging.INFO,
				"Запуск рассылки отклонён: нет подключённых аккаунтов",
				user_id=user_id,
				current_state=state_snapshot,
				sessions=[],
				materials_available=False,
			)
			run_manager.clear(user_id)
			await event.respond(
				"Нельзя запустить рассылку: нет подключённых аккаунтов и/или нет сохранённого текста или картинки.\n\n"
				"Подключите аккаунт командой /login_phone или /login_qr, затем вернитесь в главное меню и повторите запуск.",
				buttons=build_main_menu_keyboard(),
			)
			return

		snapshot = _collect_session_materials_snapshot(sessions)
		has_text = any(entry["has_text"] for entry in snapshot)
		has_image = any(entry["has_image"] for entry in snapshot)
		has_materials = any(entry["has_text"] or entry["has_image"] for entry in snapshot)

		if not has_materials:
			_log_broadcast(
				logging.INFO,
				"Запуск рассылки отклонён: нет материалов",
				user_id=user_id,
				current_state=state_snapshot,
				sessions=snapshot,
			)
			run_manager.clear(user_id)
			await event.respond(
				"Нельзя запустить рассылку: нет сохранённого текста или картинки."
				"\nДобавьте материалы через /add_text или /add_image и запустите рассылку снова.",
				buttons=build_main_menu_keyboard(),
			)
			return

		_log_broadcast(
			logging.INFO,
			"Запуск рассылки: предварительные проверки пройдены",
			user_id=user_id,
			current_state=state_snapshot,
			sessions=snapshot,
			has_text=has_text,
			has_image=has_image,
		)

		run_manager.begin(
			user_id,
			step=BroadcastRunStep.CHOOSING_SCOPE,
			scope=BroadcastRunScope.SINGLE,
			sessions={session.session_id: session for session in sessions},
			last_message_id=event.id,
			trigger_message_id=event.id,
		)
		message = await event.respond(
			"Выберите, с каких аккаунтов отправлять рассылку.",
			buttons=_build_broadcast_scope_buttons(),
		)
		run_manager.update(user_id, last_message_id=message.id)

	@client.on(events.CallbackQuery(pattern=rf"^{RUN_SCOPE_PREFIX}:".encode("utf-8")))
	async def handle_broadcast_scope_selection(event: events.CallbackQuery.Event) -> None:
		user_id = event.sender_id
		state = run_manager.get(user_id)
		if state is None or state.step != BroadcastRunStep.CHOOSING_SCOPE:
			await event.answer("Эта операция больше неактуальна.", alert=True)
			return

		selection = _extract_payload(event.data, RUN_SCOPE_PREFIX)
		if selection not in {"single", "all"}:
			await event.answer("Некорректный выбор.", alert=True)
			return

		sessions = list(state.sessions.values())
		if not sessions:
			run_manager.clear(user_id)
			await event.edit(
				"Нет доступных аккаунтов для рассылки.",
				buttons=build_main_menu_keyboard(),
			)
			return

		if selection == "single":
			run_manager.update(user_id, step=BroadcastRunStep.CHOOSING_ACCOUNT, scope=BroadcastRunScope.SINGLE)
			edited = await event.edit(
				"Выберите аккаунт, от имени которого отправлять рассылку.",
				buttons=_build_broadcast_account_buttons(sessions),
			)
			run_manager.update(user_id, last_message_id=edited.id)
			return

		session_ids = [session.session_id for session in sessions]
		plan, errors = await _build_broadcast_plan(context, user_id, session_ids, state.sessions)
		if plan is None or errors:
			unique_errors = list(dict.fromkeys(errors)) if errors else []
			error_lines = ["Не удалось подготовить рассылку:"]
			if unique_errors:
				error_lines.extend(f"• {message}" for message in unique_errors)
			else:
				error_lines.append("• Проверьте настройки материалов и списков групп.")
			run_manager.clear(user_id)
			await event.edit("\n".join(error_lines), buttons=build_main_menu_keyboard())
			return

		run_manager.update(
			user_id,
			step=BroadcastRunStep.CONFIRMING,
			scope=BroadcastRunScope.ALL,
			target_session_ids=session_ids,
			plan=plan,
		)
		confirmation = _build_confirmation_text(plan)
		edited = await event.edit(confirmation, buttons=_build_broadcast_confirmation_buttons())
		run_manager.update(user_id, last_message_id=edited.id)

	@client.on(events.CallbackQuery(pattern=rf"^{RUN_SELECT_PREFIX}:".encode("utf-8")))
	async def handle_broadcast_account_selection(event: events.CallbackQuery.Event) -> None:
		user_id = event.sender_id
		state = run_manager.get(user_id)
		if state is None or state.step != BroadcastRunStep.CHOOSING_ACCOUNT:
			await event.answer("Эта операция больше неактуальна.", alert=True)
			return

		session_id = _extract_payload(event.data, RUN_SELECT_PREFIX)
		if not session_id:
			await event.answer("Некорректный выбор.", alert=True)
			return

		plan, errors = await _build_broadcast_plan(context, user_id, [session_id], state.sessions)
		if plan is None or errors:
			unique_errors = list(dict.fromkeys(errors)) if errors else []
			error_lines = ["Не удалось подготовить рассылку:"]
			if unique_errors:
				error_lines.extend(f"• {message}" for message in unique_errors)
			else:
				error_lines.append("• Проверьте материалы и список групп выбранного аккаунта.")
			run_manager.clear(user_id)
			await event.edit("\n".join(error_lines), buttons=build_main_menu_keyboard())
			return

		run_manager.update(
			user_id,
			step=BroadcastRunStep.CONFIRMING,
			scope=BroadcastRunScope.SINGLE,
			target_session_ids=[session_id],
			plan=plan,
		)
		confirmation = _build_confirmation_text(plan)
		edited = await event.edit(confirmation, buttons=_build_broadcast_confirmation_buttons())
		run_manager.update(user_id, last_message_id=edited.id)

	@client.on(events.CallbackQuery(pattern=rf"^{RUN_CONFIRM_PREFIX}:".encode("utf-8")))
	async def handle_broadcast_confirmation(event: events.CallbackQuery.Event) -> None:
		user_id = event.sender_id
		state = run_manager.get(user_id)
		if state is None or state.step != BroadcastRunStep.CONFIRMING:
			await event.answer("Эта операция больше неактуальна.", alert=True)
			return

		decision = _extract_payload(event.data, RUN_CONFIRM_PREFIX)
		if decision == "cancel":
			run_manager.clear(user_id)
			await event.edit("Рассылка отменена.", buttons=build_main_menu_keyboard())
			return

		if decision != "start":
			await event.answer("Некорректный выбор.", alert=True)
			return

		plan = state.plan
		if plan is None:
			run_manager.clear(user_id)
			await event.edit("Не удалось подготовить материалы для рассылки. Попробуйте начать заново.", buttons=build_main_menu_keyboard())
			return

		run_manager.update(user_id, step=BroadcastRunStep.RUNNING, cancel_requested=False)
		await event.edit("Рассылка запускается...", buttons=None)

		initial_text = _build_progress_text(
			"Рассылка запущена",
			plan.total_groups,
			0,
			0,
			0,
			None,
			None,
			_estimate_remaining_seconds(plan.total_groups),
		)
		progress_message = await event.client.send_message(
			user_id,
			initial_text,
			buttons=_build_progress_buttons(cancel_requested=False),
		)

		task = asyncio.create_task(
			_execute_broadcast_plan(
				context,
				user_id,
				plan,
				progress_message.id,
				bot_client=event.client,
			)
		)

		def _log_task_result(future: asyncio.Future) -> None:
			if future.cancelled():
				return
			exc = future.exception()
			if exc is not None:
				logger.exception("Ошибка фоновой задачи рассылки", exc_info=exc)

		task.add_done_callback(_log_task_result)
		run_manager.update(
			user_id,
			progress_message_id=progress_message.id,
			task=task,
		)

	@client.on(events.CallbackQuery(pattern=rf"^{RUN_CANCEL_PREFIX}:".encode("utf-8")))
	async def handle_broadcast_flow_cancel(event: events.CallbackQuery.Event) -> None:
		user_id = event.sender_id
		state = run_manager.get(user_id)
		if state is None or (state.task and not state.task.done()):
			await event.answer("Рассылка уже выполняется. Используйте кнопку «Отмена рассылки».", alert=True)
			return

		if not run_manager.has_active_flow(user_id):
			await event.answer("Нечего отменять.", alert=True)
			return

		run_manager.clear(user_id)
		await event.edit("Рассылка отменена.", buttons=build_main_menu_keyboard())

	@client.on(events.CallbackQuery(pattern=rf"^{RUN_STOP_PREFIX}:".encode("utf-8")))
	async def handle_broadcast_stop(event: events.CallbackQuery.Event) -> None:
		user_id = event.sender_id
		state = run_manager.get(user_id)
		if state is None or state.step != BroadcastRunStep.RUNNING:
			await event.answer("Рассылка не запущена.", alert=True)
			return
		if state.task is None or state.task.done():
			await event.answer("Рассылка уже завершена.", alert=True)
			return
		if state.cancel_requested:
			await event.answer("Отмена уже запрошена. Ожидайте завершения текущей отправки.", alert=True)
			return

		run_manager.update(user_id, cancel_requested=True)
		await event.answer("Рассылка будет остановлена после текущей отправки.", alert=True)

		@client.on(events.NewMessage(pattern=BROADCAST_PATTERN))
		async def handle_broadcast_run_command(event: NewMessage.Event) -> None:
			if not event.is_private:
				return

			user_id = event.sender_id
			previous_setup = context.broadcast_manager.get(user_id)
			state_snapshot = _describe_broadcast_flow_state(previous_setup)
			if previous_setup and previous_setup.step != BroadcastStep.IDLE:
				_log_broadcast(
					logging.INFO,
					"Запуск рассылки заблокирован активным сценарием настройки материалов",
					user_id=user_id,
					current_state=state_snapshot,
				)
				await event.respond(
					"Вы сейчас настраиваете материалы для рассылки. Завершите текущее действие или нажмите «Отмена», чтобы его прервать.",
					buttons=[[Button.text(CANCEL_LABEL, resize=True)]],
				)
				return

			run_state = run_manager.get(user_id)
			if run_state:
				if run_state.task and not run_state.task.done():
					_log_broadcast(
						logging.INFO,
						"Запрос запуска отклонён: рассылка уже выполняется",
						user_id=user_id,
						run_state_step=run_state.step.value,
						cancel_requested=run_state.cancel_requested,
					)
					await event.respond(
						"Рассылка уже выполняется. Используйте кнопку «Отмена рассылки» в сообщении прогресса.",
						buttons=build_main_menu_keyboard(),
					)
					return
				if run_state.last_trigger_message_id == event.id:
					_log_broadcast(
						logging.DEBUG,
						"Повторный запуск рассылки проигнорирован",
						user_id=user_id,
						message_id=event.id,
					)
					return
				run_manager.clear(user_id)

			try:
				sessions_iter = await context.session_manager.get_active_sessions(
					user_id,
					verify_live=True,
				)
			except Exception:
				logger.exception("Не удалось получить список аккаунтов для рассылки", extra={"user_id": user_id})
				await event.respond(
					"Не удалось получить список аккаунтов. Попробуйте позже.",
					buttons=build_main_menu_keyboard(),
				)
				return

			sessions = list(sessions_iter)
			if not sessions:
				_log_broadcast(
					logging.INFO,
					"Запуск рассылки отклонён: нет подключённых аккаунтов",
					user_id=user_id,
					current_state=state_snapshot,
					sessions=[],
					materials_available=False,
				)
				run_manager.clear(user_id)
				await event.respond(
					"Нельзя запустить рассылку: нет подключённых аккаунтов и/или нет сохранённого текста или картинки.\n\n"
					"Подключите аккаунт командой /login_phone или /login_qr, затем вернитесь в главное меню и повторите запуск.",
					buttons=build_main_menu_keyboard(),
				)
				return

			snapshot = _collect_session_materials_snapshot(sessions)
			has_text = any(entry["has_text"] for entry in snapshot)
			has_image = any(entry["has_image"] for entry in snapshot)
			has_materials = any(entry["has_text"] or entry["has_image"] for entry in snapshot)

			if not has_materials:
				_log_broadcast(
					logging.INFO,
					"Запуск рассылки отклонён: нет материалов",
					user_id=user_id,
					current_state=state_snapshot,
					sessions=snapshot,
				)
				run_manager.clear(user_id)
				await event.respond(
					"Нельзя запустить рассылку: нет сохранённого текста или картинки."
					"\nДобавьте материалы через /add_text или /add_image и запустите рассылку снова.",
					buttons=build_main_menu_keyboard(),
				)
				return

			_log_broadcast(
				logging.INFO,
				"Запуск рассылки: предварительные проверки пройдены",
				user_id=user_id,
				current_state=state_snapshot,
				sessions=snapshot,
				has_text=has_text,
				has_image=has_image,
			)

			run_manager.begin(
				user_id,
				step=BroadcastRunStep.CHOOSING_SCOPE,
				scope=BroadcastRunScope.SINGLE,
				sessions={session.session_id: session for session in sessions},
				last_message_id=event.id,
				trigger_message_id=event.id,
			)
			message = await event.respond(
				"Выберите, с каких аккаунтов отправлять рассылку.",
				buttons=_build_broadcast_scope_buttons(),
			)
			run_manager.update(user_id, last_message_id=message.id)

		@client.on(events.CallbackQuery(pattern=rf"^{RUN_SCOPE_PREFIX}:".encode("utf-8")))
		async def handle_broadcast_scope_selection(event: events.CallbackQuery.Event) -> None:
			user_id = event.sender_id
			state = run_manager.get(user_id)
			if state is None or state.step != BroadcastRunStep.CHOOSING_SCOPE:
				await event.answer("Эта операция больше неактуальна.", alert=True)
				return

			selection = _extract_payload(event.data, RUN_SCOPE_PREFIX)
			if selection not in {"single", "all"}:
				await event.answer("Некорректный выбор.", alert=True)
				return

			sessions = list(state.sessions.values())
			if not sessions:
				run_manager.clear(user_id)
				await event.edit(
					"Нет доступных аккаунтов для рассылки.",
					buttons=build_main_menu_keyboard(),
				)
				return

			if selection == "single":
				run_manager.update(user_id, step=BroadcastRunStep.CHOOSING_ACCOUNT, scope=BroadcastRunScope.SINGLE)
				edited = await event.edit(
					"Выберите аккаунт, от имени которого отправлять рассылку.",
					buttons=_build_broadcast_account_buttons(sessions),
				)
				run_manager.update(user_id, last_message_id=edited.id)
				return

			session_ids = [session.session_id for session in sessions]
			plan, errors = await _build_broadcast_plan(context, user_id, session_ids, state.sessions)
			if plan is None or errors:
				unique_errors = list(dict.fromkeys(errors)) if errors else []
				error_lines = ["Не удалось подготовить рассылку:"]
				if unique_errors:
					error_lines.extend(f"• {message}" for message in unique_errors)
				else:
					error_lines.append("• Проверьте настройки материалов и списков групп.")
				run_manager.clear(user_id)
				await event.edit("\n".join(error_lines), buttons=build_main_menu_keyboard())
				return

			run_manager.update(
				user_id,
				step=BroadcastRunStep.CONFIRMING,
				scope=BroadcastRunScope.ALL,
				target_session_ids=session_ids,
				plan=plan,
			)
			confirmation = _build_confirmation_text(plan)
			edited = await event.edit(confirmation, buttons=_build_broadcast_confirmation_buttons())
			run_manager.update(user_id, last_message_id=edited.id)

		@client.on(events.CallbackQuery(pattern=rf"^{RUN_SELECT_PREFIX}:".encode("utf-8")))
		async def handle_broadcast_account_selection(event: events.CallbackQuery.Event) -> None:
			user_id = event.sender_id
			state = run_manager.get(user_id)
			if state is None or state.step != BroadcastRunStep.CHOOSING_ACCOUNT:
				await event.answer("Эта операция больше неактуальна.", alert=True)
				return

			session_id = _extract_payload(event.data, RUN_SELECT_PREFIX)
			if not session_id:
				await event.answer("Некорректный выбор.", alert=True)
				return

			plan, errors = await _build_broadcast_plan(context, user_id, [session_id], state.sessions)
			if plan is None or errors:
				unique_errors = list(dict.fromkeys(errors)) if errors else []
				error_lines = ["Не удалось подготовить рассылку:"]
				if unique_errors:
					error_lines.extend(f"• {message}" for message in unique_errors)
				else:
					error_lines.append("• Проверьте материалы и список групп выбранного аккаунта.")
				run_manager.clear(user_id)
				await event.edit("\n".join(error_lines), buttons=build_main_menu_keyboard())
				return

			run_manager.update(
				user_id,
				step=BroadcastRunStep.CONFIRMING,
				scope=BroadcastRunScope.SINGLE,
				target_session_ids=[session_id],
				plan=plan,
			)
			confirmation = _build_confirmation_text(plan)
			edited = await event.edit(confirmation, buttons=_build_broadcast_confirmation_buttons())
			run_manager.update(user_id, last_message_id=edited.id)

		@client.on(events.CallbackQuery(pattern=rf"^{RUN_CONFIRM_PREFIX}:".encode("utf-8")))
		async def handle_broadcast_confirmation(event: events.CallbackQuery.Event) -> None:
			user_id = event.sender_id
			state = run_manager.get(user_id)
			if state is None or state.step != BroadcastRunStep.CONFIRMING:
				await event.answer("Эта операция больше неактуальна.", alert=True)
				return

			decision = _extract_payload(event.data, RUN_CONFIRM_PREFIX)
			if decision == "cancel":
				run_manager.clear(user_id)
				await event.edit("Рассылка отменена.", buttons=build_main_menu_keyboard())
				return

			if decision != "start":
				await event.answer("Некорректный выбор.", alert=True)
				return

			plan = state.plan
			if plan is None:
				run_manager.clear(user_id)
				await event.edit("Не удалось подготовить материалы для рассылки. Попробуйте начать заново.", buttons=build_main_menu_keyboard())
				return

			run_manager.update(user_id, step=BroadcastRunStep.RUNNING, cancel_requested=False)
			await event.edit("Рассылка запускается...", buttons=None)

			initial_text = _build_progress_text(
				"Рассылка запущена",
				plan.total_groups,
				0,
				0,
				0,
				None,
				None,
				_estimate_remaining_seconds(plan.total_groups),
			)
			progress_message = await event.client.send_message(
				user_id,
				initial_text,
				buttons=_build_progress_buttons(cancel_requested=False),
			)

			task = asyncio.create_task(
				_execute_broadcast_plan(
					context,
					user_id,
					plan,
					progress_message.id,
					bot_client=event.client,
				)
			)

			def _log_task_result(future: asyncio.Future) -> None:
				if future.cancelled():
					return
				exc = future.exception()
				if exc is not None:
					logger.exception("Ошибка фоновой задачи рассылки", exc_info=exc)

			task.add_done_callback(_log_task_result)
			run_manager.update(
				user_id,
				progress_message_id=progress_message.id,
				task=task,
			)

		@client.on(events.CallbackQuery(pattern=rf"^{RUN_CANCEL_PREFIX}:".encode("utf-8")))
		async def handle_broadcast_flow_cancel(event: events.CallbackQuery.Event) -> None:
			user_id = event.sender_id
			state = run_manager.get(user_id)
			if state is None or (state.task and not state.task.done()):
				await event.answer("Рассылка уже выполняется. Используйте кнопку «Отмена рассылки».", alert=True)
				return

			if not run_manager.has_active_flow(user_id):
				await event.answer("Нечего отменять.", alert=True)
				return

			run_manager.clear(user_id)
			await event.edit("Рассылка отменена.", buttons=build_main_menu_keyboard())

		@client.on(events.CallbackQuery(pattern=rf"^{RUN_STOP_PREFIX}:".encode("utf-8")))
		async def handle_broadcast_stop(event: events.CallbackQuery.Event) -> None:
			user_id = event.sender_id
			state = run_manager.get(user_id)
			if state is None or state.step != BroadcastRunStep.RUNNING:
				await event.answer("Рассылка не запущена.", alert=True)
				return
			if state.task is None or state.task.done():
				await event.answer("Рассылка уже завершена.", alert=True)
				return
			if state.cancel_requested:
				await event.answer("Отмена уже запрошена. Ожидайте завершения текущей отправки.", alert=True)
				return

			run_manager.update(user_id, cancel_requested=True)
			await event.answer("Рассылка будет остановлена после текущей отправки.", alert=True)

	@client.on(events.NewMessage(pattern=ADD_TEXT_PATTERN))
	async def handle_add_text_command(event: NewMessage.Event) -> None:
		await _start_flow(event, BroadcastFlow.TEXT)

	@client.on(events.NewMessage(pattern=ADD_IMAGE_PATTERN))
	async def handle_add_image_command(event: NewMessage.Event) -> None:
		await _start_flow(event, BroadcastFlow.IMAGE)

	@client.on(events.NewMessage(pattern=VIEW_BROADCAST_PATTERN))
	async def handle_view_broadcast_command(event: NewMessage.Event) -> None:
		if not event.is_private:
			return

		user_id = event.sender_id
		previous_state = context.broadcast_manager.clear(user_id)
		if previous_state and previous_state.step != BroadcastStep.IDLE:
			logger.info(
				"Пользователь переключился на просмотр материалов во время настройки",
				extra={"user_id": user_id, "flow": previous_state.flow.value, "step": previous_state.step.value},
			)

		try:
			sessions = list(
				await context.session_manager.get_active_sessions(user_id, verify_live=True)
			)
		except Exception:
			logger.exception(
				"Не удалось получить список аккаунтов для просмотра материалов",
				extra={"user_id": user_id},
			)
			await event.respond(
				"Не удалось получить список аккаунтов. Попробуйте позже.",
				buttons=build_main_menu_keyboard(),
			)
			return

		if not sessions:
			await event.respond(
				"У вас нет подключённых аккаунтов. Сначала подключите аккаунт через /login_phone или /login_qr.",
				buttons=build_main_menu_keyboard(),
			)
			return

		await event.respond(
			"Для каких аккаунтов показать текущие материалы?\nВыберите вариант ниже.",
			buttons=_build_view_scope_buttons(),
		)

	@client.on(events.CallbackQuery(pattern=rf"^{SCOPE_PREFIX}:".encode("utf-8")))
	async def handle_scope_choice(event: events.CallbackQuery.Event) -> None:
		user_id = event.sender_id
		state = context.broadcast_manager.get(user_id)
		if state is None or state.step != BroadcastStep.CHOOSING_SCOPE:
			await event.answer("Эта операция больше неактуальна.", alert=True)
			return

		flow = state.flow
		config = _flow_config(flow)

		scope = _extract_payload(event.data, SCOPE_PREFIX)
		if scope is None:
			await event.answer("Некорректный выбор.", alert=True)
			return

		sessions = list(
			await context.session_manager.get_active_sessions(user_id, verify_live=True)
		)
		if not sessions:
			await event.answer("Нет подключённых аккаунтов.", alert=True)
			await event.edit(config.no_sessions, buttons=build_main_menu_keyboard())
			context.broadcast_manager.clear(user_id)
			return

		if scope == SCOPE_SINGLE:
			context.broadcast_manager.update(user_id, step=BroadcastStep.CHOOSING_ACCOUNT, apply_to_all=False)
			edited = await event.edit(config.select_prompt, buttons=_build_accounts_buttons(sessions))
			context.broadcast_manager.update(user_id, last_message_id=edited.id)
			return

		if scope == SCOPE_ALL:
			session_ids = [session.session_id for session in sessions]
			context.broadcast_manager.update(
				user_id,
				apply_to_all=True,
				target_session_ids=session_ids,
			)
			existing = [s for s in sessions if _has_existing_content(s, flow)]
			if existing:
				context.broadcast_manager.update(user_id, step=BroadcastStep.CONFIRMING_REPLACE)
				edited = await event.edit(config.replace_warning_all, buttons=_build_confirmation_buttons())
				context.broadcast_manager.update(user_id, last_message_id=edited.id)
				return

			waiting_step = WAITING_STEP[flow]
			context.broadcast_manager.update(user_id, step=waiting_step)
			edited = await event.edit(
				_waiting_prompt(flow, apply_to_all=True),
				buttons=[[Button.text(CANCEL_LABEL, resize=True)]],
			)
			context.broadcast_manager.update(user_id, last_message_id=edited.id)
			return

		await event.answer("Неизвестный вариант.", alert=True)

	@client.on(events.CallbackQuery(pattern=rf"^{SELECT_PREFIX}:".encode("utf-8")))
	async def handle_account_selection(event: events.CallbackQuery.Event) -> None:
		user_id = event.sender_id
		state = context.broadcast_manager.get(user_id)
		if state is None or state.step != BroadcastStep.CHOOSING_ACCOUNT:
			await event.answer("Эта операция больше неактуальна.", alert=True)
			return

		flow = state.flow
		config = _flow_config(flow)

		session_id = _extract_payload(event.data, SELECT_PREFIX)
		if session_id is None:
			await event.answer("Некорректный выбор.", alert=True)
			return

		session = await context.session_repository.get_by_session_id(session_id)
		if session is None or session.owner_id != user_id:
			await event.answer("Сессия не найдена.", alert=True)
			return

		context.broadcast_manager.update(
			user_id,
			target_session_ids=[session.session_id],
			apply_to_all=False,
		)

		if _has_existing_content(session, flow):
			context.broadcast_manager.update(user_id, step=BroadcastStep.CONFIRMING_REPLACE)
			edited = await event.edit(config.replace_warning_single, buttons=_build_confirmation_buttons())
			context.broadcast_manager.update(user_id, last_message_id=edited.id)
			return

		waiting_step = WAITING_STEP[flow]
		context.broadcast_manager.update(user_id, step=waiting_step)
		edited = await event.edit(
			_waiting_prompt(flow, apply_to_all=False),
			buttons=[[Button.text(CANCEL_LABEL, resize=True)]],
		)
		context.broadcast_manager.update(user_id, last_message_id=edited.id)

	@client.on(events.CallbackQuery(pattern=rf"^{CONFIRM_PREFIX}:".encode("utf-8")))
	async def handle_confirmation(event: events.CallbackQuery.Event) -> None:
		user_id = event.sender_id
		state = context.broadcast_manager.get(user_id)
		if state is None or state.step != BroadcastStep.CONFIRMING_REPLACE:
			await event.answer("Эта операция больше неактуальна.", alert=True)
			return

		flow = state.flow
		config = _flow_config(flow)

		payload = _extract_payload(event.data, CONFIRM_PREFIX)
		if payload == "yes":
			waiting_step = WAITING_STEP[flow]
			context.broadcast_manager.update(user_id, step=waiting_step)
			edited = await event.edit(
				config.confirm_prompt,
				buttons=[[Button.text(CANCEL_LABEL, resize=True)]],
			)
			context.broadcast_manager.update(user_id, last_message_id=edited.id)
			return

		await event.edit(config.replace_cancelled, buttons=build_main_menu_keyboard())
		context.broadcast_manager.clear(user_id)

	@client.on(events.CallbackQuery(pattern=rf"^{CANCEL_PREFIX}:".encode("utf-8")))
	async def handle_flow_cancel(event: events.CallbackQuery.Event) -> None:
		user_id = event.sender_id
		if not context.broadcast_manager.has_active_flow(user_id):
			await event.answer("Нечего отменять.", alert=True)
			return

		context.broadcast_manager.clear(user_id)
		await event.edit("Настройка рассылки отменена.", buttons=build_main_menu_keyboard())

	@client.on(events.CallbackQuery(pattern=rf"^{VIEW_SCOPE_PREFIX}:".encode("utf-8")))
	async def handle_view_scope_choice(event: events.CallbackQuery.Event) -> None:
		user_id = event.sender_id
		scope = _extract_payload(event.data, VIEW_SCOPE_PREFIX)
		if scope is None:
			await event.answer("Некорректный выбор.", alert=True)
			return

		try:
			sessions = list(
				await context.session_manager.get_active_sessions(user_id, verify_live=True)
			)
		except Exception:
			logger.exception(
				"Не удалось получить список аккаунтов для просмотра материалов",
				extra={"user_id": user_id},
			)
			await event.edit(
				"Не удалось получить список аккаунтов. Попробуйте позже.",
				buttons=build_main_menu_keyboard(),
			)
			return

		if not sessions:
			await event.edit(
				"У вас нет подключённых аккаунтов. Сначала подключите аккаунт через /login_phone или /login_qr.",
				buttons=build_main_menu_keyboard(),
			)
			return

		if scope == SCOPE_SINGLE:
			await event.edit(
				"Выберите аккаунт, для которого показать материалы:",
				buttons=_build_view_accounts_buttons(sessions),
			)
			return

		if scope == SCOPE_ALL:
			await event.edit("Показываю текущие материалы для всех аккаунтов...", buttons=None)
			await _send_broadcast_overview(event.client, user_id, sessions)
			return

		await event.answer("Неизвестный вариант.", alert=True)

	@client.on(events.CallbackQuery(pattern=rf"^{VIEW_SELECT_PREFIX}:".encode("utf-8")))
	async def handle_view_account_selection(event: events.CallbackQuery.Event) -> None:
		user_id = event.sender_id
		session_id = _extract_payload(event.data, VIEW_SELECT_PREFIX)
		if session_id is None:
			await event.answer("Некорректный выбор.", alert=True)
			return

		try:
			session = await context.session_repository.get_by_session_id(session_id)
		except Exception:
			logger.exception(
				"Ошибка при получении данных сессии для просмотра материалов",
				extra={"user_id": user_id, "session_id": session_id},
			)
			await event.edit(
				"Не удалось получить данные аккаунта. Попробуйте позже.",
				buttons=build_main_menu_keyboard(),
			)
			return

		if session is None or session.owner_id != user_id:
			await event.answer("Сессия не найдена.", alert=True)
			return

		await event.edit(
			f"Показываю материалы для аккаунта {_render_session_label(session)}...",
			buttons=None,
		)
		await _send_broadcast_overview(event.client, user_id, [session])

	@client.on(events.CallbackQuery(pattern=rf"^{VIEW_CANCEL_PREFIX}:".encode("utf-8")))
	async def handle_view_cancel(event: events.CallbackQuery.Event) -> None:
		await event.edit(
			"Просмотр материалов отменён.",
			buttons=build_main_menu_keyboard(),
		)

	@client.on(events.NewMessage(incoming=True, func=_expect_step(context, BroadcastStep.WAITING_TEXT)))
	async def handle_broadcast_text(event: NewMessage.Event) -> None:
		user_id = event.sender_id
		config = _flow_config(BroadcastFlow.TEXT)

		text = (event.raw_text or "").strip()
		if _is_broadcast_trigger(text):
			context.broadcast_manager.clear(user_id)
			return
		if text.lower() == CANCEL_LABEL.lower():
			context.broadcast_manager.clear(user_id)
			await event.respond("Настройка рассылки отменена.", buttons=build_main_menu_keyboard())
			return

		if not text:
			context.broadcast_manager.update(user_id, last_message_id=event.id)
			await event.respond(
				config.invalid_input,
				buttons=[[Button.text(CANCEL_LABEL, resize=True)]],
			)
			return

		state = context.broadcast_manager.get(user_id)
		if state is None or not state.target_session_ids:
			logger.warning("Нет целевых сессий для сохранения текста", extra={"user_id": user_id})
			await event.respond(
				f"Не удалось определить целевые аккаунты. Попробуйте начать заново командой {config.restart_hint}.",
				buttons=build_main_menu_keyboard(),
			)
			context.broadcast_manager.clear(user_id)
			return

		try:
			modified = await context.session_repository.set_broadcast_texts(state.target_session_ids, text)
		except Exception:
			logger.exception("Ошибка при сохранении текста рассылки", extra={"user_id": user_id})
			context.broadcast_manager.update(user_id, last_message_id=event.id)
			await event.respond(
				config.save_error,
				buttons=[[Button.text(CANCEL_LABEL, resize=True)]],
			)
			return

		context.broadcast_manager.clear(user_id)
		logger.info(
			"Пользователь %s сохранил %s для %s аккаунтов",
			user_id,
			config.log_saved_subject,
			modified,
		)
		await event.respond(config.success_message, buttons=build_main_menu_keyboard())

	@client.on(events.NewMessage(incoming=True, func=_expect_step(context, BroadcastStep.WAITING_IMAGE)))
	async def handle_broadcast_image(event: NewMessage.Event) -> None:
		user_id = event.sender_id
		config = _flow_config(BroadcastFlow.IMAGE)

		entity = _resolve_image_entity(event)
		if entity is None:
			text = (event.raw_text or "").strip()
			if _is_broadcast_trigger(text):
				context.broadcast_manager.clear(user_id)
				return
			if text.lower() == CANCEL_LABEL.lower():
				context.broadcast_manager.clear(user_id)
				await event.respond("Настройка рассылки отменена.", buttons=build_main_menu_keyboard())
				return

			context.broadcast_manager.update(user_id, last_message_id=event.id)
			await event.respond(
				config.invalid_input,
				buttons=[[Button.text(CANCEL_LABEL, resize=True)]],
			)
			return

		try:
			image_payload = dict(_serialize_image_entity(entity))
		except ValueError:
			context.broadcast_manager.update(user_id, last_message_id=event.id)
			await event.respond(
				config.invalid_input,
				buttons=[[Button.text(CANCEL_LABEL, resize=True)]],
			)
			return
		except Exception:
			logger.exception("Не удалось подготовить данные изображения для рассылки", extra={"user_id": user_id})
			context.broadcast_manager.update(user_id, last_message_id=event.id)
			await event.respond(
				config.invalid_input,
				buttons=[[Button.text(CANCEL_LABEL, resize=True)]],
			)
			return

		file_name = None
		mime_type = None
		file_attr = getattr(event.message, "file", None)
		if file_attr is not None:
			file_name = getattr(file_attr, "name", None) or None
			mime_type = getattr(file_attr, "mime_type", None) or image_payload.get("mime_type")

		try:
			file_bytes = await event.client.download_media(event.message, bytes)
		except Exception:
			logger.exception("Не удалось скачать изображение для сохранения", extra={"user_id": user_id})
			context.broadcast_manager.update(user_id, last_message_id=event.id)
			await event.respond(
				config.invalid_input,
				buttons=[[Button.text(CANCEL_LABEL, resize=True)]],
			)
			return

		if not file_bytes:
			context.broadcast_manager.update(user_id, last_message_id=event.id)
			await event.respond(
				config.invalid_input,
				buttons=[[Button.text(CANCEL_LABEL, resize=True)]],
			)
			return
		if isinstance(file_bytes, bytearray):
			file_bytes = bytes(file_bytes)

		image_payload["data_b64"] = base64.b64encode(file_bytes).decode("ascii")
		if file_name:
			image_payload["file_name"] = file_name
		if mime_type:
			image_payload["mime_type"] = mime_type
		elif image_payload.get("type") == "photo":
			image_payload["mime_type"] = "image/jpeg"

		state = context.broadcast_manager.get(user_id)
		if state is None or not state.target_session_ids:
			logger.warning("Нет целевых сессий для сохранения картинки", extra={"user_id": user_id})
			await event.respond(
				f"Не удалось определить целевые аккаунты. Попробуйте начать заново командой {config.restart_hint}.",
				buttons=build_main_menu_keyboard(),
			)
			context.broadcast_manager.clear(user_id)
			return

		try:
			modified = await context.session_repository.set_broadcast_images(state.target_session_ids, image_payload)
		except Exception:
			logger.exception("Ошибка при сохранении картинки для рассылки", extra={"user_id": user_id})
			context.broadcast_manager.update(user_id, last_message_id=event.id)
			await event.respond(
				config.save_error,
				buttons=[[Button.text(CANCEL_LABEL, resize=True)]],
			)
			return

		context.broadcast_manager.clear(user_id)
		logger.info(
			"Пользователь %s сохранил %s для %s аккаунтов",
			user_id,
			config.log_saved_subject,
			modified,
		)
		await event.respond(config.success_message, buttons=build_main_menu_keyboard())

