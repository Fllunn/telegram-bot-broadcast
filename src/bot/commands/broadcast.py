from __future__ import annotations

import logging
import re
import base64
import binascii
from dataclasses import dataclass
from typing import Iterable, Mapping, Optional

from telethon import Button, events
from telethon.tl import types as tl_types
from telethon.events import NewMessage

from src.bot.context import BotContext
from src.bot.keyboards import (
	ADD_IMAGE_LABEL,
	ADD_TEXT_LABEL,
	VIEW_BROADCAST_LABEL,
	build_main_menu_keyboard,
)
from src.models.session import TelethonSession
from src.services.broadcast_state import BroadcastFlow, BroadcastStep

logger = logging.getLogger(__name__)

CANCEL_LABEL = "Отмена"
SCOPE_SINGLE = "single"
SCOPE_ALL = "all"
SCOPE_PREFIX = "broadcast_scope"
SELECT_PREFIX = "broadcast_select"
CONFIRM_PREFIX = "broadcast_confirm"
CANCEL_PREFIX = "broadcast_cancel"
ADD_TEXT_PATTERN = rf"^(?:/add_text(?:@\w+)?|{re.escape(ADD_TEXT_LABEL)})$"
ADD_IMAGE_PATTERN = rf"^(?:/add_image(?:@\w+)?|{re.escape(ADD_IMAGE_LABEL)})$"
VIEW_SCOPE_PREFIX = "view_scope"
VIEW_SELECT_PREFIX = "view_select"
VIEW_CANCEL_PREFIX = "view_cancel"
VIEW_BROADCAST_PATTERN = rf"^(?:/view_broadcast(?:@\w+)?|{re.escape(VIEW_BROADCAST_LABEL)})$"

WAITING_STEP = {
	BroadcastFlow.TEXT: BroadcastStep.WAITING_TEXT,
	BroadcastFlow.IMAGE: BroadcastStep.WAITING_IMAGE,
}


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

	async def _start_flow(event: NewMessage.Event, flow: BroadcastFlow) -> None:
		if not event.is_private:
			return

		user_id = event.sender_id
		config = _flow_config(flow)

		if context.broadcast_manager.has_active_flow(user_id):
			await event.respond(
				"Вы уже настраиваете параметры рассылки. Завершите текущий процесс или отправьте «Отмена».",
				buttons=[[Button.text(CANCEL_LABEL, resize=True)]],
			)
			return

		sessions = list(await context.session_manager.get_active_sessions(user_id))
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

		if context.broadcast_manager.has_active_flow(user_id):
			await event.respond(
				"Сначала завершите текущую настройку рассылки или отправьте «Отмена».",
				buttons=[[Button.text(CANCEL_LABEL, resize=True)]],
			)
			return

		try:
			sessions = list(await context.session_manager.get_active_sessions(user_id))
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

		sessions = list(await context.session_manager.get_active_sessions(user_id))
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
			sessions = list(await context.session_manager.get_active_sessions(user_id))
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
