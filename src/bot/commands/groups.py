from __future__ import annotations

import io
import logging
import math
import re
from dataclasses import dataclass
from typing import Iterable, List, Mapping, Optional
from urllib.parse import urlparse

import xlrd
from openpyxl import load_workbook
from telethon import Button, events, utils
from telethon.events import NewMessage
from telethon.tl.custom import Message
from telethon.tl.types import DocumentAttributeFilename

from src.bot.context import BotContext
from src.bot.keyboards import UPLOAD_GROUPS_LABEL, build_main_menu_keyboard
from src.models.session import TelethonSession
from src.services.groups_state import GroupUploadStateManager, GroupUploadStep

logger = logging.getLogger(__name__)

CANCEL_LABEL = "Отмена"
UPLOAD_GROUPS_PATTERN = rf"^(?:/upload_groups(?:@\w+)?|{re.escape(UPLOAD_GROUPS_LABEL)})$"
SELECT_PREFIX = "groups_select"
CONFIRM_PREFIX = "groups_confirm"
CANCEL_PREFIX = "groups_cancel"

ALLOWED_EXTENSIONS = {".xlsx", ".xls"}


@dataclass(frozen=True)
class ParsedGroup:
    name: Optional[str]
    username: Optional[str]
    link: Optional[str]


def _expect_step(context: BotContext, step: GroupUploadStep):
    def predicate(event: NewMessage.Event) -> bool:
        if not event.is_private or getattr(event.message, "out", False):
            return False
        state = context.groups_manager.get(event.sender_id)
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


def _build_account_buttons(sessions: Iterable[TelethonSession]) -> list[list[Button]]:
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
    rows.append([Button.inline("❌ Отмена", f"{CANCEL_PREFIX}:select".encode("utf-8"))])
    return rows


def _build_confirmation_buttons(session_id: str) -> list[list[Button]]:
    return [
        [
            Button.inline("✅ Да", f"{CONFIRM_PREFIX}:yes:{session_id}".encode("utf-8")),
            Button.inline("❌ Нет", f"{CONFIRM_PREFIX}:no:{session_id}".encode("utf-8")),
        ]
    ]


def _build_file_prompt_buttons() -> list[list[Button]]:
    return [[Button.text(CANCEL_LABEL, resize=True)]]


def _normalize_cell_value(value) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            return ""
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return str(value).strip()
    return str(value).strip()


def _parse_xlsx(content: bytes) -> List[ParsedGroup]:
    workbook = load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    sheet = workbook.active
    rows: List[ParsedGroup] = []
    for idx, row in enumerate(sheet.iter_rows(values_only=True)):
        name = _normalize_cell_value(row[0]) if len(row) > 0 else ""
        username = _normalize_cell_value(row[1]) if len(row) > 1 else ""
        link = _normalize_cell_value(row[2]) if len(row) > 2 else ""
        if idx == 0 and _is_header_row(name, username, link):
            continue
        if not any((name, username, link)):
            continue
        rows.append(ParsedGroup(name=name or None, username=username or None, link=link or None))
    return rows


def _parse_xls(content: bytes) -> List[ParsedGroup]:
    workbook = xlrd.open_workbook(file_contents=content)
    sheet = workbook.sheet_by_index(0)
    rows: List[ParsedGroup] = []
    for idx in range(sheet.nrows):
        row = sheet.row_values(idx)
        name = _normalize_cell_value(row[0]) if len(row) > 0 else ""
        username = _normalize_cell_value(row[1]) if len(row) > 1 else ""
        link = _normalize_cell_value(row[2]) if len(row) > 2 else ""
        if idx == 0 and _is_header_row(name, username, link):
            continue
        if not any((name, username, link)):
            continue
        rows.append(ParsedGroup(name=name or None, username=username or None, link=link or None))
    return rows


def _is_header_row(name: str, username: str, link: str) -> bool:
    header_tokens = {token.lower() for token in (name, username, link) if token}
    return bool(
        {"название", "название группы", "name"} & header_tokens
        or {"username", "юзернейм"} & header_tokens
        or {"ссылка", "link"} & header_tokens
    )


async def _resolve_chat_id(client, username: Optional[str], link: Optional[str]) -> tuple[Optional[int], bool]:
    candidate = _sanitize_username(username) or _extract_identifier_from_link(link)
    if not candidate:
        return None, False

    try:
        entity = await client.get_input_entity(candidate)
    except Exception:
        return None, False

    try:
        peer_id = utils.get_peer_id(entity)
    except Exception:
        return None, False

    return peer_id, True


def _sanitize_username(username: Optional[str]) -> Optional[str]:
    if not username:
        return None
    username = username.strip()
    if not username:
        return None
    username = username.lstrip("@")
    if not username:
        return None
    return username


def _extract_identifier_from_link(link: Optional[str]) -> Optional[str]:
    if not link:
        return None
    link = link.strip()
    if not link:
        return None
    if link.startswith("http://") or link.startswith("https://"):
        parsed = urlparse(link)
        if parsed.netloc and parsed.netloc.lower().endswith("t.me"):
            path = parsed.path.lstrip("/")
            if path:
                return path.split("/", 1)[0]
        return None
    if link.startswith("t.me/"):
        return link.split("/", 1)[-1]
    return None


def _extract_filename(document) -> str:
    for attribute in document.attributes or []:
        if isinstance(attribute, DocumentAttributeFilename):
            return attribute.file_name or ""
    return ""


async def _parse_groups_file(file_bytes: bytes, extension: str) -> List[ParsedGroup]:
    if extension == ".xlsx":
        return _parse_xlsx(file_bytes)
    if extension == ".xls":
        return _parse_xls(file_bytes)
    raise ValueError("Unsupported file extension")


def _serialize_group(group: ParsedGroup, chat_id: Optional[int], is_member: bool) -> Mapping[str, object]:
    return {
        "name": group.name,
        "username": group.username,
        "link": group.link,
        "chat_id": chat_id,
        "is_member": is_member,
    }


async def _handle_cancel(event: NewMessage.Event, manager: GroupUploadStateManager, message: str) -> None:
    user_id = event.sender_id
    manager.clear(user_id)
    await event.respond(message, buttons=build_main_menu_keyboard())


def setup_group_commands(client, context: BotContext) -> None:
    """Register commands for uploading broadcast group lists."""

    @client.on(events.NewMessage(pattern=UPLOAD_GROUPS_PATTERN))
    async def handle_upload_groups(event: NewMessage.Event) -> None:
        if not event.is_private:
            return

        user_id = event.sender_id
        if context.groups_manager.has_active_flow(user_id):
            await event.respond(
                "Вы уже загружаете список групп. Завершите текущий процесс или отправьте «Отмена».",
                buttons=_build_file_prompt_buttons(),
            )
            return

        sessions = list(await context.session_manager.get_active_sessions(user_id))
        if not sessions:
            await event.respond(
                "У вас нет подключённых аккаунтов. Подключите аккаунт, чтобы загрузить список групп.",
                buttons=build_main_menu_keyboard(),
            )
            return

        context.groups_manager.begin(user_id, step=GroupUploadStep.CHOOSING_ACCOUNT, last_message_id=event.id)
        body = "Выберите аккаунт, для которого нужно загрузить список групп."
        message: Message = await event.respond(body, buttons=_build_account_buttons(sessions))
        context.groups_manager.update(user_id, last_message_id=message.id)

    @client.on(events.CallbackQuery(pattern=rf"^{SELECT_PREFIX}:".encode("utf-8")))
    async def handle_account_selection(event: events.CallbackQuery.Event) -> None:
        user_id = event.sender_id
        state = context.groups_manager.get(user_id)
        if state is None or state.step != GroupUploadStep.CHOOSING_ACCOUNT:
            await event.answer("Эта операция больше неактуальна.", alert=True)
            return

        payload = event.data.decode("utf-8")
        session_id = payload.split(":", maxsplit=1)[-1]
        session = await context.session_repository.get_by_session_id(session_id)
        if session is None or session.owner_id != user_id:
            await event.answer("Сессия не найдена.", alert=True)
            return

        existing = (session.metadata or {}).get("broadcast_groups")
        context.groups_manager.update(user_id, target_session_id=session.session_id)

        if existing:
            context.groups_manager.update(user_id, step=GroupUploadStep.CONFIRMING_REPLACE)
            await event.edit(
                "Для выбранного аккаунта уже загружен список групп. Заменить его?",
                buttons=_build_confirmation_buttons(session.session_id),
            )
            return

        context.groups_manager.update(user_id, step=GroupUploadStep.WAITING_FILE)
        await event.edit(
            "Отправьте Excel-файл (.xlsx или .xls) со списком групп. Первая строка может быть заголовком.",
            buttons=_build_file_prompt_buttons(),
        )

    @client.on(events.CallbackQuery(pattern=rf"^{CONFIRM_PREFIX}:".encode("utf-8")))
    async def handle_confirmation(event: events.CallbackQuery.Event) -> None:
        user_id = event.sender_id
        state = context.groups_manager.get(user_id)
        if state is None or state.step != GroupUploadStep.CONFIRMING_REPLACE:
            await event.answer("Эта операция больше неактуальна.", alert=True)
            return

        payload = event.data.decode("utf-8").split(":")
        if len(payload) < 3:
            await event.answer("Некорректный запрос.", alert=True)
            return
        action, _, session_id = payload
        if session_id != (state.target_session_id or ""):
            await event.answer("Некорректный запрос.", alert=True)
            return

        if action.endswith("yes"):
            context.groups_manager.update(user_id, step=GroupUploadStep.WAITING_FILE)
            await event.edit(
                "Отправьте Excel-файл (.xlsx или .xls) со списком групп. Первая строка может быть заголовком.",
                buttons=_build_file_prompt_buttons(),
            )
            return

        context.groups_manager.clear(user_id)
        await event.edit("Загрузка списка групп отменена.", buttons=build_main_menu_keyboard())

    @client.on(events.CallbackQuery(pattern=rf"^{CANCEL_PREFIX}:".encode("utf-8")))
    async def handle_inline_cancel(event: events.CallbackQuery.Event) -> None:
        user_id = event.sender_id
        if not context.groups_manager.has_active_flow(user_id):
            await event.answer("Нечего отменять.", alert=True)
            return
        context.groups_manager.clear(user_id)
        await event.edit("Загрузка списка групп отменена.", buttons=build_main_menu_keyboard())

    @client.on(events.NewMessage(incoming=True, func=_expect_step(context, GroupUploadStep.WAITING_FILE)))
    async def handle_file_upload(event: NewMessage.Event) -> None:
        user_id = event.sender_id
        message_text = (event.raw_text or "").strip()
        if message_text.lower() == CANCEL_LABEL.lower():
            await _handle_cancel(event, context.groups_manager, "Загрузка списка групп отменена.")
            return

        document = event.document
        if document is None:
            context.groups_manager.update(user_id, last_message_id=event.id)
            await event.respond(
                "Пожалуйста, отправьте Excel-файл формата .xlsx или .xls, либо напишите «Отмена».",
                buttons=_build_file_prompt_buttons(),
            )
            return

        filename = _extract_filename(document)
        extension = _detect_extension(document.mime_type, filename)
        if extension not in ALLOWED_EXTENSIONS:
            context.groups_manager.update(user_id, last_message_id=event.id)
            await event.respond(
                "Формат файла не поддерживается. Отправьте Excel-файл (.xlsx или .xls) или напишите «Отмена».",
                buttons=_build_file_prompt_buttons(),
            )
            return

        try:
            file_bytes = await event.download_media(bytes)
        except Exception:
            logger.exception("Не удалось скачать файл со списком групп", extra={"user_id": user_id})
            context.groups_manager.update(user_id, last_message_id=event.id)
            await event.respond(
                "Не удалось скачать файл. Попробуйте ещё раз или отправьте «Отмена».",
                buttons=_build_file_prompt_buttons(),
            )
            return

        try:
            parsed_groups = await _parse_groups_file(file_bytes, extension)
        except Exception:
            logger.exception("Ошибка при чтении Excel с группами", extra={"user_id": user_id})
            context.groups_manager.update(user_id, last_message_id=event.id)
            await event.respond(
                "Не удалось прочитать файл. Убедитесь, что это корректный Excel (.xlsx или .xls).",
                buttons=_build_file_prompt_buttons(),
            )
            return

        if not parsed_groups:
            context.groups_manager.update(user_id, last_message_id=event.id)
            await event.respond(
                "Файл не содержит строк со списком групп. Заполните хотя бы одно поле в каждой строке.",
                buttons=_build_file_prompt_buttons(),
            )
            return

        state = context.groups_manager.get(user_id)
        if state is None or not state.target_session_id:
            logger.warning("Нет целевой сессии для сохранения групп", extra={"user_id": user_id})
            await _handle_cancel(event, context.groups_manager, "Не удалось определить целевой аккаунт. Попробуйте снова.")
            return

        enriched_groups = []
        for group in parsed_groups:
            chat_id, is_member = await _resolve_chat_id(event.client, group.username, group.link)
            enriched_groups.append(_serialize_group(group, chat_id, is_member))

        try:
            await context.session_repository.set_broadcast_groups(state.target_session_id, enriched_groups)
        except Exception:
            logger.exception(
                "Ошибка при сохранении списка групп",
                extra={"user_id": user_id, "session_id": state.target_session_id},
            )
            context.groups_manager.update(user_id, last_message_id=event.id)
            await event.respond(
                "Не удалось сохранить список групп. Попробуйте позже или отправьте «Отмена».",
                buttons=_build_file_prompt_buttons(),
            )
            return

        context.groups_manager.clear(user_id)
        logger.info(
            "Пользователь %s загрузил список групп для сессии %s (%s записей)",
            user_id,
            state.target_session_id,
            len(enriched_groups),
        )
        await event.respond(
            "Список групп для рассылки успешно загружен. Вы можете изменить его, загрузив новый файл.",
            buttons=build_main_menu_keyboard(),
        )


def _detect_extension(mime_type: Optional[str], filename: str) -> str:
    if filename:
        lowered = filename.lower()
        for ext in ALLOWED_EXTENSIONS:
            if lowered.endswith(ext):
                return ext
    if mime_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
        return ".xlsx"
    if mime_type == "application/vnd.ms-excel":
        return ".xls"
    return ""
