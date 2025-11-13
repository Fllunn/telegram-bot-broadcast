from __future__ import annotations

import io
import logging
import math
import re
import secrets
from dataclasses import dataclass
from typing import Iterable, List, Mapping, Optional, Sequence
from urllib.parse import urlparse

import xlrd
from openpyxl import load_workbook
from telethon import Button, events, utils
from telethon.events import NewMessage
from telethon.tl.types import DocumentAttributeFilename

from src.bot.context import BotContext
from src.bot.keyboards import UPLOAD_GROUPS_LABEL, VIEW_GROUPS_LABEL, build_main_menu_keyboard
from src.models.session import TelethonSession
from src.services.groups_state import (
    GroupUploadScope,
    GroupUploadStateManager,
    GroupUploadStep,
    GroupViewSession,
    GroupViewScope,
    GroupViewStateManager,
    GroupViewStep,
)

logger = logging.getLogger(__name__)

CANCEL_LABEL = "Отмена"
UPLOAD_GROUPS_PATTERN = rf"^(?:/upload_groups(?:@\w+)?|{re.escape(UPLOAD_GROUPS_LABEL)})$"
VIEW_GROUPS_PATTERN = rf"^(?:/view_groups(?:@\w+)?|{re.escape(VIEW_GROUPS_LABEL)})$"
UPLOAD_SCOPE_PREFIX = "groups_scope"
UPLOAD_SCOPE_SINGLE = "single"
UPLOAD_SCOPE_ALL = "all"
SELECT_PREFIX = "groups_select"
CONFIRM_PREFIX = "groups_confirm"
CANCEL_PREFIX = "groups_cancel"
VIEW_SCOPE_PREFIX = "view_groups_scope"
VIEW_SCOPE_SINGLE = "single"
VIEW_SCOPE_ALL = "all"
VIEW_SELECT_PREFIX = "view_groups_select"
VIEW_PAGE_PREFIX = "view_groups_page"
VIEW_CANCEL_PREFIX = "view_groups_cancel"

ALLOWED_EXTENSIONS = {".xlsx", ".xls"}
PAGE_SIZE = 10


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


def _build_upload_scope_buttons() -> list[list[Button]]:
    return [
        [
            Button.inline("Один аккаунт", f"{UPLOAD_SCOPE_PREFIX}:{UPLOAD_SCOPE_SINGLE}".encode("utf-8")),
            Button.inline("Все аккаунты", f"{UPLOAD_SCOPE_PREFIX}:{UPLOAD_SCOPE_ALL}".encode("utf-8")),
        ],
        [Button.inline("❌ Отмена", f"{CANCEL_PREFIX}:scope".encode("utf-8"))],
    ]


def _build_upload_account_buttons(sessions: Iterable[TelethonSession]) -> list[list[Button]]:
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


def _build_upload_confirmation_buttons(scope: GroupUploadScope, session_id: Optional[str] = None) -> list[list[Button]]:
    if scope == GroupUploadScope.SINGLE and session_id:
        yes_payload = f"{CONFIRM_PREFIX}:{UPLOAD_SCOPE_SINGLE}:yes:{session_id}".encode("utf-8")
        no_payload = f"{CONFIRM_PREFIX}:{UPLOAD_SCOPE_SINGLE}:no:{session_id}".encode("utf-8")
    else:
        yes_payload = f"{CONFIRM_PREFIX}:{UPLOAD_SCOPE_ALL}:yes".encode("utf-8")
        no_payload = f"{CONFIRM_PREFIX}:{UPLOAD_SCOPE_ALL}:no".encode("utf-8")
    return [[Button.inline("✅ Да", yes_payload), Button.inline("❌ Нет", no_payload)]]


def _build_file_prompt_buttons() -> list[list[Button]]:
    return [[Button.text(CANCEL_LABEL, resize=True)]]


def _build_view_scope_buttons() -> list[list[Button]]:
    return [
        [
            Button.inline("Один аккаунт", f"{VIEW_SCOPE_PREFIX}:{VIEW_SCOPE_SINGLE}".encode("utf-8")),
            Button.inline("Все аккаунты", f"{VIEW_SCOPE_PREFIX}:{VIEW_SCOPE_ALL}".encode("utf-8")),
        ],
        [Button.inline("❌ Отмена", f"{VIEW_CANCEL_PREFIX}:scope".encode("utf-8"))],
    ]


def _build_view_account_buttons(sessions: Iterable[TelethonSession]) -> list[list[Button]]:
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
    rows.append([Button.inline("❌ Отмена", f"{VIEW_CANCEL_PREFIX}:select".encode("utf-8"))])
    return rows


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


def _extract_groups(metadata: Optional[Mapping[str, object]]) -> List[Mapping[str, object]]:
    if not metadata:
        return []
    groups = metadata.get("broadcast_groups") if isinstance(metadata, Mapping) else None
    if isinstance(groups, list):
        normalized: List[Mapping[str, object]] = []
        for entry in groups:
            if isinstance(entry, Mapping):
                normalized.append(dict(entry))
        return normalized
    return []


def _purge_tokens_for_session(state: GroupViewSession, session_id: str) -> None:
    for token, stored_session in list(state.pagination_tokens.items()):
        if stored_session == session_id:
            state.pagination_tokens.pop(token, None)


def _register_pagination_token(state: GroupViewSession, session_id: str) -> str:
    token = secrets.token_hex(4)
    state.pagination_tokens[token] = session_id
    return token


def _build_view_pagination_buttons(state: GroupViewSession, session_id: str, page: int, total_pages: int) -> list[list[Button]]:
    _purge_tokens_for_session(state, session_id)
    buttons: list[list[Button]] = []
    nav_row: list[Button] = []
    if page > 0:
        prev_token = _register_pagination_token(state, session_id)
        nav_row.append(
            Button.inline(
                "‹ Предыдущие 10",
                f"{VIEW_PAGE_PREFIX}:{prev_token}:{page - 1}".encode("utf-8"),
            )
        )
    if page < total_pages - 1:
        next_token = _register_pagination_token(state, session_id)
        nav_row.append(
            Button.inline(
                "Следующие 10 ›",
                f"{VIEW_PAGE_PREFIX}:{next_token}:{page + 1}".encode("utf-8"),
            )
        )
    if nav_row:
        buttons.append(nav_row)
    buttons.append([Button.inline("❌ Закончить просмотр", f"{VIEW_CANCEL_PREFIX}:view".encode("utf-8"))])
    return buttons


def _format_group_entry(index: int, group: Mapping[str, object]) -> str:
    parts: list[str] = []

    name = group.get("name")
    if name is not None:
        name_value = str(name).strip()
        if name_value:
            parts.append(f"Название — \"{name_value}\"")

    username = group.get("username")
    if username is not None:
        username_value = str(username).strip()
        if username_value:
            parts.append(f"Username — @{username_value.lstrip('@')}")

    link = group.get("link")
    if link is not None:
        link_value = str(link).strip()
        if link_value:
            parts.append(f"Ссылка — {link_value}")

    if not parts:
        parts.append("Запись без данных")

    return f"{index}. " + ", ".join(parts)


def _format_groups_page(session: TelethonSession, groups: Sequence[Mapping[str, object]], page: int) -> str:
    label = _render_session_label(session)
    total = len(groups)
    if total == 0:
        return f"Аккаунт {label}\n\nДля этого аккаунта ещё не загружен список групп."

    total_pages = max(1, math.ceil(total / PAGE_SIZE))
    page = max(0, min(page, total_pages - 1))
    start = page * PAGE_SIZE
    end = min(start + PAGE_SIZE, total)

    lines = [
        f"Аккаунт {label}",
        "",
        f"Группы для рассылки ({start + 1}-{end} из {total}):",
    ]

    for idx, group in enumerate(groups[start:end], start=start + 1):
        lines.append(_format_group_entry(idx, group))

    lines.append("")
    lines.append(f"Страница {page + 1} из {total_pages}.")
    return "\n".join(lines)


async def _handle_cancel(event: NewMessage.Event, manager: GroupUploadStateManager, message: str) -> None:
    user_id = event.sender_id
    manager.clear(user_id)
    await event.respond(message, buttons=build_main_menu_keyboard())


def setup_group_commands(client, context: BotContext) -> None:
    """Register commands for uploading and viewing broadcast group lists."""

    upload_manager = context.groups_manager
    view_manager = context.group_view_manager

    async def _get_active_sessions(user_id: int) -> Optional[List[TelethonSession]]:
        try:
            sessions_iter = await context.session_manager.get_active_sessions(user_id)
        except Exception:
            logger.exception(
                "Не удалось получить список активных аккаунтов",
                extra={"user_id": user_id},
            )
            return None
        return list(sessions_iter)

    async def _load_session_and_groups(session_id: str, cache: Mapping[str, TelethonSession]):
        session: Optional[TelethonSession] = None
        try:
            session = await context.session_repository.get_by_session_id(session_id)
        except Exception:
            logger.exception(
                "Не удалось загрузить данные аккаунта для просмотра групп",
                extra={"session_id": session_id},
            )
        if session is None:
            session = cache.get(session_id)
        if session is None:
            return None
        groups = _extract_groups(session.metadata)
        return session, groups

    @client.on(events.NewMessage(pattern=UPLOAD_GROUPS_PATTERN))
    async def handle_upload_groups(event: NewMessage.Event) -> None:
        if not event.is_private:
            return

        user_id = event.sender_id
        if upload_manager.has_active_flow(user_id):
            await event.respond(
                "Вы уже загружаете список групп. Завершите текущий процесс или отправьте «Отмена».",
                buttons=_build_file_prompt_buttons(),
            )
            return

        sessions = await _get_active_sessions(user_id)
        if sessions is None:
            await event.respond(
                "Не удалось получить список аккаунтов. Попробуйте позже.",
                buttons=build_main_menu_keyboard(),
            )
            return
        if not sessions:
            await event.respond(
                "У вас нет подключённых аккаунтов. Подключите аккаунт, чтобы загрузить список групп.",
                buttons=build_main_menu_keyboard(),
            )
            return

        upload_manager.begin(
            user_id,
            step=GroupUploadStep.CHOOSING_SCOPE,
            scope=GroupUploadScope.SINGLE,
            sessions={session.session_id: session for session in sessions},
            last_message_id=event.id,
        )
        message = await event.respond(
            "Для каких аккаунтов загрузить список групп?",
            buttons=_build_upload_scope_buttons(),
        )
        upload_manager.update(user_id, last_message_id=message.id)

    @client.on(events.CallbackQuery(pattern=rf"^{UPLOAD_SCOPE_PREFIX}:".encode("utf-8")))
    async def handle_upload_scope_selection(event: events.CallbackQuery.Event) -> None:
        user_id = event.sender_id
        state = upload_manager.get(user_id)
        if state is None or state.step != GroupUploadStep.CHOOSING_SCOPE:
            await event.answer("Эта операция больше неактуальна.", alert=True)
            return

        selection = event.data.decode("utf-8").split(":", maxsplit=1)[-1]
        sessions = list(state.sessions.values())

        if selection == UPLOAD_SCOPE_SINGLE:
            if not sessions:
                upload_manager.clear(user_id)
                await event.edit("Нет доступных аккаунтов для загрузки.", buttons=build_main_menu_keyboard())
                return
            upload_manager.update(user_id, scope=GroupUploadScope.SINGLE, step=GroupUploadStep.CHOOSING_ACCOUNT)
            message = await event.edit(
                "Выберите аккаунт, для которого нужно загрузить список групп.",
                buttons=_build_upload_account_buttons(sessions),
            )
            upload_manager.update(user_id, last_message_id=message.id)
            return

        if selection == UPLOAD_SCOPE_ALL:
            if not sessions:
                upload_manager.clear(user_id)
                await event.edit("Нет доступных аккаунтов для загрузки.", buttons=build_main_menu_keyboard())
                return
            session_ids = [session.session_id for session in sessions]
            upload_manager.update(
                user_id,
                scope=GroupUploadScope.ALL,
                target_session_ids=session_ids,
            )
            has_existing = any(_extract_groups(session.metadata) for session in sessions)
            if has_existing:
                upload_manager.update(user_id, step=GroupUploadStep.CONFIRMING_REPLACE)
                message = await event.edit(
                    "В некоторых аккаунтах уже есть список групп. Заменить его для всех аккаунтов?",
                    buttons=_build_upload_confirmation_buttons(GroupUploadScope.ALL),
                )
            else:
                upload_manager.update(user_id, step=GroupUploadStep.WAITING_FILE)
                message = await event.edit(
                    "Отправьте Excel-файл (.xlsx или .xls) со списком групп. Первая строка может быть заголовком.",
                    buttons=_build_file_prompt_buttons(),
                )
            upload_manager.update(user_id, last_message_id=message.id)
            return

        await event.answer("Некорректный выбор.", alert=True)

    @client.on(events.CallbackQuery(pattern=rf"^{SELECT_PREFIX}:".encode("utf-8")))
    async def handle_upload_account_selection(event: events.CallbackQuery.Event) -> None:
        user_id = event.sender_id
        state = upload_manager.get(user_id)
        if state is None or state.step != GroupUploadStep.CHOOSING_ACCOUNT:
            await event.answer("Эта операция больше неактуальна.", alert=True)
            return

        session_id = event.data.decode("utf-8").split(":", maxsplit=1)[-1]
        session = state.sessions.get(session_id)
        if session is None:
            await event.answer("Аккаунт не найден.", alert=True)
            return

        upload_manager.update(
            user_id,
            scope=GroupUploadScope.SINGLE,
            target_session_ids=[session_id],
            target_session_id=session_id,
        )

        existing = _extract_groups(session.metadata)
        if existing:
            upload_manager.update(user_id, step=GroupUploadStep.CONFIRMING_REPLACE)
            message = await event.edit(
                "Для выбранного аккаунта уже есть список групп. Заменить его?",
                buttons=_build_upload_confirmation_buttons(GroupUploadScope.SINGLE, session_id),
            )
        else:
            upload_manager.update(user_id, step=GroupUploadStep.WAITING_FILE)
            message = await event.edit(
                "Отправьте Excel-файл (.xlsx или .xls) со списком групп. Первая строка может быть заголовком.",
                buttons=_build_file_prompt_buttons(),
            )
        upload_manager.update(user_id, last_message_id=message.id)

    @client.on(events.CallbackQuery(pattern=rf"^{CONFIRM_PREFIX}:".encode("utf-8")))
    async def handle_upload_confirmation(event: events.CallbackQuery.Event) -> None:
        user_id = event.sender_id
        state = upload_manager.get(user_id)
        if state is None or state.step != GroupUploadStep.CONFIRMING_REPLACE:
            await event.answer("Эта операция больше неактуальна.", alert=True)
            return

        parts = event.data.decode("utf-8").split(":")
        if len(parts) < 3:
            await event.answer("Некорректный запрос.", alert=True)
            return
        _, scope_marker, decision, *rest = parts
        scope = GroupUploadScope.ALL if scope_marker == UPLOAD_SCOPE_ALL else GroupUploadScope.SINGLE

        if decision == "no":
            upload_manager.clear(user_id)
            await event.edit("Загрузка списка групп отменена.", buttons=build_main_menu_keyboard())
            return

        if decision != "yes":
            await event.answer("Некорректный выбор.", alert=True)
            return

        if scope == GroupUploadScope.SINGLE:
            if not rest:
                await event.answer("Некорректный запрос.", alert=True)
                return
            session_id = rest[0]
            if session_id not in (state.target_session_ids or []):
                await event.answer("Некорректный аккаунт.", alert=True)
                return

        upload_manager.update(user_id, step=GroupUploadStep.WAITING_FILE)
        message = await event.edit(
            "Отправьте Excel-файл (.xlsx или .xls) со списком групп. Первая строка может быть заголовком.",
            buttons=_build_file_prompt_buttons(),
        )
        upload_manager.update(user_id, last_message_id=message.id)

    @client.on(events.CallbackQuery(pattern=rf"^{CANCEL_PREFIX}:".encode("utf-8")))
    async def handle_upload_inline_cancel(event: events.CallbackQuery.Event) -> None:
        user_id = event.sender_id
        if not upload_manager.has_active_flow(user_id):
            await event.answer("Нечего отменять.", alert=True)
            return
        upload_manager.clear(user_id)
        await event.edit("Загрузка списка групп отменена.", buttons=build_main_menu_keyboard())

    @client.on(events.NewMessage(incoming=True, func=_expect_step(context, GroupUploadStep.WAITING_FILE)))
    async def handle_upload_file(event: NewMessage.Event) -> None:
        user_id = event.sender_id
        message_text = (event.raw_text or "").strip()
        if message_text.lower() == CANCEL_LABEL.lower():
            await _handle_cancel(event, upload_manager, "Загрузка списка групп отменена.")
            return

        document = event.document
        if document is None:
            upload_manager.update(user_id, last_message_id=event.id)
            await event.respond(
                "Пожалуйста, отправьте Excel-файл формата .xlsx или .xls, либо напишите «Отмена».",
                buttons=_build_file_prompt_buttons(),
            )
            return

        filename = _extract_filename(document)
        extension = _detect_extension(document.mime_type, filename)
        if extension not in ALLOWED_EXTENSIONS:
            upload_manager.update(user_id, last_message_id=event.id)
            await event.respond(
                "Формат файла не поддерживается. Отправьте Excel-файл (.xlsx или .xls) или напишите «Отмена».",
                buttons=_build_file_prompt_buttons(),
            )
            return

        try:
            file_bytes = await event.download_media(bytes)
        except Exception:
            logger.exception("Не удалось скачать файл со списком групп", extra={"user_id": user_id})
            upload_manager.update(user_id, last_message_id=event.id)
            await event.respond(
                "Не удалось скачать файл. Попробуйте ещё раз или отправьте «Отмена».",
                buttons=_build_file_prompt_buttons(),
            )
            return

        try:
            parsed_groups = await _parse_groups_file(file_bytes, extension)
        except Exception:
            logger.exception("Ошибка при чтении Excel с группами", extra={"user_id": user_id})
            upload_manager.update(user_id, last_message_id=event.id)
            await event.respond(
                "Не удалось прочитать файл. Убедитесь, что это корректный Excel (.xlsx или .xls).",
                buttons=_build_file_prompt_buttons(),
            )
            return

        if not parsed_groups:
            upload_manager.update(user_id, last_message_id=event.id)
            await event.respond(
                "Файл не содержит строк со списком групп. Заполните хотя бы одно поле в каждой строке.",
                buttons=_build_file_prompt_buttons(),
            )
            return

        state = upload_manager.get(user_id)
        if state is None:
            logger.warning("Состояние загрузки групп потеряно", extra={"user_id": user_id})
            await _handle_cancel(event, upload_manager, "Не удалось определить целевые аккаунты. Попробуйте снова.")
            return
        target_ids = list(state.target_session_ids or [])
        if not target_ids and state.target_session_id:
            target_ids = [state.target_session_id]
        if not target_ids:
            logger.warning("Нет целевых аккаунтов для сохранения групп", extra={"user_id": user_id})
            await _handle_cancel(event, upload_manager, "Не удалось определить целевые аккаунты. Попробуйте снова.")
            return

        enriched_groups = []
        for group in parsed_groups:
            chat_id, is_member = await _resolve_chat_id(event.client, group.username, group.link)
            enriched_groups.append(_serialize_group(group, chat_id, is_member))

        try:
            if state.scope == GroupUploadScope.ALL:
                updated = await context.session_repository.set_broadcast_groups_bulk(target_ids, enriched_groups)
                if updated == 0:
                    raise RuntimeError("Не удалось обновить ни один аккаунт")
            else:
                success = await context.session_repository.set_broadcast_groups(target_ids[0], enriched_groups)
                if not success:
                    raise RuntimeError("Не удалось обновить выбранный аккаунт")
        except Exception:
            logger.exception(
                "Ошибка при сохранении списка групп",
                extra={"user_id": user_id, "scope": state.scope.value, "targets": target_ids},
            )
            upload_manager.update(user_id, last_message_id=event.id)
            await event.respond(
                "Не удалось сохранить список групп. Попробуйте позже или отправьте «Отмена».",
                buttons=_build_file_prompt_buttons(),
            )
            return

        if state.scope == GroupUploadScope.ALL:
            success_text = "Список групп для рассылки успешно загружен для всех подключённых аккаунтов."
        else:
            session_obj = state.sessions.get(target_ids[0])
            label = _render_session_label(session_obj) if session_obj else "выбранного аккаунта"
            success_text = f"Список групп для аккаунта {label} успешно обновлён."

        logger.info(
            "Пользователь %s загрузил список групп (scope=%s, count=%s)",
            user_id,
            state.scope.value,
            len(enriched_groups),
        )

        upload_manager.clear(user_id)
        await event.respond(success_text, buttons=build_main_menu_keyboard())

    @client.on(events.NewMessage(pattern=VIEW_GROUPS_PATTERN))
    async def handle_view_groups(event: NewMessage.Event) -> None:
        if not event.is_private:
            return

        user_id = event.sender_id
        if view_manager.has_active_flow(user_id):
            await event.respond(
                "Вы уже просматриваете списки групп. Завершите текущий просмотр или используйте кнопку «❌ Закончить просмотр».",
                buttons=build_main_menu_keyboard(),
            )
            return

        sessions = await _get_active_sessions(user_id)
        if sessions is None:
            await event.respond(
                "Не удалось получить список аккаунтов. Попробуйте позже.",
                buttons=build_main_menu_keyboard(),
            )
            return
        if not sessions:
            await event.respond(
                "У вас нет подключённых аккаунтов. Подключите аккаунт, чтобы просматривать списки групп.",
                buttons=build_main_menu_keyboard(),
            )
            return

        view_manager.begin(
            user_id,
            step=GroupViewStep.CHOOSING_SCOPE,
            session_ids=[session.session_id for session in sessions],
            sessions={session.session_id: session for session in sessions},
            last_message_id=event.id,
        )
        message = await event.respond(
            "Для каких аккаунтов показать сохранённые группы?",
            buttons=_build_view_scope_buttons(),
        )
        view_manager.update(user_id, last_message_id=message.id)

    @client.on(events.CallbackQuery(pattern=rf"^{VIEW_SCOPE_PREFIX}:".encode("utf-8")))
    async def handle_view_scope_selection(event: events.CallbackQuery.Event) -> None:
        user_id = event.sender_id
        state = view_manager.get(user_id)
        if state is None or state.step != GroupViewStep.CHOOSING_SCOPE:
            await event.answer("Эта операция больше неактуальна.", alert=True)
            return

        selection = event.data.decode("utf-8").split(":", maxsplit=1)[-1]
        sessions = list(state.sessions.values())

        if selection == VIEW_SCOPE_SINGLE:
            if not sessions:
                view_manager.clear(user_id)
                await event.edit("Нет доступных аккаунтов для просмотра.", buttons=build_main_menu_keyboard())
                return
            view_manager.update(user_id, scope=GroupViewScope.SINGLE, step=GroupViewStep.CHOOSING_ACCOUNT)
            message = await event.edit(
                "Выберите аккаунт, для которого показать сохранённые группы.",
                buttons=_build_view_account_buttons(sessions),
            )
            view_manager.update(user_id, last_message_id=message.id)
            return

        if selection == VIEW_SCOPE_ALL:
            if not sessions:
                view_manager.clear(user_id)
                await event.edit("Нет доступных аккаунтов для просмотра.", buttons=build_main_menu_keyboard())
                return
            view_manager.update(user_id, scope=GroupViewScope.ALL, step=GroupViewStep.VIEWING)
            await event.edit(
                "Показываю списки групп для всех подключённых аккаунтов. Используйте кнопки под сообщениями для навигации.",
            )
            for session in sessions:
                loaded = await _load_session_and_groups(session.session_id, state.sessions)
                if loaded is None:
                    await event.client.send_message(
                        user_id,
                        f"Не удалось получить данные для аккаунта {_render_session_label(session)}. Попробуйте позже.",
                    )
                    continue
                session_obj, groups = loaded
                state.sessions[session_obj.session_id] = session_obj
                total_pages = max(1, math.ceil(len(groups) / PAGE_SIZE))
                text = _format_groups_page(session_obj, groups, 0)
                buttons = _build_view_pagination_buttons(state, session_obj.session_id, 0, total_pages)
                await event.client.send_message(user_id, text, buttons=buttons)
            return

        await event.answer("Некорректный выбор.", alert=True)

    @client.on(events.CallbackQuery(pattern=rf"^{VIEW_SELECT_PREFIX}:".encode("utf-8")))
    async def handle_view_account_selection(event: events.CallbackQuery.Event) -> None:
        user_id = event.sender_id
        state = view_manager.get(user_id)
        if state is None or state.step != GroupViewStep.CHOOSING_ACCOUNT:
            await event.answer("Эта операция больше неактуальна.", alert=True)
            return

        session_id = event.data.decode("utf-8").split(":", maxsplit=1)[-1]
        loaded = await _load_session_and_groups(session_id, state.sessions)
        if loaded is None:
            view_manager.clear(user_id)
            await event.edit(
                "Не удалось получить данные выбранного аккаунта. Попробуйте позже.",
                buttons=build_main_menu_keyboard(),
            )
            return

        session_obj, groups = loaded
        state.sessions[session_obj.session_id] = session_obj
        view_manager.update(
            user_id,
            scope=GroupViewScope.SINGLE,
            step=GroupViewStep.VIEWING,
            session_ids=[session_obj.session_id],
        )
        await event.edit(
            f"Аккаунт {_render_session_label(session_obj)} выбран. Отображаю список групп.",
        )
        total_pages = max(1, math.ceil(len(groups) / PAGE_SIZE))
        text = _format_groups_page(session_obj, groups, 0)
        buttons = _build_view_pagination_buttons(state, session_obj.session_id, 0, total_pages)
        await event.client.send_message(user_id, text, buttons=buttons)

    @client.on(events.CallbackQuery(pattern=rf"^{VIEW_PAGE_PREFIX}:".encode("utf-8")))
    async def handle_view_pagination(event: events.CallbackQuery.Event) -> None:
        user_id = event.sender_id
        try:
            text = event.data.decode("utf-8")
        except UnicodeDecodeError:
            await event.answer("Некорректный запрос.", alert=True)
            return

        parts = text.split(":", maxsplit=2)
        if len(parts) != 3 or parts[0] != VIEW_PAGE_PREFIX:
            await event.answer("Некорректный запрос.", alert=True)
            return

        _, token, page_str = parts
        state = view_manager.get(user_id)
        if state is None:
            await event.answer("Сеанс просмотра устарел. Начните заново командой /view_groups.", alert=True)
            return

        session_id = state.pagination_tokens.pop(token, None)
        if session_id is None:
            logger.warning(
                "Получен неизвестный токен пагинации",
                extra={"sender_id": user_id, "token": token},
            )
            await event.answer("Сеанс просмотра устарел. Начните заново командой /view_groups.", alert=True)
            return

        if state.step != GroupViewStep.VIEWING:
            await event.answer("Эта операция больше неактуальна.", alert=True)
            return

        loaded = await _load_session_and_groups(session_id, state.sessions)
        if loaded is None:
            await event.answer("Не удалось получить данные аккаунта.", alert=True)
            return

        session_obj, groups = loaded
        if session_obj.owner_id != user_id:
            logger.warning(
                "Попытка просмотра списка групп чужого аккаунта",
                extra={"sender_id": user_id, "owner_id": session_obj.owner_id, "session_id": session_obj.session_id},
            )
            await event.answer("Нет доступа к этому аккаунту.", alert=True)
            return

        state.sessions[session_obj.session_id] = session_obj
        if session_obj.session_id not in state.session_ids:
            state.session_ids.append(session_obj.session_id)

        try:
            requested_page = int(page_str)
        except ValueError:
            await event.answer("Некорректный запрос.", alert=True)
            return

        total_pages = max(1, math.ceil(len(groups) / PAGE_SIZE))
        page = max(0, min(requested_page, total_pages - 1))
        text = _format_groups_page(session_obj, groups, page)
        buttons = _build_view_pagination_buttons(state, session_obj.session_id, page, total_pages)
        await event.edit(text, buttons=buttons)
        await event.answer()

    @client.on(events.CallbackQuery(pattern=rf"^{VIEW_CANCEL_PREFIX}:".encode("utf-8")))
    async def handle_view_cancel(event: events.CallbackQuery.Event) -> None:
        user_id = event.sender_id
        if not view_manager.has_active_flow(user_id):
            await event.answer("Нечего отменять.", alert=True)
            return
        view_manager.clear(user_id)
        await event.edit("Просмотр списков групп завершён.", buttons=build_main_menu_keyboard())


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
