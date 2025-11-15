from __future__ import annotations

import asyncio
import logging
import mimetypes
from dataclasses import dataclass
from functools import partial
from io import BytesIO
from typing import Any, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlparse

from telethon.errors import FloodWaitError, RPCError
from telethon.errors.rpcerrorlist import ChatWriteForbiddenError, FileReferenceExpiredError, MediaEmptyError

logger = logging.getLogger(__name__)


class DialogsFetchError(RuntimeError):
    """Raised when dialogs cannot be fetched for a given account."""

    def __init__(self, account_session_id: str, *, account_label: str, original_error: Exception) -> None:
        self.account_session_id = account_session_id
        self.account_label = account_label
        self.original_error = original_error
        self.error_type = original_error.__class__.__name__
        message = (
            f"Failed to fetch dialogs for account {account_label}"
            f" ({account_session_id}): {original_error}"
        )
        super().__init__(message)


def sanitize_username_value(value: object) -> Optional[str]:
    if value is None:
        return None
    username = str(value).strip()
    if not username:
        return None
    username = username.lstrip("@")
    return username or None


def describe_content_payload(has_text: bool, has_image: bool) -> str:
    if has_text and has_image:
        return "текст+фото"
    if has_text:
        return "текст"
    if has_image:
        return "фото"
    return "нет материалов"


def extract_identifier_from_link_value(value: object) -> Optional[str]:
    if value is None:
        return None
    link = str(value).strip()
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


def extract_group_log_context(group: Mapping[str, object]) -> dict[str, Any]:
    if not isinstance(group, Mapping):
        return {}
    chat_id = group.get("chat_id")
    username_value = sanitize_username_value(group.get("username"))
    username = f"@{username_value}" if username_value else None
    link_value = group.get("link")
    link = str(link_value).strip() if isinstance(link_value, str) and link_value.strip() else None
    name_value = group.get("name")
    name = str(name_value).strip() if isinstance(name_value, str) and name_value else None
    return {
        "chat_id": chat_id,
        "chat_username": username,
        "chat_link": link,
        "group_name": name,
    }


@dataclass(slots=True)
class BroadcastImageData:
    """Prepared input media reference for broadcasting images."""

    media: object
    force_document: bool = False
    raw_bytes: Optional[bytes] = None
    file_name: Optional[str] = None
    mime_type: Optional[str] = None


@dataclass(slots=True)
class ResolvedGroupTarget:
    """Resolved chat target for broadcast delivery."""

    entity: object
    group: Mapping[str, object]
    label: str
    log_context: dict[str, Any]


def log_broadcast_event(level: int, message: str, **details: Any) -> None:
    formatted_extra = {f"broadcast_{key}": value for key, value in details.items()}
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        logger.log(level, message, extra=formatted_extra)
    else:
        loop.call_soon(partial(logger.log, level, message, extra=formatted_extra))


def render_group_label(group: Mapping[str, object]) -> str:
    name_value = group.get("name")
    if isinstance(name_value, str) and name_value.strip():
        return name_value.strip()
    username_value = sanitize_username_value(group.get("username"))
    if username_value:
        return f"@{username_value}"
    chat_id = group.get("chat_id")
    if isinstance(chat_id, int):
        return str(chat_id)
    if isinstance(chat_id, str) and chat_id.strip():
        return chat_id.strip()
    link_value = group.get("link")
    if isinstance(link_value, str) and link_value.strip():
        return link_value.strip()
    return "неизвестная группа"


async def resolve_group_targets(
    client,
    group: Mapping[str, object],
    *,
    user_id: int,
    account_label: str,
    account_session_id: str,
    content_type: Optional[str] = None,
    dialogs_cache: Optional[dict[str, list[object]]] = None,
) -> Tuple[list[ResolvedGroupTarget], Optional[str]]:
    base_context = {
        "user_id": user_id,
        "account_label": account_label,
        "account_session_id": account_session_id,
        **extract_group_log_context(group),
    }
    if content_type:
        base_context.setdefault("content_type", content_type)
    log_broadcast_event(logging.INFO, "Проверяем доступ к чату", **base_context)

    targets: list[ResolvedGroupTarget] = []
    duplicates_message: Optional[str] = None

    def _build_target(
        entity,
        *,
        title: Optional[str] = None,
        username: Optional[str] = None,
        chat_id: Optional[int] = None,
    ) -> ResolvedGroupTarget:
        target_group: dict[str, Any] = dict(group)
        if title:
            target_group["name"] = title
        if username:
            target_group["username"] = username
        if chat_id is not None:
            target_group["chat_id"] = chat_id
        label = render_group_label(target_group)
        log_context = dict(base_context)
        log_context.update(
            match_title=title,
            match_username=(f"@{username}" if username else None),
            match_chat_id=chat_id,
            target_label=label,
        )
        return ResolvedGroupTarget(entity=entity, group=target_group, label=label, log_context=log_context)

    chat_id = group.get("chat_id")
    parsed_id: Optional[int] = None
    if isinstance(chat_id, int):
        parsed_id = chat_id
    elif isinstance(chat_id, str) and chat_id.strip():
        try:
            parsed_id = int(chat_id)
        except (TypeError, ValueError):
            parsed_id = None
    if parsed_id is not None:
        try:
            entity = await client.get_input_entity(parsed_id)
        except Exception as exc:
            log_broadcast_event(
                logging.DEBUG,
                "Не удалось получить чат по chat_id",
                error=str(exc),
                **base_context,
            )
        else:
            target = _build_target(entity, title=str(group.get("name") or "") or None, chat_id=parsed_id)
            log_broadcast_event(logging.INFO, "Доступ к чату подтверждён (chat_id)", **target.log_context)
            return [target], None

    username = sanitize_username_value(group.get("username"))
    if username:
        try:
            entity = await client.get_input_entity(username)
        except Exception as exc:
            log_broadcast_event(
                logging.DEBUG,
                "Не удалось получить чат по username",
                error=str(exc),
                **base_context,
            )
        else:
            target = _build_target(entity, title=str(group.get("name") or "") or None, username=username)
            log_broadcast_event(logging.INFO, "Доступ к чату подтверждён (username)", **target.log_context)
            return [target], None

    identifier = extract_identifier_from_link_value(group.get("link"))
    if identifier:
        try:
            entity = await client.get_input_entity(identifier)
        except Exception as exc:
            log_broadcast_event(
                logging.DEBUG,
                "Не удалось получить чат по ссылке",
                error=str(exc),
                **base_context,
            )
        else:
            target = _build_target(entity, title=str(group.get("name") or "") or None)
            log_broadcast_event(logging.INFO, "Доступ к чату подтверждён (ссылка)", **target.log_context)
            return [target], None

    group_name_value = str(group.get("name") or "").strip()
    if group_name_value:
        normalized_target = group_name_value.casefold()
        log_broadcast_event(
            logging.INFO,
            f"Поиск чата \"{group_name_value}\" среди диалогов аккаунта",
            **base_context,
        )
        dialogs_store = dialogs_cache if dialogs_cache is not None else {}
        dialogs = dialogs_store.get(account_session_id)
        if dialogs is None:
            try:
                dialogs = await client.get_dialogs(limit=None)
            except Exception as exc:
                log_broadcast_event(
                    logging.ERROR,
                    "Не удалось получить список диалогов для поиска чата",
                    error=str(exc),
                    **base_context,
                )
                raise DialogsFetchError(
                    account_session_id,
                    account_label=account_label,
                    original_error=exc,
                ) from exc
            else:
                dialogs_store[account_session_id] = dialogs
        matches: list[object] = []
        for dialog in dialogs:
            dialog_entity = getattr(dialog, "entity", dialog)
            dialog_name = getattr(dialog, "name", None) or getattr(dialog_entity, "title", None)
            if not dialog_name:
                continue
            if str(dialog_name).strip().casefold() == normalized_target:
                matches.append(dialog)
        log_broadcast_event(
            logging.INFO,
            f"Найдено совпадений по названию — {len(matches)}",
            search_title=group_name_value,
            **base_context,
        )
        if matches:
            accessible_targets: list[ResolvedGroupTarget] = []
            for dialog in matches:
                matched_entity = getattr(dialog, "entity", dialog)
                match_title = getattr(dialog, "name", None) or getattr(matched_entity, "title", None)
                match_username = getattr(matched_entity, "username", None)
                match_chat_id = getattr(matched_entity, "id", None)
                if getattr(matched_entity, "left", False) or getattr(matched_entity, "kicked", False):
                    log_broadcast_event(
                        logging.WARNING,
                        f"Пропущена группа \"{match_title or group_name_value}\" — нет доступа.",
                        match_chat_id=match_chat_id,
                        match_username=(f"@{match_username}" if match_username else None),
                        **base_context,
                    )
                    continue
                try:
                    entity = await client.get_input_entity(matched_entity)
                except Exception as exc:
                    log_broadcast_event(
                        logging.ERROR,
                        "Не удалось получить entity найденного чата",
                        error=str(exc),
                        match_chat_id=match_chat_id,
                        **base_context,
                    )
                    continue
                target = _build_target(
                    entity,
                    title=str(match_title or group_name_value) or None,
                    username=match_username,
                    chat_id=match_chat_id if isinstance(match_chat_id, int) else None,
                )
                log_broadcast_event(
                    logging.DEBUG,
                    "Совпадение найдено среди диалогов",
                    **target.log_context,
                )
                accessible_targets.append(target)
            if accessible_targets:
                targets.extend(accessible_targets)
                if len(accessible_targets) > 1:
                    duplicates_message = (
                        f"Найдено несколько групп с названием \"{group_name_value}\". "
                        "Отправка выполнена во все совпадения."
                    )
                    log_broadcast_event(
                        logging.INFO,
                        duplicates_message,
                        matches=len(accessible_targets),
                        search_title=group_name_value,
                        **base_context,
                    )
                return targets, duplicates_message
        else:
            log_broadcast_event(
                logging.WARNING,
                "Группа не найдена среди диалогов аккаунта",
                search_title=group_name_value,
                **base_context,
            )

    log_broadcast_event(logging.WARNING, "Не удалось подтвердить доступ к чату", **base_context)
    return [], None


async def send_payload_to_group(
    session_client,
    entity,
    text: Optional[str],
    image_data: Optional[BroadcastImageData],
    *,
    user_id: int,
    account_label: str,
    account_session_id: str,
    group: Mapping[str, object],
    group_label: str,
    content_type: str,
    extra_log_context: Optional[Mapping[str, Any]] = None,
) -> Tuple[bool, Optional[str]]:
    context = {
        "user_id": user_id,
        "account_label": account_label,
        "account_session_id": account_session_id,
        "content_type": content_type,
        **extract_group_log_context(group),
    }
    context.setdefault("target_label", group_label)
    if extra_log_context:
        context.update({k: v for k, v in extra_log_context.items() if v is not None})

    async def _send_once() -> None:
        if image_data is not None:
            if image_data.media is not None:
                await session_client.send_file(
                    entity,
                    file=image_data.media,
                    caption=text or None,
                    force_document=image_data.force_document,
                    parse_mode="html",
                    link_preview=False,
                )
            elif image_data.raw_bytes is not None:
                await _send_from_bytes()
            else:
                raise RuntimeError("Недоступны данные картинки")
        elif text:
            await session_client.send_message(
                entity,
                text,
                parse_mode="html",
                link_preview=False,
            )
        else:
            raise RuntimeError("Нет данных для отправки")

    async def _send_from_bytes() -> None:
        if image_data is None or image_data.raw_bytes is None:
            raise RuntimeError("Нет байт картинки для отправки")
        buffer = BytesIO(image_data.raw_bytes)
        file_name = image_data.file_name
        if not file_name:
            extension = None
            if image_data.mime_type:
                extension = mimetypes.guess_extension(image_data.mime_type)
            if not extension:
                extension = ".jpg" if not image_data.force_document else ".bin"
            file_name = "broadcast" + extension
        buffer.name = file_name
        await session_client.send_file(
            entity,
            file=buffer,
            caption=text or None,
            force_document=image_data.force_document,
            parse_mode="html",
            link_preview=False,
        )

    log_broadcast_event(logging.INFO, f"Начата отправка в группу — {group_label}", **context)

    try:
        await _send_once()
    except FloodWaitError as exc:
        wait_seconds = max(0, int(getattr(exc, "seconds", 0) or 0))
        log_broadcast_event(
            logging.WARNING,
            f"FloodWaitError: ожидание {wait_seconds} секунд",
            error=str(exc),
            error_type=exc.__class__.__name__,
            **context,
        )
        if wait_seconds:
            await asyncio.sleep(wait_seconds)
        try:
            await _send_once()
        except Exception as err:
            log_broadcast_event(
                logging.ERROR,
                "Повторная отправка после FloodWait завершилась ошибкой",
                error=str(err),
                error_type=err.__class__.__name__,
                **context,
            )
            return False, err.__class__.__name__
        else:
            log_broadcast_event(logging.INFO, "Сообщение отправлено после ожидания FloodWait", **context)
            return True, None
    except FileReferenceExpiredError as exc:
        log_broadcast_event(
            logging.WARNING,
            f"Ссылка на файл устарела ({exc.__class__.__name__}), пробуем переотправить из сохранённых данных",
            error=str(exc),
            error_type=exc.__class__.__name__,
            **context,
        )
        if image_data is None or image_data.raw_bytes is None:
            log_broadcast_event(
                logging.ERROR,
                f"Не удалось восстановить файл картинки ({exc.__class__.__name__}): отсутствуют сохранённые байты",
                error=str(exc),
                error_type=exc.__class__.__name__,
                **context,
            )
            return False, exc.__class__.__name__
        try:
            await _send_from_bytes()
        except Exception as err:
            log_broadcast_event(
                logging.ERROR,
                f"Повторная отправка картинки из байтов завершилась ошибкой ({err.__class__.__name__})",
                error=str(err),
                error_type=err.__class__.__name__,
                **context,
            )
            return False, err.__class__.__name__
        else:
            log_broadcast_event(
                logging.INFO,
                "Сообщение отправлено после обновления файла",
                **context,
            )
            return True, None
    except ChatWriteForbiddenError as exc:
        log_broadcast_event(
            logging.ERROR,
            f"Нет прав на отправку в чат ({exc.__class__.__name__})",
            error=str(exc),
            error_type=exc.__class__.__name__,
            **context,
        )
        return False, "нет прав на отправку"
    except RPCError as rpc_error:
        if isinstance(rpc_error, MediaEmptyError) and image_data is not None and image_data.raw_bytes is not None:
            log_broadcast_event(
                logging.WARNING,
                f"Получен MediaEmptyError ({rpc_error.__class__.__name__}), пробуем отправить изображение из сохранённых байтов",
                error=str(rpc_error),
                error_type=rpc_error.__class__.__name__,
                **context,
            )
            try:
                await _send_from_bytes()
            except Exception as err:
                log_broadcast_event(
                    logging.ERROR,
                    f"Повторная отправка после MediaEmptyError не удалась ({err.__class__.__name__})",
                    error=str(err),
                    error_type=err.__class__.__name__,
                    **context,
                )
                return False, err.__class__.__name__
            else:
                log_broadcast_event(
                    logging.INFO,
                    "Сообщение отправлено после повторной загрузки медиа",
                    **context,
                )
                return True, None
        log_broadcast_event(
            logging.ERROR,
            f"Ошибка RPC при отправке сообщения ({rpc_error.__class__.__name__})",
            error=str(rpc_error),
            error_type=rpc_error.__class__.__name__,
            **context,
        )
        return False, rpc_error.__class__.__name__
    except Exception as err:
        log_broadcast_event(
            logging.ERROR,
            f"Не удалось отправить сообщение ({err.__class__.__name__})",
            error=str(err),
            error_type=err.__class__.__name__,
            **context,
        )
        return False, err.__class__.__name__
    else:
        log_broadcast_event(logging.INFO, "Сообщение успешно отправлено", **context)
        return True, None


_log_broadcast = log_broadcast_event
_render_group_label = render_group_label
_extract_group_log_context = extract_group_log_context
_resolve_group_targets = resolve_group_targets
_send_payload_to_group = send_payload_to_group
_describe_content_payload = describe_content_payload
_sanitize_username_value = sanitize_username_value
_extract_identifier_from_link_value = extract_identifier_from_link_value

__all__ = [
    "BroadcastImageData",
    "ResolvedGroupTarget",
    "DialogsFetchError",
    "sanitize_username_value",
    "describe_content_payload",
    "extract_identifier_from_link_value",
    "extract_group_log_context",
    "render_group_label",
    "resolve_group_targets",
    "send_payload_to_group",
    "log_broadcast_event",
    "_log_broadcast",
    "_render_group_label",
    "_extract_group_log_context",
    "_resolve_group_targets",
    "_send_payload_to_group",
    "_describe_content_payload",
    "_sanitize_username_value",
    "_extract_identifier_from_link_value",
]
