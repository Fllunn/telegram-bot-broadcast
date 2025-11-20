from __future__ import annotations

import asyncio
import logging
import mimetypes
from dataclasses import dataclass
from functools import partial
from io import BytesIO
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple
from urllib.parse import urlparse

from telethon import utils
from telethon.errors import FloodWaitError, RPCError
from telethon.errors.rpcerrorlist import (
    ChatSendMediaForbiddenError,
    ChatWriteForbiddenError,
    FileReferenceExpiredError,
    FileReferenceInvalidError,
    MediaEmptyError,
    PhotoInvalidDimensionsError,
)
from telethon.extensions import markdown as markdown_ext
from telethon.tl import types as tl_types

logger = logging.getLogger(__name__)

_KEY_INFO_PREFIXES: tuple[str, ...] = (
    "Рассылка запущена",
    "Рассылка завершена",
)

_MEDIA_ERROR_KEYWORDS: Tuple[str, ...] = (
    "MEDIA",
    "PHOTO",
    "IMAGE",
    "DOCUMENT",
    "ALBUM",
    "STICKER",
    "FILE",
    "GIF",
    "VIDEO",
    "WEBDOCUMENT",
)

_MEDIA_RETRY_KEYWORDS: Tuple[str, ...] = (
    "FILE_REFERENCE",
    "FILE_PART",
    "FILE_ID",
    "MEDIA_EMPTY",
    "MEDIA_INVALID",
    "PHOTO_INVALID",
    "UPLOAD",
)


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


def _normalize_chat_id_value(value: object) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    try:
        string = str(value).strip()
    except Exception:
        return None
    if not string:
        return None
    if string.endswith(".0"):
        string = string[:-2]
    try:
        return int(string)
    except (TypeError, ValueError):
        return None


def _group_identity_tuple(group: Mapping[str, object]) -> Tuple[str, object]:
    chat_id = _normalize_chat_id_value(group.get("chat_id"))
    if chat_id is not None:
        return ("id", chat_id)

    username = sanitize_username_value(group.get("username"))
    if username:
        return ("username", username.casefold())

    link_identifier = extract_identifier_from_link_value(group.get("link"))
    if link_identifier:
        return ("link", link_identifier.casefold())

    raw_link = group.get("link")
    if isinstance(raw_link, str) and raw_link.strip():
        return ("raw_link", raw_link.strip().casefold())

    name_value = group.get("name")
    if isinstance(name_value, str) and name_value.strip():
        return ("name", name_value.strip().casefold())

    fallback: List[Tuple[str, str]] = []
    for key, value in group.items():
        try:
            string_key = str(key)
        except Exception:
            string_key = repr(key)
        try:
            string_value = str(value)
        except Exception:
            string_value = repr(value)
        fallback.append((string_key, string_value))
    fallback.sort()
    return ("fallback", tuple(fallback))


def deduplicate_broadcast_groups(groups: Sequence[Mapping[str, object]]) -> List[dict[str, Any]]:
    if not groups:
        return []
    unique: Dict[Tuple[str, object], dict[str, Any]] = {}
    order: List[Tuple[str, object]] = []
    for entry in groups:
        if not isinstance(entry, Mapping):
            continue
        identity = _group_identity_tuple(entry)
        payload = dict(entry)
        occurrences_raw = payload.get("source_occurrences")
        try:
            initial_occurrences = int(occurrences_raw) if occurrences_raw is not None else 1
        except (TypeError, ValueError):
            initial_occurrences = 1
        if identity not in unique:
            payload_copy = dict(payload)
            payload_copy["source_occurrences"] = max(1, initial_occurrences)
            unique[identity] = payload_copy
            order.append(identity)
        else:
            stored = unique[identity]
            stored_occurrences = stored.get("source_occurrences", 1)
            try:
                stored_count = int(stored_occurrences)
            except (TypeError, ValueError):
                stored_count = 1
            stored["source_occurrences"] = stored_count + max(1, initial_occurrences)
            for key, value in payload.items():
                if key not in stored:
                    stored[key] = value
    return [unique[key] for key in order]



def _prepare_broadcast_text(raw_text: Optional[str]) -> PreparedMessageContent:
    if raw_text is None:
        return PreparedMessageContent(text=None, entities=())
    text = raw_text if isinstance(raw_text, str) else str(raw_text)
    if not text:
        return PreparedMessageContent(text=None, entities=())
    try:
        parsed_text, entities = markdown_ext.parse(text)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.warning(
            "Не удалось разобрать текст рассылки через markdown.parse",
            extra={"error": str(exc), "error_type": exc.__class__.__name__},
            exc_info=True,
        )
        return PreparedMessageContent(text=text, entities=())
    normalized_text = parsed_text or None
    normalized_entities: Tuple[tl_types.TypeMessageEntity, ...] = tuple(entities or ())
    return PreparedMessageContent(text=normalized_text, entities=normalized_entities)


def _is_media_related_error(error: BaseException) -> bool:
    if isinstance(error, ChatWriteForbiddenError):
        return True
    if isinstance(error, (ChatSendMediaForbiddenError, MediaEmptyError, PhotoInvalidDimensionsError)):
        return True
    if isinstance(error, RPCError):
        name = error.__class__.__name__.upper()
        if any(keyword in name for keyword in _MEDIA_ERROR_KEYWORDS):
            return True
        message = getattr(error, "rpc_error", None) or getattr(error, "message", None)
        if isinstance(message, str) and any(keyword in message.upper() for keyword in _MEDIA_ERROR_KEYWORDS):
            return True
        error_text = str(error)
        if any(keyword in error_text.upper() for keyword in _MEDIA_ERROR_KEYWORDS):
            return True
    return False


def _should_retry_media_from_bytes(error: BaseException) -> bool:
    if isinstance(error, (FileReferenceExpiredError, FileReferenceInvalidError, MediaEmptyError)):
        return True
    if isinstance(error, RPCError):
        name = error.__class__.__name__.upper()
        if any(keyword in name for keyword in _MEDIA_RETRY_KEYWORDS):
            return True
        message = getattr(error, "rpc_error", None) or getattr(error, "message", None)
        if isinstance(message, str) and any(keyword in message.upper() for keyword in _MEDIA_RETRY_KEYWORDS):
            return True
        error_text = str(error)
        if any(keyword in error_text.upper() for keyword in _MEDIA_RETRY_KEYWORDS):
            return True
    return False


@dataclass(slots=True)
class BroadcastImageData:
    """Prepared input media reference for broadcasting images."""

    media: object
    force_document: bool = False
    raw_bytes: Optional[bytes] = None
    file_name: Optional[str] = None
    mime_type: Optional[str] = None


@dataclass(slots=True)
class PreparedMessageContent:
    """Represents parsed text and entities ready for Telethon delivery."""

    text: Optional[str]
    entities: Tuple[tl_types.TypeMessageEntity, ...] = ()


@dataclass(slots=True)
class ResolvedGroupTarget:
    """Resolved chat target for broadcast delivery."""

    entity: object
    group: Mapping[str, object]
    label: str
    log_context: dict[str, Any]


@dataclass(slots=True)
class BroadcastAttemptOutcome:
    success: bool
    reason: Optional[str]
    error: Optional[BaseException]


@dataclass(slots=True)
class BroadcastSendResult:
    success: bool
    attempts: int
    transient_errors: List[str]
    final_error: Optional[str]
    final_exception: Optional[BaseException]


DEFAULT_MAX_SEND_ATTEMPTS = 3
DEFAULT_RETRY_BASE_DELAY = 1.5
MAX_RETRY_BACKOFF_SECONDS = 10.0
TRANSIENT_ERROR_REASONS: Set[str] = {
    "FloodWaitError",
    "RetryAfter",
    "TimedOutError",
    "TimeoutError",
    "ServerError",
    "RpcCallFailError",
    "SlowModeWaitError",
    "PhoneMigrateError",
}
TRANSIENT_ERROR_TYPES: Tuple[type[BaseException], ...] = (
    asyncio.TimeoutError,
    TimeoutError,
    ConnectionError,
    FloodWaitError,
)


def _resolved_target_identity(target: ResolvedGroupTarget) -> tuple[str, object | tuple]:
    try:
        peer_id = utils.get_peer_id(target.entity)
    except Exception:
        peer_id = None
    if peer_id is not None:
        return ("peer", int(peer_id))
    match_chat_id = target.log_context.get("match_chat_id")
    if match_chat_id is not None:
        return ("match_chat_id", match_chat_id)
    match_username = target.log_context.get("match_username")
    if match_username:
        username = str(match_username).strip().lstrip("@")
        if username:
            return ("match_username", username.casefold())
    group_chat_id = target.group.get("chat_id") if isinstance(target.group, Mapping) else None
    if isinstance(group_chat_id, int):
        return ("group_chat_id", group_chat_id)
    group_username = sanitize_username_value(target.group.get("username")) if isinstance(target.group, Mapping) else None
    if group_username:
        return ("group_username", group_username.casefold())
    label = target.label.strip().casefold() if isinstance(target.label, str) else repr(target.label)
    return ("label", label)


def resolved_target_identity(target: ResolvedGroupTarget) -> tuple[str, object | tuple]:
    """Public helper returning identity tuple for a resolved broadcast target."""

    return _resolved_target_identity(target)


async def collect_unique_target_peer_keys(
    client,
    groups: Sequence[Mapping[str, object]],
    *,
    user_id: int,
    account_label: str,
    account_session_id: str,
    content_type: Optional[str] = None,
    dialogs_cache: Optional[dict[str, list[object]]] = None,
) -> Set[tuple[str, object | tuple]]:
    if not groups:
        return set()
    dialogs_store = dialogs_cache if dialogs_cache is not None else {}
    peer_keys: Set[tuple[str, object | tuple]] = set()
    for group in groups:
        if not isinstance(group, Mapping):
            continue
        targets, _ = await resolve_group_targets(
            client,
            group,
            user_id=user_id,
            account_label=account_label,
            account_session_id=account_session_id,
            content_type=content_type,
            dialogs_cache=dialogs_store,
        )
        for target in targets:
            peer_keys.add(_resolved_target_identity(target))
    return peer_keys


def log_broadcast_event(level: int, message: str, **details: Any) -> None:
    if level == logging.INFO and not _should_keep_info_message(message):
        level = logging.DEBUG
    formatted_extra = {f"broadcast_{key}": value for key, value in details.items()}
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        logger.log(level, message, extra=formatted_extra)
    else:
        loop.call_soon(partial(logger.log, level, message, extra=formatted_extra))


def _should_keep_info_message(message: object) -> bool:
    if not isinstance(message, str):
        return False
    return any(message.startswith(prefix) for prefix in _KEY_INFO_PREFIXES)


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


async def _send_payload_once(
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
) -> BroadcastAttemptOutcome:
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

    prepared_text = _prepare_broadcast_text(text)

    async def _send_text_message(allow_entities: bool = True) -> None:
        if not prepared_text.text:
            raise RuntimeError("Нет текста для отправки")
        formatting_entities = prepared_text.entities if allow_entities and prepared_text.entities else None
        try:
            await session_client.send_message(
                entity,
                prepared_text.text,
                formatting_entities=formatting_entities,
                parse_mode=None,
                link_preview=False,
            )
        except TypeError as exc:
            log_broadcast_event(
                logging.WARNING,
                "Форматирование текста недоступно, отправляем без entities",
                error=str(exc),
                error_type=exc.__class__.__name__,
                **context,
            )
            if allow_entities and prepared_text.entities:
                await _send_text_message(False)
            else:
                raise

    async def _send_file_from_media(allow_entities: bool = True) -> None:
        if image_data is None or image_data.media is None:
            raise RuntimeError("Медиа отсутствует для отправки")
        caption = prepared_text.text or None
        caption_entities = prepared_text.entities if allow_entities and caption else None
        try:
            await session_client.send_file(
                entity,
                file=image_data.media,
                caption=caption,
                caption_entities=caption_entities,
                force_document=image_data.force_document,
                parse_mode=None,
            )
        except TypeError as exc:
            log_broadcast_event(
                logging.WARNING,
                "Ошибка применения entities к подписи, пробуем без форматирования",
                error=str(exc),
                error_type=exc.__class__.__name__,
                **context,
            )
            if allow_entities and caption_entities:
                await _send_file_from_media(False)
            else:
                raise

    async def _send_file_from_bytes(allow_entities: bool = True) -> None:
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
        caption = prepared_text.text or None
        caption_entities = prepared_text.entities if allow_entities and caption else None
        try:
            await session_client.send_file(
                entity,
                file=buffer,
                caption=caption,
                caption_entities=caption_entities,
                force_document=image_data.force_document,
                parse_mode=None,
            )
        except TypeError as exc:
            log_broadcast_event(
                logging.WARNING,
                "Ошибка применения entities к подписи из байтов, пробуем без форматирования",
                error=str(exc),
                error_type=exc.__class__.__name__,
                **context,
            )
            if allow_entities and caption_entities:
                await _send_file_from_bytes(False)
            else:
                raise

    async def _fallback_to_text(
        trigger_error: BaseException,
        *,
        message: str,
        log_level: int = logging.DEBUG,
    ) -> None:
        if not prepared_text.text:
            raise trigger_error
        log_broadcast_event(
            log_level,
            message,
            error=str(trigger_error),
            error_type=trigger_error.__class__.__name__,
            fallback_mode="text_only",
            **context,
        )
        await _send_text_message()

    async def _send_media_with_optional_fallback() -> None:
        if image_data is None:
            raise RuntimeError("Нет данных для отправки")
        try:
            if image_data.media is not None:
                await _send_file_from_media()
            elif image_data.raw_bytes is not None:
                await _send_file_from_bytes()
            else:
                raise RuntimeError("Недоступны данные картинки")
        except Exception as exc:
            if _should_retry_media_from_bytes(exc):
                retry_message = "Ссылка на медиа недействительна, пробуем повторную загрузку из байтов"
                if isinstance(exc, FileReferenceExpiredError):
                    retry_message = "Ссылка на медиа устарела, пытаемся отправить из байтов"
                if image_data.raw_bytes is None:
                    await _fallback_to_text(
                        exc,
                        message="Ссылка на медиа недоступна, сохраняем доставку только текстом",
                    )
                    return
                log_broadcast_event(
                    logging.DEBUG,
                    retry_message,
                    error=str(exc),
                    error_type=exc.__class__.__name__,
                    **context,
                )
                try:
                    await _send_file_from_bytes()
                except Exception as upload_exc:
                    if isinstance(upload_exc, TypeError):
                        await _fallback_to_text(
                            upload_exc,
                            message="Ошибка обработки entities при повторной загрузке медиа, переключаемся на текст",
                            log_level=logging.DEBUG,
                        )
                        return
                    if _is_media_related_error(upload_exc):
                        await _fallback_to_text(
                            upload_exc,
                            message="Отправка медиа невозможна после повторной загрузки, выполняем fallback на текст",
                            log_level=logging.DEBUG,
                        )
                        return
                    raise
                return
            if isinstance(exc, TypeError):
                await _fallback_to_text(
                    exc,
                    message="Ошибка обработки entities при отправке медиа, переключаемся на текст",
                    log_level=logging.DEBUG,
                )
                return
            if _is_media_related_error(exc):
                await _fallback_to_text(
                    exc,
                    message="Отправка медиа невозможна, выполняем fallback на текст",
                    log_level=logging.DEBUG,
                )
                return
            raise

    async def _send_once() -> None:
        if image_data is not None:
            await _send_media_with_optional_fallback()
            return
        if prepared_text.text:
            await _send_text_message()
            return
        raise RuntimeError("Нет данных для отправки")

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
            return BroadcastAttemptOutcome(False, err.__class__.__name__, err)
        else:
            log_broadcast_event(logging.INFO, "Сообщение отправлено после ожидания FloodWait", **context)
            return BroadcastAttemptOutcome(True, None, None)
    except FileReferenceExpiredError as exc:
        log_broadcast_event(
            logging.ERROR,
            "Медиа недоступно: устаревшая ссылка, отсутствуют байты и fallback-текст",
            error=str(exc),
            error_type=exc.__class__.__name__,
            **context,
        )
        return BroadcastAttemptOutcome(False, exc.__class__.__name__, exc)
    except ChatWriteForbiddenError as exc:
        log_broadcast_event(
            logging.ERROR,
            f"Нет прав на отправку в чат ({exc.__class__.__name__})",
            error=str(exc),
            error_type=exc.__class__.__name__,
            **context,
        )
        return BroadcastAttemptOutcome(False, "нет прав на отправку", exc)
    except RPCError as rpc_error:
        log_broadcast_event(
            logging.ERROR,
            f"Ошибка RPC при отправке сообщения ({rpc_error.__class__.__name__})",
            error=str(rpc_error),
            error_type=rpc_error.__class__.__name__,
            **context,
        )
        return BroadcastAttemptOutcome(False, rpc_error.__class__.__name__, rpc_error)
    except Exception as err:
        log_broadcast_event(
            logging.ERROR,
            f"Не удалось отправить сообщение ({err.__class__.__name__})",
            error=str(err),
            error_type=err.__class__.__name__,
            **context,
        )
        return BroadcastAttemptOutcome(False, err.__class__.__name__, err)
    else:
        log_broadcast_event(logging.INFO, "Сообщение успешно отправлено", **context)
        return BroadcastAttemptOutcome(True, None, None)


def _is_transient_failure(outcome: BroadcastAttemptOutcome) -> bool:
    if outcome.success:
        return False
    if outcome.error and isinstance(outcome.error, TRANSIENT_ERROR_TYPES):
        return True
    if outcome.reason and outcome.reason in TRANSIENT_ERROR_REASONS:
        return True
    return False


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
    max_attempts: int = DEFAULT_MAX_SEND_ATTEMPTS,
    retry_delay: float = DEFAULT_RETRY_BASE_DELAY,
) -> BroadcastSendResult:
    attempts = 0
    transient_errors: List[str] = []
    last_outcome: Optional[BroadcastAttemptOutcome] = None
    max_attempts = max(1, int(max_attempts))
    delay = max(0.0, retry_delay)

    for attempt in range(1, max_attempts + 1):
        last_outcome = await _send_payload_once(
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
        attempts = attempt
        if last_outcome.success:
            return BroadcastSendResult(True, attempts, transient_errors, None, None)

        is_transient = _is_transient_failure(last_outcome)
        if is_transient:
            reason_label = last_outcome.reason
            if not reason_label and last_outcome.error is not None:
                reason_label = last_outcome.error.__class__.__name__
            if reason_label:
                transient_errors.append(reason_label)
        else:
            break

        if attempt >= max_attempts:
            break

        if delay > 0:
            await asyncio.sleep(min(MAX_RETRY_BACKOFF_SECONDS, delay))
            delay = min(MAX_RETRY_BACKOFF_SECONDS, delay * 2 or DEFAULT_RETRY_BASE_DELAY)

    final_error: Optional[str] = None
    final_exception: Optional[BaseException] = None
    if last_outcome is not None:
        if last_outcome.reason:
            final_error = last_outcome.reason
        elif last_outcome.error is not None:
            final_error = last_outcome.error.__class__.__name__
        final_exception = last_outcome.error
    if attempts == 0:
        attempts = 1
    return BroadcastSendResult(False, attempts, transient_errors, final_error, final_exception)


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
    "BroadcastAttemptOutcome",
    "BroadcastSendResult",
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
    "deduplicate_broadcast_groups",
    "collect_unique_target_peer_keys",
    "_log_broadcast",
    "_render_group_label",
    "_extract_group_log_context",
    "_resolve_group_targets",
    "_send_payload_to_group",
    "_describe_content_payload",
    "_sanitize_username_value",
    "_extract_identifier_from_link_value",
]
