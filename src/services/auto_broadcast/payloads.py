from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass
from typing import Mapping, Optional

from telethon.tl import types as tl_types


@dataclass(slots=True)
class ImagePayload:
    """Represents media payload ready for sending."""

    media: object | None
    force_document: bool
    raw_bytes: bytes | None
    file_name: Optional[str]
    mime_type: Optional[str]
    is_legacy: bool = False


def _decode_file_reference(value: object) -> Optional[bytes]:
    if value is None:
        return b""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, str) and value:
        try:
            return base64.b64decode(value.encode("ascii"))
        except (ValueError, binascii.Error):
            return None
    return None


def extract_image_metadata(metadata: Mapping[str, object] | None) -> Mapping[str, object] | None:
    if not metadata:
        return None
    image_meta = metadata.get("broadcast_image") if isinstance(metadata, Mapping) else None
    if isinstance(image_meta, Mapping):
        return dict(image_meta)
    legacy = metadata.get("broadcast_image_file_id") if isinstance(metadata, Mapping) else None
    if isinstance(legacy, str) and legacy.strip():
        return {"legacy_file_id": legacy}
    return None


def _as_int(value: object) -> Optional[int]:
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def build_input_media(image_meta: Mapping[str, object]) -> tuple[object | None, bool]:
    if "legacy_file_id" in image_meta:
        return image_meta.get("legacy_file_id"), True

    media_type = image_meta.get("type")
    media_id = _as_int(image_meta.get("id"))
    access_hash = _as_int(image_meta.get("access_hash"))
    file_reference = _decode_file_reference(image_meta.get("file_reference"))
    if media_id is None or access_hash is None or file_reference is None:
        return None, False

    if media_type == "photo":
        return tl_types.InputPhoto(media_id, access_hash, file_reference), False
    if media_type == "document":
        return tl_types.InputDocument(media_id, access_hash, file_reference), False
    return None, False


def prepare_image_payload(image_meta: Mapping[str, object]) -> ImagePayload:
    media, is_legacy = build_input_media(image_meta)
    raw_bytes: bytes | None = None
    file_name = None
    mime_type = None

    encoded = image_meta.get("data_b64")
    if isinstance(encoded, str) and encoded:
        try:
            raw_bytes = base64.b64decode(encoded.encode("ascii"))
        except (ValueError, binascii.Error):
            raw_bytes = None

    file_name_value = image_meta.get("file_name")
    if isinstance(file_name_value, str) and file_name_value.strip():
        file_name = file_name_value.strip()

    mime_type_value = image_meta.get("mime_type")
    if isinstance(mime_type_value, str) and mime_type_value.strip():
        mime_type = mime_type_value.strip()

    force_document = (
        bool(image_meta.get("force_document"))
        or isinstance(media, tl_types.InputDocument)
        or image_meta.get("type") == "document"
    )

    return ImagePayload(
        media=media,
        force_document=force_document,
        raw_bytes=raw_bytes,
        file_name=file_name,
        mime_type=mime_type,
        is_legacy=is_legacy,
    )
