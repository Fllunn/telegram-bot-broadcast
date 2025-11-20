from __future__ import annotations

import asyncio
import os
import re
from typing import List, Optional, Tuple

import aiohttp

from aiohttp import ClientError


class GoogleSheetsError(Exception):
    pass


class InvalidLinkError(GoogleSheetsError):
    pass


class PublicAccessRequiredError(GoogleSheetsError):
    pass


class NotFoundError(GoogleSheetsError):
    pass


class FetchError(GoogleSheetsError):
    pass


_SHEETS_HOSTS = {"docs.google.com"}
_SHEETS_ID_RE = re.compile(r"/spreadsheets/d/([a-zA-Z0-9-_]+)")
_GID_RE = re.compile(r"(?:[?#&]gid=)(\d+)")


def is_google_sheets_link(text: str) -> bool:
    if not text:
        return False
    text = text.strip()
    if not (text.startswith("http://") or text.startswith("https://")):
        return False
    try:
        # lightweight host check
        return any(host in text for host in _SHEETS_HOSTS) and "/spreadsheets/d/" in text
    except Exception:
        return False


def parse_google_sheets_link(url: str) -> Tuple[str, str]:
    if not is_google_sheets_link(url):
        raise InvalidLinkError("not a sheets link")
    m = _SHEETS_ID_RE.search(url)
    if not m:
        raise InvalidLinkError("missing spreadsheet id")
    spreadsheet_id = m.group(1)
    gid_match = _GID_RE.search(url)
    gid = gid_match.group(1) if gid_match else "0"
    return spreadsheet_id, gid


def build_public_csv_url(spreadsheet_id: str, gid: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"


async def _http_get_text(url: str, *, timeout: int = 20) -> tuple[int, str, str]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=timeout, allow_redirects=True) as resp:
                status = resp.status
                content_type = resp.headers.get("Content-Type", "")
                text = await resp.text(errors="ignore")
                return status, content_type, text
    except asyncio.TimeoutError as exc:
        raise FetchError("request timed out") from exc
    except asyncio.CancelledError as exc:
        raise FetchError("request cancelled") from exc
    except ClientError as exc:
        raise FetchError("network error") from exc


def _get_service_account_path() -> Optional[str]:
    path = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if path and os.path.isfile(path):
        return path
    return None


async def _fetch_rows_via_service_account(spreadsheet_id: str, gid: str) -> List[List[str]]:
    path = _get_service_account_path()
    if not path:
        raise PublicAccessRequiredError("no service account")

    # Importing heavy deps lazily
    from google.oauth2 import service_account  # type: ignore
    from googleapiclient.discovery import build  # type: ignore

    creds = service_account.Credentials.from_service_account_file(
        path,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/drive.readonly",
        ],
    )
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)

    # Resolve sheet title by gid
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = (meta or {}).get("sheets", [])
    title: Optional[str] = None
    try:
        target_id = int(gid)
    except (TypeError, ValueError):
        target_id = 0
    for sheet in sheets:
        props = (sheet or {}).get("properties", {})
        if int(props.get("sheetId", -1)) == target_id:
            title = str(props.get("title", "")).strip() or None
            break
    if not title:
        # fallback to first sheet title
        if sheets:
            props = (sheets[0] or {}).get("properties", {})
            title = str(props.get("title", "")).strip() or None
    if not title:
        raise FetchError("cannot resolve sheet title")

    values = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=title)
        .execute()
        .get("values", [])
    )
    # Ensure we return list of list of strings
    normalized: List[List[str]] = []
    for row in values:
        if not isinstance(row, list):
            continue
        normalized.append([str(cell) if cell is not None else "" for cell in row])
    return normalized


async def fetch_rows_from_link(url: str) -> List[List[str]]:
    """Fetch worksheet rows from a Google Sheets URL.

    Prefer public CSV export; if not accessible and a service account is configured,
    try Sheets API to read rows even for private sheets.
    """
    spreadsheet_id, gid = parse_google_sheets_link(url)
    csv_url = build_public_csv_url(spreadsheet_id, gid)

    status, content_type, text = await _http_get_text(csv_url)
    if status == 200:
        # Quick CSV parse (no quotes-heavy expectations): split by lines and commas safely
        import csv
        from io import StringIO

        reader = csv.reader(StringIO(text))
        return [list(row) for row in reader]

    if status in (401, 403):
        # Try service account fallback
        try:
            return await _fetch_rows_via_service_account(spreadsheet_id, gid)
        except PublicAccessRequiredError:
            raise PublicAccessRequiredError("viewer access required")
        except Exception as exc:  # noqa: BLE001
            # If service account present but still cannot read
            raise PublicAccessRequiredError(str(exc) or "viewer access required")

    if status == 404:
        raise NotFoundError("not found")

    raise FetchError(f"unexpected status {status}; content-type={content_type}")
