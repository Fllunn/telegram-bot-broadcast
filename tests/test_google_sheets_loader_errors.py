import asyncio
import os
import types

import pytest

from src.services import google_sheets as gs


@pytest.mark.asyncio
async def test_public_csv_403_without_service_account(monkeypatch):
    async def fake_http_get_text(url: str, timeout: int = 20):
        return 403, "text/html", "forbidden"

    monkeypatch.setattr(gs, "_http_get_text", fake_http_get_text)
    monkeypatch.delenv("GOOGLE_SERVICE_ACCOUNT_JSON", raising=False)

    # valid link pieces
    url = "https://docs.google.com/spreadsheets/d/abc123/edit#gid=0"
    with pytest.raises(gs.PublicAccessRequiredError):
        await gs.fetch_rows_from_link(url)


@pytest.mark.asyncio
async def test_public_csv_404(monkeypatch):
    async def fake_http_get_text(url: str, timeout: int = 20):
        return 404, "text/html", "not found"

    monkeypatch.setattr(gs, "_http_get_text", fake_http_get_text)

    url = "https://docs.google.com/spreadsheets/d/abc123/edit#gid=0"
    with pytest.raises(gs.NotFoundError):
        await gs.fetch_rows_from_link(url)


@pytest.mark.asyncio
async def test_public_csv_200_parses_csv(monkeypatch):
    sample_csv = "Name,Username,Link\nG1,@g1,\n,group2,https://t.me/group2\n"

    async def fake_http_get_text(url: str, timeout: int = 20):
        return 200, "text/csv", sample_csv

    monkeypatch.setattr(gs, "_http_get_text", fake_http_get_text)

    url = "https://docs.google.com/spreadsheets/d/abc123/edit#gid=0"
    rows = await gs.fetch_rows_from_link(url)
    assert rows[0] == ["Name", "Username", "Link"]
    assert rows[1][1] == "@g1"
