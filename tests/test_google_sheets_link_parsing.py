import asyncio
import os
import types

import pytest

from src.services.google_sheets import (
    InvalidLinkError,
    is_google_sheets_link,
    parse_google_sheets_link,
)


def test_is_google_sheets_link_true():
    url = "https://docs.google.com/spreadsheets/d/1AbCdEfGhIjKlMn1234567890/edit#gid=987654321"
    assert is_google_sheets_link(url)


def test_is_google_sheets_link_false():
    url = "https://example.com/spreadsheets/d/abc"
    assert not is_google_sheets_link(url)


def test_parse_google_sheets_link_ok():
    url = "https://docs.google.com/spreadsheets/d/1AbCdeF_12345/edit#gid=42"
    sid, gid = parse_google_sheets_link(url)
    assert sid == "1AbCdeF_12345"
    assert gid == "42"


def test_parse_google_sheets_link_default_gid_zero():
    url = "https://docs.google.com/spreadsheets/d/1AbCdeF_12345/edit"
    sid, gid = parse_google_sheets_link(url)
    assert sid == "1AbCdeF_12345"
    assert gid == "0"


def test_parse_google_sheets_link_invalid():
    with pytest.raises(InvalidLinkError):
        parse_google_sheets_link("https://example.com/doc")
