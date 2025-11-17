from __future__ import annotations

import fnmatch
import logging
import logging.config
import os
import re
import time
from pathlib import Path
from typing import Any, Dict

from src.config.settings import settings


LOG_RETENTION_SECONDS = 24 * 60 * 60
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
LOG_ROTATION_WHEN = "midnight"
LOG_ROTATION_INTERVAL = 1
LOG_ROTATION_BACKUP_COUNT = 1
_VALID_LOG_LEVELS = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"}

_module_logger = logging.getLogger(__name__)

class RetentionTimedRotatingFileHandler(logging.handlers.TimedRotatingFileHandler):
    """Timed rotating handler that also purges files older than retention."""

    def __init__(self, *args: Any, retention_seconds: int = LOG_RETENTION_SECONDS, **kwargs: Any) -> None:
        self._retention_seconds = retention_seconds
        super().__init__(*args, **kwargs)

    def doRollover(self) -> None:
        super().doRollover()
        self._purge_expired()

    def _purge_expired(self) -> None:
        if self._retention_seconds <= 0:
            return
        directory, base = os.path.split(self.baseFilename)
        if not directory:
            directory = "."
        try:
            entries = os.listdir(directory)
        except OSError:
            return
        now = time.time()
        pattern = f"{base}*"
        for entry in entries:
            if not fnmatch.fnmatch(entry, pattern):
                continue
            path = os.path.join(directory, entry)
            if not os.path.isfile(path):
                continue
            try:
                modified_at = os.stat(path).st_mtime
            except OSError:
                continue
            if now - modified_at <= self._retention_seconds:
                continue
            try:
                os.remove(path)
            except OSError:
                continue


def _sanitize_log_filename(source: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9_.-]+", "-", source or "application")
    sanitized = sanitized.strip("-.") or "application"
    if not sanitized.lower().endswith(".log"):
        sanitized = f"{sanitized}.log"
    return sanitized


def _resolve_log_file() -> Path:
    directory_name = getattr(settings, "log_directory", "logs") or "logs"
    log_dir = Path(directory_name).expanduser()
    log_dir.mkdir(parents=True, exist_ok=True)
    file_name = _sanitize_log_filename(getattr(settings, "app_name", "application"))
    return log_dir / file_name


def _purge_expired_logs(log_file: Path, retention_seconds: int) -> None:
    directory = log_file.parent
    pattern = f"{log_file.name}*"
    now = time.time()
    for candidate in directory.glob(pattern):
        if not candidate.is_file():
            continue
        try:
            modified_at = candidate.stat().st_mtime
        except OSError:
            continue
        if now - modified_at <= retention_seconds:
            continue
        try:
            candidate.unlink()
        except OSError:
            continue


def _build_logging_config(level: str, log_file: Path) -> Dict[str, Any]:
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": LOG_FORMAT,
            }
        },
        "handlers": {
            "file": {
                "class": "src.utils.logging.RetentionTimedRotatingFileHandler",
                "formatter": "default",
                "level": level,
                "filename": str(log_file),
                "when": LOG_ROTATION_WHEN,
                "interval": LOG_ROTATION_INTERVAL,
                "backupCount": LOG_ROTATION_BACKUP_COUNT,
                "encoding": "utf-8",
                "delay": True,
                "utc": True,
                "retention_seconds": LOG_RETENTION_SECONDS,
            }
        },
        "root": {
            "handlers": ["file"],
            "level": level,
        },
    }


def _configure_third_party_loggers() -> None:
    suppressed_loggers = (
        "telethon",
        "telethon.client",
        "telethon.network",
        "telethon.extensions",
    )
    for logger_name in suppressed_loggers:
        logging.getLogger(logger_name).setLevel(logging.WARNING)


def configure_logging(level: str | None = None) -> None:
    logging_level = (level or settings.log_level or "INFO").upper()
    if logging_level not in _VALID_LOG_LEVELS:
        logging_level = "INFO"
    log_file = _resolve_log_file()
    _purge_expired_logs(log_file, LOG_RETENTION_SECONDS)
    logging.config.dictConfig(_build_logging_config(logging_level, log_file))
    logging.captureWarnings(True)
    _configure_third_party_loggers()
    _module_logger.debug("Logging configured: file=%s level=%s", log_file, logging_level)
