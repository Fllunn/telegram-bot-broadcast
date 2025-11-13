from __future__ import annotations

import logging
import logging.config
from typing import Dict, Any

from src.config.settings import settings


def _build_logging_config(level: str) -> Dict[str, Any]:
    """Construct a dictConfig-compatible logging configuration."""
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default",
                "level": level,
            }
        },
        "root": {
            "handlers": ["console"],
            "level": level,
        },
    }


def configure_logging(level: str | None = None) -> None:
    """Initialize logging using application settings."""
    logging_level = level or settings.log_level
    logging.config.dictConfig(_build_logging_config(logging_level))
    logging.getLogger(__name__).debug("Logging configured with level %s", logging_level)
