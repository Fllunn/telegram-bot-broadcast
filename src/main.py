from __future__ import annotations

import asyncio
import logging

from src.app import create_application
from src.utils.logging import configure_logging


def main() -> None:
    """Entrypoint for launching the Telegram bot application."""
    configure_logging()
    logging.getLogger(__name__).info("Starting Telegram broadcast bot")

    application = create_application()

    try:
        asyncio.run(application.run())
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("Shutdown requested by user")
    finally:
        logging.getLogger(__name__).info("Telegram broadcast bot stopped")


if __name__ == "__main__":
    main()
