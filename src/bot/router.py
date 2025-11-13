from __future__ import annotations

from telethon import TelegramClient

from src.bot.commands import COMMAND_REGISTRY
from src.bot.context import BotContext


def register_commands(client: TelegramClient, context: BotContext) -> None:
    """Register all bot commands with the provided Telethon client."""
    for setup in COMMAND_REGISTRY:
        setup(client, context)
