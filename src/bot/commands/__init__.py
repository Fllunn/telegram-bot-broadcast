from __future__ import annotations

from typing import Protocol

from telethon import TelegramClient

from src.bot.context import BotContext


class CommandSetup(Protocol):
    """Protocol for registering Telethon command handlers."""

    def __call__(self, client: TelegramClient, context: BotContext) -> None:
        ...


from src.bot.commands.account import setup_account_commands  # noqa: E402
from src.bot.commands.help import setup_help_command  # noqa: E402
from src.bot.commands.start import setup_start_command  # noqa: E402
from src.bot.commands.broadcast import setup_broadcast_commands  # noqa: E402
from src.bot.commands.groups import setup_group_commands  # noqa: E402
from src.bot.commands.cancel import setup_cancel_command  # noqa: E402


COMMAND_REGISTRY: tuple[CommandSetup, ...] = (
    setup_start_command,
    setup_help_command,
    setup_account_commands,
    setup_broadcast_commands,
    setup_group_commands,
    setup_cancel_command,
)
