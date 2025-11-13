from __future__ import annotations

from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration sourced from environment variables."""

    telegram_api_id: int = Field(..., alias="TELEGRAM_API_ID")
    telegram_api_hash: str = Field(..., alias="TELEGRAM_API_HASH")
    telegram_bot_token: str = Field(..., alias="TELEGRAM_BOT_TOKEN")

    mongo_dsn: str = Field(..., alias="MONGO_DSN")
    mongo_database: str = Field(..., alias="MONGO_DATABASE")

    app_name: str = Field(default="telegram-broadcast-bot", alias="APP_NAME")
    bot_session_name: str = Field(default="bot_session", alias="BOT_SESSION_NAME")
    user_collection: str = Field(default="users", alias="USER_COLLECTION")
    session_collection: str = Field(default="telethon_sessions", alias="SESSION_COLLECTION")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached settings instance."""
    return Settings()


settings = get_settings()
