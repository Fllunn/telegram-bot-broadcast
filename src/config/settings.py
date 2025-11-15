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
    auto_task_collection: str = Field(default="auto_broadcast_tasks", alias="AUTO_TASK_COLLECTION")
    auto_account_collection: str = Field(default="auto_accounts", alias="AUTO_ACCOUNT_COLLECTION")
    auto_task_poll_interval_seconds: int = Field(default=15, alias="AUTO_TASK_POLL_INTERVAL")
    auto_task_lock_ttl_seconds: int = Field(default=180, alias="AUTO_TASK_LOCK_TTL")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    account_status_concurrency: int = Field(default=10, alias="ACCOUNT_STATUS_CONCURRENCY")
    account_status_timeout_seconds: float = Field(default=2.0, alias="ACCOUNT_STATUS_TIMEOUT_SECONDS")
    account_status_cache_ttl_seconds: float = Field(default=20.0, alias="ACCOUNT_STATUS_CACHE_TTL_SECONDS")
    account_status_db_refresh_seconds: float = Field(default=180.0, alias="ACCOUNT_STATUS_DB_REFRESH_SECONDS")

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached settings instance."""
    return Settings()


settings = get_settings()
