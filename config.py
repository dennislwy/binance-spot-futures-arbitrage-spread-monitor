import logging

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    """Application settings loaded from environment variables.

    All settings can be configured via environment variables or .env file.
    See .env.example for all available options.
    """

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)

    # Application Configuration
    TELEGRAM_BOT_TOKEN: str = Field(
        default="",
        description="Telegram bot token for sending notifications",
    )

    TELEGRAM_CHAT_ID: str = Field(
        default="",
        description="Telegram chat ID for sending notifications",
    )


# Global settings instance
settings = Settings()
