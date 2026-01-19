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
    DEBUG: bool = Field(default=True, description="Debug mode flag")

    SYMBOLS: str = Field(
        default="BTCUSDT,ETHUSDT",
        description="Comma-separated list of trading symbols",
    )
    
    MIN_SPREAD: float = Field(
        default=0.0024,
        description="Minimum spread threshold to trigger notifications (e.g., 0.0024 for 0.24%)",
    )
    
    SAFETY_MARGIN: float = Field(
        default=0.0004,
        description="Safety margin to subtract from spread (e.g., 0.0004 for 0.04%)",
    )
    
    MIN_FUNDING_RATE: float = Field(
        default=0.0,
        description="Minimum funding rate threshold (e.g., 0.0 for positive funding)",
    )

    # Exit Configuration
    EXIT_MAX_SPREAD: float = Field(
        default=0.0,
        description="Maximum spread threshold to trigger exit signal (e.g., 0.0 for 0%)",
    )

    # Notification settings
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
