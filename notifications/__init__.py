"""Notifications module for sending alerts to various messaging platforms.

This module provides a unified interface for sending notifications through
different messaging services. Currently supports Telegram notifications with
an extensible base class for adding additional notification providers.

Example:
    Basic usage with Telegram:

        from notifications import Telegram

        notifier = Telegram(token="your_bot_token", chat_id="your_chat_id")
        await notifier.text("Hello, World!")
        await notifier.close()

Attributes:
    Telegram: Telegram notification implementation class.
    NotificationBase: Abstract base class for notification providers.
"""

from notifications.base import NotificationBase
from notifications.telegram import Telegram

__all__ = ["Telegram", "NotificationBase"]
