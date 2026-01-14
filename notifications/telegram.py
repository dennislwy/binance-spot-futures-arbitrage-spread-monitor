"""Telegram notification provider implementation.

This module provides a Telegram Bot API integration for sending notifications.
It uses aiohttp for async HTTP requests and supports HTML message formatting.

Example:
    Basic usage:

        from notifications import Telegram

        # Initialize with bot credentials
        notifier = Telegram(
            token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
            chat_id="-1001234567890"
        )

        # Send a notification
        success = await notifier.text("Market alert: BTC spread > 0.3%!")

        # Clean up resources
        await notifier.close()

    With optional title prefix:

        notifier = Telegram(
            token="your_token",
            chat_id="your_chat_id",
            title="[ALERT] "
        )
        # All messages will be prefixed with "[ALERT] "
        await notifier.text("Arbitrage opportunity detected!")
"""

import asyncio
import logging
from typing import Optional

import aiohttp

from notifications.base import NotificationBase

# Module-level logger for Telegram notification events
logger = logging.getLogger(__name__)


class Telegram(NotificationBase):
    """Telegram Bot API notification provider.

    Sends notifications to Telegram chats using the Bot API. Supports
    HTML formatting and manages HTTP sessions efficiently for multiple
    message sends.

    Attributes:
        _title: Optional default title prefix for messages.
        _token: Telegram Bot API token.
        _chat_id: Target chat/channel/group ID.
        _timeout: aiohttp client timeout configuration.
        _session: Reusable aiohttp client session for HTTP requests.
    """

    def __init__(self, token: str, chat_id: str, **kwargs) -> None:
        """Initialize the Telegram notification provider.

        Args:
            token: Telegram Bot API token obtained from @BotFather.
                Format: "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
            chat_id: Target chat ID to send messages to. Can be:
                - User ID (positive integer as string)
                - Group ID (negative integer as string)
                - Channel username ("@channelname")
            **kwargs: Additional optional arguments.
                title (str): Default title prefix for all messages.

        Example:
            notifier = Telegram(
                token="123456:ABC",
                chat_id="-1001234567890",
                title="[Monitor] "
            )
        """
        # Extract optional title from kwargs, default to None
        self._title = kwargs.get("title", None)

        # Store bot credentials for API authentication
        self._token = token
        self._chat_id = chat_id

        # Configure 5-second timeout for API requests to prevent hanging
        self._timeout = aiohttp.ClientTimeout(total=5)

        # Lazy-initialized HTTP session (created on first request)
        self._session: Optional[aiohttp.ClientSession] = None

    @property
    def title(self) -> str | None:
        """Get the default title prefix for notifications.

        Returns:
            str | None: The current title prefix, or None if not set.
        """
        return self._title

    @title.setter
    def title(self, value: str) -> None:
        """Set the default title prefix for notifications.

        Args:
            value: The title prefix to prepend to all messages.
        """
        self._title = value

    async def text(self, msg: str, title: Optional[str] = None) -> bool:
        """Send a text message via Telegram Bot API.

        Sends a message to the configured chat using the Telegram
        sendMessage endpoint. Supports HTML formatting for rich text.

        Args:
            msg: The message content to send. Supports Telegram HTML
                formatting tags like <b>, <i>, <code>, <pre>, etc.
            title: Optional title to prepend to this specific message.
                If None, uses the default title set during initialization.

        Returns:
            bool: True if the message was sent successfully (HTTP 200),
                False if there was an error (API error or network failure).

        Raises:
            No exceptions are raised; errors are logged and False is returned.

        Example:
            success = await notifier.text(
                "<b>Alert:</b> Spread threshold exceeded!",
                title="[URGENT] "
            )
        """
        # Construct Telegram Bot API sendMessage endpoint URL
        url = f"https://api.telegram.org/bot{self._token}/sendMessage"

        # Build request payload with message and formatting options
        payload = {
            "chat_id": self._chat_id,
            # Concatenate title prefix (if any) with message body
            "text": (title or self._title or "") + msg,
            # Enable HTML parsing for rich text formatting
            "parse_mode": "HTML",
        }

        # Get or create HTTP session for request
        session = await self._get_session()

        try:
            # Send POST request to Telegram API
            async with session.post(url, json=payload) as resp:
                # Parse JSON response for error details if needed
                data = await resp.json()

                if resp.status == 200:
                    # Message sent successfully
                    return True

                # Log API error response (e.g., invalid chat_id, rate limit)
                logger.error("Telegram send failed: %s", data)
                return False

        except aiohttp.ClientError as e:
            # Handle network errors (timeout, DNS failure, connection refused)
            logger.error("Telegram HTTP error: %s", e)
            return False

    async def close(self) -> None:
        """Close the HTTP session and release resources.

        Should be called when the notifier is no longer needed to
        properly clean up the aiohttp ClientSession and its underlying
        TCP connections.

        Example:
            notifier = Telegram(token="...", chat_id="...")
            try:
                await notifier.text("Hello!")
            finally:
                await notifier.close()
        """
        # Only close if session exists and is not already closed
        if self._session and not self._session.closed:
            await self._session.close()

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create the aiohttp client session.

        Implements lazy initialization of the HTTP session. Creates
        a new session if one doesn't exist or if the existing one
        has been closed.

        Returns:
            aiohttp.ClientSession: Active HTTP session for making requests.
        """
        # Create new session if needed (lazy initialization pattern)
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session


# Script entry point for manual testing
if __name__ == "__main__":
    import os
    from datetime import datetime

    # Retrieve credentials from environment or prompt user interactively
    token = os.getenv("TELEGRAM_BOT_TOKEN") or input("Enter your Telegram bot token: ")
    chat_id = os.getenv("TELEGRAM_CHAT_ID") or input("Enter your Telegram chat ID: ")

    # Create notifier instance with provided credentials
    notify = Telegram(token=token, chat_id=chat_id)

    # Generate test message with current timestamp
    msg = f"Testing, {str(datetime.now())}"
    print(f"Sending '{msg}'")

    # Send test message and clean up session
    asyncio.run(notify.text(f"Testing, {msg}"))
    asyncio.run(notify.close())
