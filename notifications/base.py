"""Abstract base class for notification providers.

This module defines the interface that all notification providers must implement.
It ensures consistent behavior across different messaging platforms (Telegram,
Discord, Slack, etc.) by enforcing a common API contract.

Example:
    Creating a custom notification provider:

        class MyNotifier(NotificationBase):
            @property
            def title(self) -> str | None:
                return self._title

            @title.setter
            def title(self, value: str) -> None:
                self._title = value

            async def text(self, msg: str, title: str | None = None) -> bool:
                # Implementation here
                return True
"""

from abc import ABC, abstractmethod
from typing import Optional


class NotificationBase(ABC):
    """Abstract base class defining the notification provider interface.

    All notification providers (Telegram, Discord, Slack, etc.) must inherit
    from this class and implement all abstract methods and properties.

    Attributes:
        title: Optional default title prefix for all messages.
    """

    @property
    @abstractmethod
    def title(self) -> str | None:
        """Get the default title prefix for notifications.

        Returns:
            str | None: The current title prefix, or None if not set.

        Raises:
            NotImplementedError: If not implemented by subclass.
        """
        raise NotImplementedError()

    @title.setter
    @abstractmethod
    def title(self, value: str) -> None:
        """Set the default title prefix for notifications.

        Args:
            value: The title prefix to prepend to all messages.

        Raises:
            NotImplementedError: If not implemented by subclass.
        """
        raise NotImplementedError()

    @abstractmethod
    async def text(self, msg: str, title: Optional[str] = None) -> bool:
        """Send a text notification through the messaging platform.

        This method must be implemented by all notification providers to
        handle the actual delivery of text messages to their respective
        platforms.

        Args:
            msg: The message content to send. Should support the platform's
                text formatting (e.g., HTML for Telegram, Markdown for Discord).
            title: Optional title to prepend to this specific message.
                Overrides the default title if provided.

        Returns:
            bool: True if the message was sent successfully, False otherwise.

        Raises:
            NotImplementedError: If not implemented by subclass.
        """
        raise NotImplementedError()
