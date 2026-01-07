from abc import ABC, abstractmethod
from typing import Optional


class NotificationBase(ABC):
    @property
    @abstractmethod
    def title(self):
        raise NotImplementedError()

    @title.setter
    @abstractmethod
    def title(self, value: str):
        raise NotImplementedError()

    @abstractmethod
    async def text(self, msg: str, title: Optional[str] = None) -> bool:
        """
        Send out text notification
        """
        raise NotImplementedError()
