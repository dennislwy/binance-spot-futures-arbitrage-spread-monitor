import asyncio
import logging
from typing import Optional

import aiohttp

from notifications.base import NotificationBase

logger = logging.getLogger(__name__)


class Telegram(NotificationBase):
    def __init__(self, token: str, chat_id: str, **kwargs):
        self._title = kwargs.get("title", None)
        self._token = token
        self._chat_id = chat_id
        self._timeout = aiohttp.ClientTimeout(total=5)
        self._session: Optional[aiohttp.ClientSession] = None

    @property
    def title(self):
        return self._title

    @title.setter
    def title(self, value: str):
        self._title = value

    async def text(self, msg: str, title: Optional[str] = None) -> bool:
        url = f"https://api.telegram.org/bot{self._token}/sendMessage"
        payload = {
            "chat_id": self._chat_id,
            "text": (title or self._title or "") + msg,
            "parse_mode": "HTML",
        }
        session = await self._get_session()
        try:
            async with session.post(url, json=payload) as resp:
                data = await resp.json()
                if resp.status == 200:
                    return True
                logger.error("Telegram send failed: %s", data)
                return False
        except aiohttp.ClientError as e:
            logger.error("Telegram HTTP error: %s", e)
            return False

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _get_session(self) -> aiohttp.ClientSession:
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session


if __name__ == "__main__":
    import os
    from datetime import datetime

    # ask user for token and chat id input
    token = os.getenv("TELEGRAM_BOT_TOKEN") or input("Enter your Telegram bot token: ")
    chat_id = os.getenv("TELEGRAM_CHAT_ID") or input("Enter your Telegram chat ID: ")

    notify = Telegram(token=token, chat_id=chat_id)

    # send message of time now
    msg = f"Testing, {str(datetime.now())}"
    print(f"Sending '{msg}'")
    asyncio.run(notify.text(f"Testing, {msg}"))
    asyncio.run(notify.close())
