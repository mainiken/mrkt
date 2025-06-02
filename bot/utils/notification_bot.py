import aiohttp
import asyncio
from typing import Optional

from bot.config.config import settings
from bot.utils import logger


class NotificationBot:
    """Класс для отправки уведомлений через Telegram бота."""

    def __init__(self, bot_token: str, chat_id: int):
        self._bot_token = bot_token
        self._chat_id = chat_id
        self._base_url = f"https://api.telegram.org/bot{self._bot_token}"
        self._http_client: Optional[aiohttp.ClientSession] = None

    async def _get_http_client(self) -> aiohttp.ClientSession:
        """Возвращает существующий или создает новый HTTP клиент."""
        if self._http_client is None or self._http_client.closed:
            self._http_client = aiohttp.ClientSession()
        return self._http_client

    async def send_message(self, message: str) -> None:
        """Отправляет сообщение в указанный чат."""
        if not self._bot_token or not self._chat_id:
            # Логируем только если настройки не заданы, чтобы не спамить при каждом вызове
            if settings.DEBUG_LOGGING:
                 logger.debug("Токен бота или ID чата для уведомлений не указаны.")
            return

        client = await self._get_http_client()
        url = f"{self._base_url}/sendMessage"
        payload = {
            "chat_id": self._chat_id,
            "text": self._escape_markdown_v2(message),
            "parse_mode": "MarkdownV2"
        }

        try:
            async with client.post(url, json=payload) as resp:
                if resp.status != 200:
                    logger.error(
                        f"Не удалось отправить уведомление в Telegram: {resp.status} {await resp.text()}"
                    )
                # else:
                    # Логируем успешную отправку только в режиме дебага
                    # if settings.DEBUG_LOGGING:
                         # logger.debug("Уведомление в Telegram отправлено.")

        except Exception as e:
            logger.error(f"Ошибка при отправке уведомления в Telegram: {e}")

    def _escape_markdown_v2(self, text: str) -> str:
        """Экранирует специальные символы MarkdownV2 в тексте."""
        # Список специальных символов, которые нужно экранировать
        special_chars = '_*[]()~`>#+-=|{}.!'
        # Создаем строку с экранированными символами
        escaped_text = "".join([f'\\{char}' if char in special_chars else char for char in text])
        return escaped_text

    async def close(self) -> None:
        """Закрывает HTTP клиент."""
        if self._http_client and not self._http_client.closed:
            await self._http_client.close()

    async def отправить_уведомление_о_запуске(self) -> None:
        """Отправляет сообщение о запуске программы."""
        сообщение_о_запуске = "Программа успешно запущена."
        await self.send_message(сообщение_о_запуске) 