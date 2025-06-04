import asyncio
import random
import datetime
from typing import List

from bot.utils.universal_telegram_client import UniversalTelegramClient
from bot.utils import logger

import pyrogram.raw.functions.messages as messages
from pyrogram.raw.types import DialogFilter, InputPeerChannel, InputPeerChat

import telethon.tl.functions.messages
import telethon.tl.types

import pyrogram.enums as pyrogram_enums
import pyrogram.errors
import telethon.errors

class ChannelUnsubscriber:

    def __init__(self, client: UniversalTelegramClient):
        self.client = client
        self.session_name = client.session_name

    async def get_all_channel_usernames(self) -> List[str]:
        channel_usernames: List[str] = []
        logger.info(f"{self.session_name} | Анализ всех каналов...")

        was_connected = True
        if not self.client.is_pyrogram:
            was_connected = self.client.client.is_connected()
        else:
            was_connected = self.client.client.is_connected

        try:
            if not was_connected:
                await self.client.client.connect()

            logger.info(f"{self.session_name} | Fetching all dialogs to identify channels...")

            async for dialog in self.client.client.get_dialogs():
                 is_channel = False
                 if hasattr(dialog, 'chat'):
                      chat_type = getattr(dialog.chat, 'type', None)
                      if self.client.is_pyrogram:
                           if chat_type == pyrogram_enums.ChatType.CHANNEL:
                                is_channel = True
                      else:
                           if hasattr(dialog.chat, 'access_hash') and hasattr(dialog.chat, 'title'):
                                is_channel = True

                 if is_channel and hasattr(dialog.chat, 'username') and dialog.chat.username:
                      channel_usernames.append(dialog.chat.username)
                      logger.debug(f"{self.session_name} | Found channel with username: @{dialog.chat.username}")

        except Exception as e:
            logger.error(f"{self.session_name} | Ошибка при получении списка каналов: {e}")
        finally:
            if not was_connected:
                if not self.client.is_pyrogram:
                    if self.client.client.is_connected():
                         await self.client.client.disconnect()
                else:
                    if self.client.client.is_connected: # type: ignore
                        await self.client.client.disconnect()


        return channel_usernames

    async def unsubscribe_from_channels(self, channel_usernames_to_unsubscribe: List[str]) -> int:
        total_channels = len(channel_usernames_to_unsubscribe)
        unsubscribed_count = 0

        if not channel_usernames_to_unsubscribe:
            logger.info(f"{self.session_name} | Нет каналов для отписки этим клиентом.")
            return 0

        logger.info(f"{self.session_name} | Запуск процедуры отписки от {total_channels} каналов этим клиентом...")

        for i, channel_username in enumerate(channel_usernames_to_unsubscribe):
            logger.info(f"{self.session_name} | Отписка от канала <y>@{channel_username}</y> ({i + 1}/{total_channels})...")
            success = False
            while not success:
                try:
                    await self.client._check_and_apply_rate_limit("unsubscribe")
                    success = await self.client.leave_telegram_channel(channel_username)
                    if success:
                        unsubscribed_count += 1
                        logger.info(f"{self.session_name} | Успешно отписались от <y>@{channel_username}</y>.")
                    else:
                        logger.warning(f"{self.session_name} | Не удалось отписаться от <y>@{channel_username}</y>.")
                except (pyrogram.errors.FloodWait, telethon.errors.FloodWaitError) as e:
                    wait_time = e.value if isinstance(e, pyrogram.errors.FloodWait) else e.seconds
                    logger.warning(f"{self.session_name} | Получен FloodWait на {wait_time} секунд для канала <y>@{channel_username}</y>. Ожидание...")
                    await asyncio.sleep(wait_time)

            if success and i < total_channels - 1:
                delay = random.uniform(3, 30)
                logger.debug(f"{self.session_name} | Задержка перед следующей отпиской: {delay:.2f} сек.")
                await asyncio.sleep(delay)

        logger.info(f"{self.session_name} | Процедура отписки этим клиентом завершена.")
        return unsubscribed_count
