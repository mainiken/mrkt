import aiohttp
import asyncio
import re
import random
import datetime
from typing import Optional, Dict, Any, List, Set
from urllib.parse import unquote

from bot.config.config import settings
from bot.utils import logger
from bot.utils.first_run import check_is_first_run, append_recurring_session
from bot.utils.updater import UpdateManager
from bot.exceptions.error_handler import ErrorHandler, UnauthorizedError
from bot.utils.channel_repository import ChannelRepository


class BaseBot:
    API_BASE_URL: str = "https://api.tgmrkt.io/api/v1"
    AUTH_URL: str = f"{API_BASE_URL}/auth"
    ME_URL: str = f"{API_BASE_URL}/me"
    BALANCE_URL: str = f"{API_BASE_URL}/balance"
    WALLET_URL: str = f"{API_BASE_URL}/wallet"
    GIFT_STATISTICS_URL: str = f"{API_BASE_URL}/gift-statistics"
    GIVEAWAYS_URL: str = f"{API_BASE_URL}/giveaways"
    GIVEAWAY_VALIDATIONS_URL: str = f"{API_BASE_URL}/giveaways/check-validations"
    GIVEAWAY_START_VALIDATION_URL: str = f"{API_BASE_URL}/giveaways/start-validation"
    GIVEAWAY_BUY_TICKETS_URL: str = f"{API_BASE_URL}/giveaways/buy-tickets"
    GIFTS_URL: str = f"{API_BASE_URL}/gifts"

    DEFAULT_HEADERS: Dict[str, str] = {
        'accept': '*/*',
        'accept-language': 'ru,en-US;q=0.9,en;q=0.8',
        'cache-control': 'no-cache',
        'content-type': 'application/json',
        'dnt': '1',
        'origin': 'https://cdn.tgmrkt.io',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': 'https://cdn.tgmrkt.io/',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 '
                      '(KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1',
    }

    EMOJI = {
        'debug': '🔍',
        'success': '✅',
        'info': 'ℹ️',
        'warning': '⚠️',
        'error': '❌',
        'balance': '💎',
        'giveaway': '⭐'
    }

    def __init__(self, tg_client: Any):
        self._tg_client = tg_client
        self._token: Optional[str] = None
        self._giveaway_id: Optional[str] = None
        self._http_client: Optional[aiohttp.ClientSession] = None
        self._current_ref_id: Optional[str] = None
        self._logger = logger

    def _log(self, level: str, message: str, emoji_key: Optional[str] = None) -> None:
        if level == 'debug' and not settings.DEBUG_LOGGING:
            return

        emoji = self.EMOJI.get(emoji_key, '') if emoji_key else ''
        formatted_message = f"{emoji} {message}" if emoji else message

        session_prefix = getattr(self._tg_client, "session_name", "Неизвестная сессия") + " | "
        full_message = session_prefix + formatted_message

        if level == 'debug':
            self._logger.debug(full_message)
        elif level == 'info':
            self._logger.info(full_message)
        elif level == 'warning':
            self._logger.warning(full_message)
        elif level == 'error':
            self._logger.error(full_message)
        elif level == 'success':
            self._logger.success(full_message)
        else:
            self._logger.info(full_message)

    @property
    def token(self) -> Optional[str]:
        return self._token

    @property
    def giveaway_id(self) -> Optional[str]:
        return self._giveaway_id

    async def _get_http_client(self) -> aiohttp.ClientSession:
        if self._http_client is None or self._http_client.closed:
            self._http_client = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(60))
        return self._http_client

    async def close(self) -> None:
        """Закрывает HTTP клиент."""
        if self._http_client is not None and not self._http_client.closed:
            await self._http_client.close()
            self._http_client = None

    async def get_ref_id(self) -> str:
        if self._current_ref_id is None:
            session_name = getattr(self._tg_client, "session_name", "unknown_session")
            is_first_run = await check_is_first_run(session_name)

            if is_first_run:
                random_number = random.randint(1, 100)
                ref_id = settings.REF_ID if random_number <= 70 else '252453226'

                await append_recurring_session(session_name)
                self._log('info', f'Первый запуск сессии. Использован REF_ID: {ref_id}', 'info')
                self._current_ref_id = ref_id
            else:
                self._current_ref_id = settings.REF_ID
                self._log('debug', f'Не первый запуск сессии. Использован REF_ID: {self._current_ref_id}', 'debug')

        return self._current_ref_id

    async def auth(self) -> Dict[str, Any]:
        client = await self._get_http_client()
        headers = self.DEFAULT_HEADERS.copy()
        self._log('debug', 'Попытка получения tg_webview_url...', 'debug')
        try:
            ref_id_to_use = await self.get_ref_id()
            tg_webview_url = await self._tg_client.get_app_webview_url(
                bot_username="mrkt",
                bot_shortname="app",
                default_val=ref_id_to_use
            )
            self._log('debug', f'Успешно получен tg_webview_url: {tg_webview_url}', 'debug')
        except Exception as e:
            self._log('error', f'Ошибка получения tg_webview_url: {e}', 'error')
            raise

        match = re.search(r'tgWebAppData=([^&#]+)', tg_webview_url)
        if not match:
            self._log('error', 'tgWebAppData не найден в webview url.', 'error')
            raise ValueError("tgWebAppData не найден в webview url")

        encoded_data = match.group(1)
        decoded_once = unquote(encoded_data)
        decoded_twice = unquote(decoded_once)
        self._log('debug', f'tg_web_data (декодировано): {decoded_twice}', 'debug')

        photo = getattr(self._tg_client, "photo", "")
        self._log('debug', f'Фото: {photo}', 'debug')

        data = {"data": decoded_twice, "photo": photo, "appId": None}
        self._log('debug', 'Отправка запроса авторизации...', 'info')

        async with client.post(self.AUTH_URL, headers=headers, json=data) as resp:
            self._log('debug', f'Статус ответа авторизации: {resp.status}', 'info')
            if resp.status != 200:
                response_text = await resp.text()
                self._log('error', f'Авторизация не удалась: {resp.status} {response_text}', 'error')
                raise Exception(f"Авторизация не удалась: {resp.status} {response_text}")

            result: Dict[str, Any] = await resp.json()
            self._token = result.get("token")
            self._giveaway_id = result.get("giveawayId")

            if self._token:
                self._log('info', 'Авторизация успешна. Токен получен.', 'success')
            else:
                self._log('error', 'Авторизация успешна, но токен не получен.', 'error')
                raise Exception("Авторизация успешна, но токен не получен")

            await self._random_delay()
            return result

    async def get_me(self) -> Dict[str, Any]:
        client = await self._get_http_client()
        headers = self.DEFAULT_HEADERS.copy()
        headers["authorization"] = self.token
        async with client.get(self.ME_URL, headers=headers) as resp:
            if resp.status != 200:
                raise Exception(f"Не удалось получить информацию о пользователе: {resp.status} {await resp.text()}")
            result = await resp.json()
            await self._random_delay()
            return result

    async def get_balance(self) -> Dict[str, Any]:
        client = await self._get_http_client()
        headers = self.DEFAULT_HEADERS.copy()
        headers["authorization"] = self.token
        async with client.get(self.BALANCE_URL, headers=headers) as resp:
            if resp.status != 200:
                raise Exception(f"Не удалось получить баланс: {resp.status} {await resp.text()}")
            result = await resp.json()
            await self._random_delay()
            return result

    async def check_balance(self) -> float:
        balance = await self.get_balance()
        hard = balance.get('hard', 0)
        ton = hard / 1e9
        self._log('info', f'Баланс: {ton:.2f} TON', 'balance')
        await self._random_delay()
        return ton

    async def check_wallet(self, ton: str, device_id: str) -> Dict[str, Any]:
        client = await self._get_http_client()
        headers = self.DEFAULT_HEADERS.copy()
        headers["authorization"] = self.token
        data = {"ton": ton, "deviceId": device_id}
        async with client.post(self.WALLET_URL, headers=headers, json=data) as resp:
            if resp.status != 200:
                raise Exception(f"Не удалось проверить кошелек: {resp.status} {await resp.text()}")
            result = await resp.json()
            await self._random_delay()
            return result

    async def get_gift_statistics(self) -> Dict[str, Any]:
        client = await self._get_http_client()
        headers = self.DEFAULT_HEADERS.copy()
        headers["authorization"] = self.token
        async with client.get(self.GIFT_STATISTICS_URL, headers=headers) as resp:
            if resp.status != 200:
                raise Exception(f"Не удалось получить статистику подарков: {resp.status} {await resp.text()}")
            result = await resp.json()
            await self._random_delay()
            return result

    async def get_gifts(self) -> Dict[str, Any]:
        """Получает список подарков для текущей сессии."""
        client = await self._get_http_client()
        headers = self.DEFAULT_HEADERS.copy()
        headers["authorization"] = self.token
        # Payload из предоставленного curl запроса
        payload = {
            "isListed": False,
            "count": 20,
            "cursor": "",
            "collectionNames": [],
            "modelNames": [],
            "backdropNames": [],
            "symbolNames": [],
            "minPrice": None,
            "maxPrice": None,
            "mintable": None,
            "number": None,
            "ordering": "Price",
            "lowToHigh": True,
            "query": None
        }
        self._log('debug', 'Попытка получения списка подарков...', 'giveaway')
        async with client.post(self.GIFTS_URL, headers=headers, json=payload) as resp:
            self._log('debug', f'Статус ответа получения подарков: {resp.status}', 'giveaway')
            if resp.status != 200:
                response_text = await resp.text()
                self._log('error', f'Не удалось получить список подарков: {resp.status} {response_text}', 'error')
                # В зависимости от требуемого поведения, можно возбудить исключение
                # или вернуть пустой список/словарь с ошибкой.
                # Для продолжения работы бота, вернем пустой список подарков.
                return {"gifts": [], "cursor": None, "total": 0}

            result: Dict[str, Any] = await resp.json()
            gifts_count = len(result.get("gifts", []))
            self._log('debug', f'Получено {gifts_count} подарков.', 'giveaway')
            if gifts_count > 0:
                self._log('info', f'Обнаружен подарок на "{getattr(self._tg_client, "session_name", "Неизвестная сессия")}"', 'giveaway') # Логируем обнаружение подарков

            await self._random_delay()
            return result

    async def get_giveaways_page(self, giveaway_type: str = "Available", count: int = 20, cursor: str = "") -> Dict[str, Any]:
        """Получает одну страницу розыгрышей."""
        client = await self._get_http_client()
        headers = self.DEFAULT_HEADERS.copy()
        headers["authorization"] = self.token
        params = {"type": giveaway_type, "count": count, "cursor": cursor}
        self._log('debug', f'Получение страницы розыгрышей с параметрами: {params}', 'giveaway')
        async with client.get(self.GIVEAWAYS_URL, headers=headers, params=params) as resp:
            if resp.status != 200:
                raise Exception(f"Не удалось получить страницу розыгрышей: {resp.status} {await resp.text()}")
            result: Dict[str, Any] = await resp.json()
            self._log('debug', f'Получено {len(result.get("items", []))} розыгрышей на странице.', 'giveaway')
            await self._random_delay()
            return result

    async def check_giveaway_validations(self, giveaway_id: str) -> Dict[str, Any]:
        client = await self._get_http_client()
        headers = self.DEFAULT_HEADERS.copy()
        headers["authorization"] = self.token
        url = f"{self.GIVEAWAY_VALIDATIONS_URL}/{giveaway_id}"
        self._log('debug', f'Проверка условий розыгрыша {giveaway_id}', 'giveaway')
        async with client.get(url, headers=headers) as resp:
            if resp.status != 200:
                raise Exception(f"Не удалось проверить условия розыгрыша: {resp.status} {await resp.text()}")
            result: Dict[str, Any] = await resp.json()
            await self._random_delay()
            return result

    async def start_giveaway_validation(self, giveaway_id: str, channel: str, validation_type: str) -> Dict[str, str]:
        client = await self._get_http_client()
        headers = self.DEFAULT_HEADERS.copy()
        headers["authorization"] = self.token
        url = f"{self.GIVEAWAY_START_VALIDATION_URL}/{giveaway_id}?channel={channel}&type={validation_type}"
        self._log('debug', f'Запуск валидации для розыгрыша {giveaway_id}, канала {channel}, типа {validation_type}', 'giveaway')
        async with client.post(url, headers=headers) as resp:
            response_text = await resp.text()
            if resp.status != 200:
                return {"status": "Failed", "message": response_text}
            await self._random_delay()
            return {"status": "Success"}

    async def join_giveaway(self, giveaway_id: str, giveaway_title: Optional[str] = None) -> Dict[str, Any]:
        client = await self._get_http_client()
        headers = self.DEFAULT_HEADERS.copy()
        headers["authorization"] = self.token
        url = f"{self.GIVEAWAY_BUY_TICKETS_URL}/{giveaway_id}?count=1"
        self._log('debug', f'Попытка присоединиться к розыгрышу {giveaway_title or giveaway_id}', 'giveaway')
        async with client.post(url, headers=headers) as resp:
            if resp.status != 200:
                response_text = await resp.text()
                self._log('info', f'Не удалось присоединиться к розыгрышу {giveaway_title or giveaway_id}: {resp.status} {response_text}', 'warning')
                return {"success": False, "status": resp.status, "message": response_text}
            result: Dict[str, Any] = await resp.json()
            self._log('info', f' Успешно присоединились к розыгрышу ⚡<y>{giveaway_title or giveaway_id}</y>!', 'success')
            await self._random_delay()
            return {"success": True, "result": result}

    async def _random_delay(self) -> None:
        """Добавляет случайную задержку между 1 и 3 секундами."""
        delay = random.uniform(1, 3)
        self._log('debug', f'Добавление случайной задержки: {delay:.2f} сек.', 'info')
        await asyncio.sleep(delay)

    async def _send_telegram_message(self, chat_id: str, message: str) -> bool:
        """Отправляет сообщение в Telegram чат."""
        if not hasattr(settings, 'NOTIFICATION_BOT_TOKEN') or not settings.NOTIFICATION_BOT_TOKEN:
            self._log('debug', 'Токен для уведомлений Telegram не настроен.', 'warning')
            return False

        client = await self._get_http_client()
        url = f'https://api.telegram.org/bot{settings.NOTIFICATION_BOT_TOKEN}/sendMessage'
        payload = {
            'chat_id': chat_id,
            'text': message,
            'parse_mode': 'MarkdownV2'
        }
        headers = {'Content-Type': 'application/json'}

        try:
            async with client.post(url, json=payload, headers=headers) as resp:
                if resp.status == 200:
                    self._log('debug', 'Сообщение в Telegram успешно отправлено.', 'success')
                    return True
                else:
                    response_text = await resp.text()
                    self._log('error', f'Ошибка при отправке сообщения в Telegram: {resp.status} {response_text}', 'error')
                    return False
        except Exception as e:
            self._log('error', f'Исключение при отправке сообщения в Telegram: {e}', 'error')
            return False


class GiveawayProcessor:
    def __init__(self, bot: BaseBot, channel_repository: ChannelRepository):
        self._bot = bot
        self._channel_repository = channel_repository

    async def _filter_giveaways(self, giveaways: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        filtered = []
        for giveaway in giveaways:
            giveaway_id = giveaway.get("id")
            if not giveaway_id:
                self._bot._log('debug', 'Пропускаем розыгрыш без ID.', 'warning')
                continue

            # Проверка условий фильтрации на основе настроек
            is_boost_required = giveaway.get("isChanelBoostRequired", False)
            is_premium_required = giveaway.get("isForPremium", False)
            is_active_trader_required = giveaway.get("isForActiveTraders", False)

            if hasattr(settings, 'GIVEAWAY_SKIP_CHANNEL_BOOST_REQUIRED') and settings.GIVEAWAY_SKIP_CHANNEL_BOOST_REQUIRED and is_boost_required:
                self._bot._log('debug', f'Пропускаем розыгрыш "{giveaway.get("previewGift", {}).get("title", "Неизвестно")}" (ID: {giveaway_id}) так как требуется буст канала и включена настройка пропуска.', 'debug')
                continue

            if hasattr(settings, 'PARTICIPATE_IN_FREE_GIVEAWAYS') and settings.PARTICIPATE_IN_FREE_GIVEAWAYS and (is_premium_required or is_active_trader_required):
                 self._bot._log('debug', f'Пропускаем розыгрыш "{giveaway.get("previewGift", {}).get("title", "Неизвестно")}" (ID: {giveaway_id}) так как он не является бесплатным.', 'debug')
                 continue

            # Проверка на минимальное и максимальное количество участников
            participants_count = giveaway.get("participantsCount", 0)
            if hasattr(settings, 'GIVEAWAY_MIN_PARTICIPANTS') and participants_count < settings.GIVEAWAY_MIN_PARTICIPANTS:
                 self._bot._log('debug', f'Пропускаем розыгрыш "{giveaway.get("previewGift", {}).get("title", "Неизвестно")}" (ID: {giveaway_id}) так как количество участников ({participants_count}) меньше минимального ({settings.GIVEAWAY_MIN_PARTICIPANTS}).', 'debug')
                 continue

            if hasattr(settings, 'GIVEAWAY_MAX_PARTICIPANTS') and participants_count > settings.GIVEAWAY_MAX_PARTICIPANTS:
                 self._bot._log('debug', f'Пропускаем розыгрыш "{giveaway.get("previewGift", {}).get("title", "Неизвестно")}" (ID: {giveaway_id}) так как количество участников ({participants_count}) больше максимального ({settings.GIVEAWAY_MAX_PARTICIPANTS}).', 'debug')
                 continue


            filtered.append(giveaway)
            self._bot._log('debug', f'Найден розыгрыш, подходящий по фильтрам: "{giveaway.get("previewGift", {}).get("title", "Неизвестно")}" (ID: {giveaway_id})', 'giveaway')
        return filtered

    async def _check_and_fulfill_channel_validation(
        self, giveaway_id: str, channel_name: str, current_is_member_status: str
    ) -> bool:
        session_name = getattr(self._bot._tg_client, "session_name", "unknown_session")
        if await self._channel_repository.is_subscribed(session_name, channel_name):
            self._bot._log('info', f'Канал <y>{channel_name}</y> уже в базе, пропускаем подписку.', 'success')
            return True
        if current_is_member_status == "Validated":
            await self._channel_repository.add_channel(session_name, channel_name)
            self._bot._log('info', f' Подписка на канал <y>{channel_name}</y> подтверждена.', 'success')
            return True

        if hasattr(settings, 'GIVEAWAY_SKIP_CHANNEL_SUBSCRIBE_REQUIRED') and settings.GIVEAWAY_SKIP_CHANNEL_SUBSCRIBE_REQUIRED:
            self._bot._log('debug', f'Пропускаем проверку подписки на канал <y>{channel_name}</y> по настройке.', 'info')
            return False

        self._bot._log('debug', f'Попытка подписаться на канал <y>{channel_name}</y>', 'debug')

        try:
            channel_join_success = await self._bot._tg_client.join_telegram_channel(
                {"additional_data": {"username": channel_name}}
            )
            if not channel_join_success:
                self._bot._log('info', f'Не удалось вступить в канал <y>{channel_name}</y>.', 'warning')
                return False

            self._bot._log('info', f' Вступление в канал <y>{channel_name}</y> успешно.', 'success')
            if hasattr(settings, 'CHANNEL_SUBSCRIBE_DELAY'):
                 self._bot._log('debug', f'Ожидание после вступления в канал согласно настройкам: {settings.CHANNEL_SUBSCRIBE_DELAY} сек.', 'info')

            start_validation_result = await self._bot.start_giveaway_validation(
                giveaway_id, channel_name, "ChannelMember"
            )
            if start_validation_result.get("status") != "Success":
                self._bot._log('info', f'Серверная валидация канала <y>{channel_name}</y> не запущена: {start_validation_result.get("message")}', 'warning')

            max_retries = 5
            base_delay = 5

            for attempt in range(max_retries):
                delay = base_delay + attempt * random.uniform(1, 3)
                await asyncio.sleep(delay)

                validations_after_sub = await self._bot.check_giveaway_validations(giveaway_id)
                updated_is_member_status = next(
                    (cv.get("isMember") for cv in validations_after_sub.get("channelValidations", []) if cv["channel"] == channel_name),
                    None
                )

                if updated_is_member_status == "Validated":
                    await self._channel_repository.add_channel(session_name, channel_name)
                    self._bot._log('info', f' Подписка на канал <y>{channel_name}</y> подтверждена.', 'success')
                    return True

                self._bot._log('debug', f'Попытка {attempt+1}/{max_retries}: подписка на канале <y>{channel_name}</y> не подтверждена ( статус: {updated_is_member_status}), ждем {delay:.2f} сек.', 'debug')

            self._bot._log('info', f' Не удалось подтвердить подписку на канале <y>{channel_name}</y> после {max_retries} попыток.', 'error')
            return False

        except ValueError as ve:
            self._bot._log('info', f'Ошибка при вступлении в канал <y>{channel_name}</y>: {ve}', 'warning')
            return False
        except Exception as e:
            self._bot._log('info', f'Неизвестная ошибка при вступлении в канал <y>{channel_name}</y>: {e}', 'error')
            return False

    async def _process_giveaway(self, giveaway: Dict[str, Any]) -> None:
        giveaway_id = giveaway.get("id")
        giveaway_title = giveaway.get("previewGift", {}).get("title", "Неизвестно")
        try:
            self._bot._log('debug', f'Проверяем условия для розыгрыша <y>{giveaway_title}</y>', 'giveaway')
            validations = await self._bot.check_giveaway_validations(giveaway_id)
            can_join = True

            if hasattr(settings, 'GIVEAWAY_REQUIRE_PREMIUM') and settings.GIVEAWAY_REQUIRE_PREMIUM and not validations.get("isPremium", False):
                self._bot._log('info', f'Розыгрыш <y>{giveaway_title}</y> требует премиум, пользователь не премиум.', 'warning')
                can_join = False

            if can_join and hasattr(settings, 'GIVEAWAY_REQUIRE_ACTIVE_TRADER') and settings.GIVEAWAY_REQUIRE_ACTIVE_TRADER and not validations.get("isActiveTrader", False):
                self._bot._log('info', f'Розыгрыш <y>{giveaway_title}</y> требует активного трейдера, пользователь не активный трейдер.', 'warning')
                can_join = False

            channel_validations = validations.get("channelValidations", [])
            if can_join:
                for channel_validation in channel_validations:
                    channel_name = channel_validation.get("channel")
                    if not channel_name:
                        continue

                    is_member = channel_validation.get("isMember")
                    is_boosted = channel_validation.get("isBoosted")

                    if hasattr(settings, 'GIVEAWAY_REQUIRE_CHANNEL_BOOST') and settings.GIVEAWAY_REQUIRE_CHANNEL_BOOST and is_boosted != "Validated":
                        if not (hasattr(settings, 'GIVEAWAY_SKIP_CHANNEL_BOOST_REQUIRED') and settings.GIVEAWAY_SKIP_CHANNEL_BOOST_REQUIRED):
                            self._bot._log('info', f'Розыгрыш <y>{giveaway_title}</y> требует буст канала <y>{channel_name}</y>, но буст не подтвержден.', 'warning')
                            can_join = False
                            break

                    if can_join:
                        channel_validation_ok = await self._check_and_fulfill_channel_validation(
                            giveaway_id, channel_name, is_member
                        )
                        if not channel_validation_ok:
                            can_join = False
                            break

                if can_join:
                    join_result = await self._bot.join_giveaway(giveaway_id, giveaway_title)
                    if not join_result.get("success"):
                        self._bot._log('info', f'Не удалось принять участие в розыгрыше <y>{giveaway_title}</y>: {join_result.get("message", "Ошибка")}', 'warning')

                    # После попытки присоединения, помечаем розыгрыш как обработанный
                    await self._channel_repository.add_processed_giveaway(giveaway_id)
                    self._bot._log('debug', f'Розыгрыш <y>{giveaway_title}</y> (ID: {giveaway_id}) помечен как обработанный.', 'info')

                else:
                    self._bot._log('info', f' Условия для розыгрыша <y>{giveaway_title}</y> не выполнены. Пропускаем.', 'info')
                    # Можно помечать как обработанный, даже если не участвовали,
                    # чтобы не проверять его условия снова.
                    await self._channel_repository.add_processed_giveaway(giveaway_id)
                    self._bot._log('debug', f'Розыгрыш <y>{giveaway_title}</y> (ID: {giveaway_id}) помечен как обработанный (условия не выполнены).', 'info')


        except Exception as e:
            self._bot._log('error', f'Ошибка при обработке розыгрыша <y>{giveaway_title}</y>: {e}', 'error')
            # В случае ошибки при обработке, возможно, стоит тоже пометить как обработанный,
            # чтобы не застрять на одном и том же розыгрыше.
            # Решение зависит от желаемого поведения при ошибках.
            # Пока оставим без пометки, чтобы была попытка повторить в следующем цикле
            # или если ошибка временная.

    async def _collect_giveaways_until_repeat(self) -> List[Dict[str, Any]]:
        """Собирает уникальные розыгрыши постранично до появления повтора."""
        self._bot._log('debug', 'Начинаем сбор уникальных розыгрышей до появления повторов...', 'giveaway')
        collected_giveaways: List[Dict[str, Any]] = []
        collected_giveaway_ids_this_run: Set[str] = set() # Сет для отслеживания ID в ТЕКУЩЕМ цикле сбора
        current_cursor = ""
        page_count = 0

        while True:
            page_count += 1
            self._bot._log('debug', f'Запрос страницы {page_count} с cursor="{current_cursor}"...', 'giveaway')
            try:
                giveaways_data = await self._bot.get_giveaways_page(
                    giveaway_type=getattr(settings, 'GIVEAWAY_LIST_TYPE', "Available"),
                    count=getattr(settings, 'GIVEAWAY_LIST_COUNT', 20),
                    cursor=current_cursor
                )
                items = giveaways_data.get("items", [])
                self._bot._log('debug', f'На странице {page_count} получено {len(items)} розыгрышей.', 'giveaway')

                if not items:
                    self._bot._log('debug', 'Получен пустой список элементов на странице — сбор завершен.', 'giveaway')
                    break

                new_giveaways_on_page = []
                repeat_found = False
                for item in items:
                    giveaway_id = item.get("id")
                    if not giveaway_id:
                        continue

                    # *** Ключевая проверка 1: был ли этот розыгрыш обработан в ПРОШЛЫХ запусках? ***
                    if await self._channel_repository.is_giveaway_processed(giveaway_id):
                        self._bot._log('debug', f'Розыгрыш ID:{giveaway_id} уже был обработан ранее. Пропускаем сбор.', 'debug')
                        continue # Пропускаем этот розыгрыш, смотрим следующий в этом списке

                    # *** Ключевая проверка 2: был ли этот розыгрыш СОБРАН в ТЕКУЩЕМ цикле сбора? ***
                    if giveaway_id in collected_giveaway_ids_this_run:
                        self._bot._log('info', f'Обнаружен повторный розыгрыш ID: {giveaway_id} в текущем цикле сбора. Сбор уникальных розыгрышей завершен.', 'giveaway')
                        repeat_found = True
                        break # Прекращаем сбор при первом повторе в этом цикле

                    # Если розыгрыш новый для этого запуска и не был обработан ранее
                    collected_giveaway_ids_this_run.add(giveaway_id)
                    new_giveaways_on_page.append(item)

                collected_giveaways.extend(new_giveaways_on_page)

                if repeat_found:
                    break # Выходим из внешнего цикла while True, если обнаружен повтор

                # Получаем курсор для следующей страницы
                next_cursor = giveaways_data.get("nextCursor") # Используем "nextCursor" как в вашем примере

                # Изменено: Прерываем цикл, если next_cursor отсутствует (None или пустая строка).
                if not next_cursor:
                     self._bot._log('debug', 'Получен пустой или отсутствующий nextCursor. Сбор завершен.', 'giveaway')
                     break

                # Если курсор есть, используем его для следующего запроса
                current_cursor = next_cursor
                self._bot._log('debug', f'Следующий cursor: "{current_cursor}".', 'giveaway')

                await self._bot._random_delay()

            except Exception as e:
                self._bot._log('error', f'Ошибка при сборе розыгрышей на странице {page_count}: {e}', 'error')
                # В случае ошибки сбора, обрабатываем то, что удалось собрать
                break # При ошибке прерываем сбор

        self._bot._log('info', f'Сбор уникальных розыгрышей завершен. Всего собрано {len(collected_giveaways)} для обработки.', 'giveaway')
        return collected_giveaways


    async def _process_available_giveaways(self) -> None:
        self._bot._log('debug', 'Начинаем обработку доступных розыгрышей...', 'giveaway')
        try:
            # Собираем уникальные розыгрыши до появления повторов или пустого списка
            giveaway_list = await self._collect_giveaways_until_repeat()

            if not giveaway_list:
                self._bot._log('debug', 'Нет новых розыгрышей для обработки.', 'giveaway')
                return

            self._bot._log('info', f'Найдено {len(giveaway_list)} новых розыгрышей для обработки.', 'giveaway')

            for giveaway in giveaway_list:
                await self._process_giveaway(giveaway) # _process_giveaway теперь сам проверяет и помечает в БД
                await self._bot._random_delay()


        except Exception as e:
            self._bot._log('error', f' Ошибка при получении/обработке розыгрышей: {e}', 'error')

    async def process_giveaways(self) -> None:
        # Очистка старых записей об обработанных розыгрышах (опционально и настраиваемо)
        # Это можно сделать здесь или в run_tapper, решать вам.
        # await self._channel_repository.clear_old_processed_giveaways(days_to_keep=settings.PROCESSED_GIVEAWAYS_DAYS_TO_KEEP)
        await self._process_available_giveaways()


async def run_tapper(tg_client: Any) -> None:
    bot = BaseBot(tg_client)

    channel_repository = ChannelRepository()
    await channel_repository.initialize()
    # Очистка старых записей об обработанных розыгрышах при старте бота
    # Убедитесь, что в settings есть настройка PROCESSED_GIVEAWAYS_DAYS_TO_KEEP
    if hasattr(settings, 'PROCESSED_GIVEAWAYS_DAYS_TO_KEEP') and settings.PROCESSED_GIVEAWAYS_DAYS_TO_KEEP is not None:
         bot._log('info', f'Очистка старых записей об обработанных розыгрышах (старше {settings.PROCESSED_GIVEAWAYS_DAYS_TO_KEEP} дней)...', 'info')
         await channel_repository.clear_old_processed_giveaways(days_to_keep=settings.PROCESSED_GIVEAWAYS_DAYS_TO_KEEP)
         bot._log('info', 'Очистка завершена.', 'info')
    else:
         bot._log('warning', 'Настройка PROCESSED_GIVEAWAYS_DAYS_TO_KEEP не найдена. Пропуск очистки старых записей.', 'warning')


    update_task = None
    if settings.AUTO_UPDATE:
        update_manager = UpdateManager()
        update_task = asyncio.create_task(update_manager.run())
        bot._log('info', 'Задача автоматического обновления запущена.', 'info')

    error_handler = ErrorHandler(session_manager=bot, logger=bot._logger)

    sleep_duration = random.uniform(1, settings.SESSION_START_DELAY)
    bot._log('info', f' Сессия запустится через ⌚ <g>{int(sleep_duration)} секунд...</g>', 'info')
    await asyncio.sleep(sleep_duration)

    try:
        await bot.auth()

        try:
            while True:
                try:
                    bot._log('debug', 'Получение информации профиля...', 'info')
                    me = await bot.get_me()
                    bot._log('debug', f'Профиль: {me}', 'debug')
                    await bot._random_delay()

                    bot._log('debug', 'Проверка баланса...', 'balance')
                    await bot.check_balance()
                    await bot._random_delay()

                    # Добавляем проверку подарков сразу после проверки баланса
                    bot._log('debug', 'Проверка подарков...', 'giveaway')
                    gifts_data = await bot.get_gifts()
                    await bot._random_delay()

                    # Отправляем уведомление, если подарки найдены
                    if gifts_data.get("gifts"):
                        session_name = getattr(tg_client, "session_name", "Неизвестная сессия")
                        уведомление_о_подарке = f"Обнаружен подарок на \`{session_name}\`"
                        # Проверяем, настроен ли чат ID для уведомлений
                        if settings.get('NOTIFICATION_CHAT_ID'):
                             await bot._send_telegram_message(settings.NOTIFICATION_CHAT_ID, уведомление_о_подарке)
                        else:
                             bot._log('warning', 'NOTIFICATION_CHAT_ID не настроен. Уведомление о подарке не отправлено.', 'warning')


                    bot._log('debug', 'Получение статистики подарков...', 'info')
                    stats = await bot.get_gift_statistics()
                    bot._log('debug', f'Статистика: {stats}', 'debug')
                    await bot._random_delay()

                    giveaway_processor = GiveawayProcessor(bot, channel_repository)
                    await giveaway_processor.process_giveaways() # Этот метод теперь управляет сбором и обработкой

                    # Задержка перед следующим циклом теперь после обработки всех собранных розыгрышей
                    # Можно настроить отдельную задержку для цикла обработки розыгрышей,
                    # чтобы не ждать CHANNEL_SUBSCRIBE_DELAY после каждого цикла парсинга.
                    # Пока оставим так, но имейте в виду.
                    sleep_duration = settings.CHANNEL_SUBSCRIBE_DELAY + random.uniform(0, 300) # Используем существующую настройку, или можно добавить новую
                    bot._log('info', f'Уход на паузу перед следующим циклом на {int(sleep_duration)} секунд...', 'info')
                    await asyncio.sleep(sleep_duration)

                except Exception as inner_e:
                    status_code = getattr(inner_e, 'status', None)
                    error_handler.handle_error(str(inner_e), error_code=status_code)
                    # Обработчик ошибок уже включает логику сна/повтора для 401,
                    # для других ошибок может потребоваться дополнительная логика,
                    # но пока оставим как есть, т.к. handle_error логирует и завершает.

        except UnauthorizedError as auth_error:
            bot._log('warning', f'Обнаружена ошибка авторизации, остановка сессии: {auth_error}', 'warning')
            # Внешняя логика должна будет перезапустить эту сессию

    except Exception as e:
        bot._log('error', f'Критическая ошибка в процессе выполнения: {e}', 'error')
        error_handler.handle_error(str(e))
    finally:
        bot._log('info', ' Завершение функции run_tapper.', 'info')
        if update_task:
            update_task.cancel()
            try:
                await update_task
            except asyncio.CancelledError:
                bot._log('info', 'Задача автоматического обновления отменена.', 'info')
        # Закрываем соединение с базой данных и HTTP клиент
        await channel_repository.close()
        await bot.close()
