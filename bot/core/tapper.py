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

    _channel_action_counts: Dict[str, int] = {"subscribe": 0, "unsubscribe": 0}
    _channel_action_window_start: datetime.datetime = datetime.datetime.now()

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

    async def _reauthenticate(self) -> bool:
        self._log('info', 'Попытка повторной авторизации...', 'info')
        try:
            await self.auth()
            self._log('success', 'Повторная авторизация успешна.', 'success')
            return True
        except Exception as e:
            self._log('error', f'Повторная авторизация не удалась: {e}', 'error')
            return False

    async def _make_api_request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        retries: int = 2
    ) -> Dict[str, Any]:
        client = await self._get_http_client()
        current_headers = self.DEFAULT_HEADERS.copy()
        if self.token:
            current_headers["authorization"] = self.token
        if headers:
            current_headers.update(headers)

        for attempt in range(retries + 1):
            try:
                response = None
                if method == 'GET':
                    response = await client.get(url, headers=current_headers, params=params)
                elif method == 'POST':
                    response = await client.post(url, headers=current_headers, json=json_data, params=params)
                else:
                    raise ValueError(f"Неподдерживаемый HTTP метод: {method}")

                async with response as resp:
                    resp.raise_for_status()
                    return await resp.json()

            except aiohttp.ClientResponseError as e:
                if e.status == 401:
                    if attempt < retries:
                        self._log('warning', f'Получен 401 Unauthorized. Попытка повторной авторизации (попытка {attempt + 1}/{retries})...', 'warning')
                        if await self._reauthenticate():
                            current_headers["authorization"] = self.token
                            continue
                        else:
                            self._log('error', 'Повторная авторизация не удалась. Отправка UnauthorizedError.', 'error')
                            raise UnauthorizedError(f"Повторная авторизация не удалась после 401: {e.message}")
                    else:
                        self._log('error', f'Повторная авторизация не удалась после 401 или исчерпаны попытки. Отправка UnauthorizedError: {e.message}', 'error')
                        raise UnauthorizedError(f"Авторизация не удалась после 401 и всех попыток: {e.message}")
                else:
                    self._log('error', f'Ошибка при выполнении запроса ({method} {url}): {e.status} {e.message}', 'error')
                    raise Exception(f"Не удалось выполнить запрос: {e.status} {e.message}")

            except Exception as e:
                self._log('error', f'Критическая ошибка сети или другая ошибка при запросе ({method} {url}): {e}', 'error')
                raise

        self._log('error', f'Непредвиденное завершение _make_api_request без возврата или исключения после {retries} попыток.', 'error')
        raise Exception("Непредвиденное завершение _make_api_request")

    async def get_me(self) -> Dict[str, Any]:
        self._log('debug', 'Получение информации о пользователе...', 'info')
        result = await self._make_api_request('GET', self.ME_URL)
        self._log('debug', f'Информация о пользователе получена: {result}', 'debug')
        await self._random_delay()
        return result

    async def get_balance(self) -> Dict[str, Any]:
        self._log('debug', 'Получение баланса...', 'balance')
        result = await self._make_api_request('GET', self.BALANCE_URL)
        self._log('debug', f'Баланс получен: {result}', 'debug')
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
        self._log('debug', f'Проверка кошелька с TON: {ton} и Device ID: {device_id}...', 'info')
        data = {"ton": ton, "deviceId": device_id}
        result = await self._make_api_request('POST', self.WALLET_URL, json_data=data)
        self._log('debug', f'Кошелек проверен: {result}', 'debug')
        await self._random_delay()
        return result

    async def get_gift_statistics(self) -> Dict[str, Any]:
        self._log('debug', 'Получение статистики подарков...', 'info')
        result = await self._make_api_request('GET', self.GIFT_STATISTICS_URL)
        self._log('debug', f'Статистика подарков получена: {result}', 'debug')
        await self._random_delay()
        return result

    async def get_gifts(self) -> Dict[str, Any]:
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
        result = await self._make_api_request('POST', self.GIFTS_URL, json_data=payload)
        gifts_count = len(result.get("gifts", []))
        self._log('debug', f'Получено {gifts_count} подарков.', 'giveaway')
        if gifts_count > 0:
            self._log('info', f'Обнаружен подарок на "{getattr(self._tg_client, "session_name", "Неизвестная сессия")}"', 'giveaway')
        await self._random_delay()
        return result

    async def get_giveaways_page(self, giveaway_type: str = "Available", count: int = 20, cursor: str = "") -> Dict[str, Any]:
        params = {"type": giveaway_type, "count": count, "cursor": cursor}
        self._log('debug', f'Получение страницы розыгрышей с параметрами: {params}', 'giveaway')
        result = await self._make_api_request('GET', self.GIVEAWAYS_URL, params=params)
        self._log('debug', f'Получено {len(result.get("items", []))} розыгрышей на странице.', 'giveaway')
        await self._random_delay()
        return result

    async def check_giveaway_validations(self, giveaway_id: str) -> Dict[str, Any]:
        url = f"{self.GIVEAWAY_VALIDATIONS_URL}/{giveaway_id}"
        self._log('debug', f'Проверка условий розыгрыша {giveaway_id}', 'giveaway')
        result = await self._make_api_request('GET', url)
        self._log('debug', f'Условия розыгрыша {giveaway_id} проверены.', 'debug')
        await self._random_delay()
        return result

    async def start_giveaway_validation(self, giveaway_id: str, channel: str, validation_type: str) -> Dict[str, str]:
        url = f"{self.GIVEAWAY_START_VALIDATION_URL}/{giveaway_id}?channel={channel}&type={validation_type}"
        self._log('debug', f'Запуск валидации для розыгрыша {giveaway_id}, канала {channel}, типа {validation_type}', 'giveaway')
        try:
            await self._make_api_request('POST', url)
            self._log('debug', f'Валидация для розыгрыша {giveaway_id}, канала {channel} успешно запущена.', 'debug')
            await self._random_delay()
            return {"status": "Success"}
        except Exception as e:
            self._log('info', f'Серверная валидация канала {channel} не запущена: {e}', 'warning')
            return {"status": "Failed", "message": str(e)}

    async def join_giveaway(self, giveaway_id: str, giveaway_title: Optional[str] = None) -> Dict[str, Any]:
        url = f"{self.GIVEAWAY_BUY_TICKETS_URL}/{giveaway_id}?count=1"
        self._log('debug', f'Попытка присоединиться к розыгрышу {giveaway_title or giveaway_id}', 'giveaway')
        try:
            result = await self._make_api_request('POST', url)
            self._log('debug', f'Запрос на присоединение к розыгрышу ⚡<y>{giveaway_title or giveaway_id}</y> отправлен.', 'success')
            await self._random_delay()
            return {"success": True, "result": result}
        except Exception as e:
            self._log('info', f'Не удалось присоединиться к розыгрышу {giveaway_title or giveaway_id}: {e}', 'warning')
            return {"success": False, "status": getattr(e, 'status', 0), "message": str(e)}

    async def _random_delay(self) -> None:
        delay = random.uniform(1, 3)
        self._log('debug', f'Добавление случайной задержки: {delay:.2f} сек.', 'info')
        await asyncio.sleep(delay)

    async def _wait_for_next_minute(self) -> None:
        now = datetime.datetime.now()
        seconds_to_next_minute = 60 - now.second
        delay = seconds_to_next_minute + random.uniform(0, 1)
        if delay > 0:
            self._log('debug', f'Ожидание {delay:.2f} секунд до начала следующей минуты для сброса лимитов.', 'info')
            await asyncio.sleep(delay)
        self._channel_action_window_start = datetime.datetime.now()


    async def _check_and_apply_rate_limit(self, action_type: str) -> None:
        now = datetime.datetime.now()
        if (now - self._channel_action_window_start).total_seconds() >= 60:
            self._log('debug', 'Окно минуты для действий с каналами сброшено.', 'debug')
            self._channel_action_counts = {"subscribe": 0, "unsubscribe": 0}
            self._channel_action_window_start = now

        current_count = self._channel_action_counts.get(action_type, 0)
        max_limit = 0
        if action_type == 'subscribe':
            max_limit = getattr(settings, 'MAX_SUBSCRIBE_PER_MINUTE', 40)
        elif action_type == 'unsubscribe':
            max_limit = getattr(settings, 'MAX_UNSUBSCRIBE_PER_MINUTE', 40)
        else:
             self._log('error', f'Неизвестный тип действия для ограничения частоты: {action_type}', 'error')
             return

        if current_count >= max_limit:
            self._log('info', f'Лимит на <y>{action_type}</y> ({max_limit} в минуту) достигнут. Ожидание до начала следующей минуты.', 'warning')
            await self._wait_for_next_minute()

        self._channel_action_counts[action_type] += 1
        self._log('debug', f'Выполнено {self._channel_action_counts[action_type]}/{max_limit} <y>{action_type}</y> действий в текущей минуте.', 'debug')


    async def _send_telegram_message(self, chat_id: str, message: str) -> bool:
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
        self._inactivity_threshold_hours = getattr(settings, 'GIVEAWAY_CHANNEL_INACTIVITY_HOURS', 12)
        self._check_interval_seconds = getattr(settings, 'GIVEAWAY_CHANNEL_LEAVE_CHECK_INTERVAL', 3600)
        self._last_leave_check_time: datetime.datetime = datetime.datetime.now() - datetime.timedelta(
            seconds=self._check_interval_seconds)

    async def _filter_giveaways(self, giveaways: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        filtered = []
        for giveaway in giveaways:
            giveaway_id = giveaway.get("id")
            if not giveaway_id:
                self._bot._log('debug', 'Пропускаем розыгрыш без ID.', 'warning')
                continue

            giveaway_title = giveaway.get("previewGift", {}).get("title", "Неизвестно")
            collection_name = giveaway.get("previewGift", {}).get("collectionName", "")

            # Проверка на черный список коллекций подарков
            if settings.blacklisted_gift_collection_names and collection_name in settings.blacklisted_gift_collection_names:
                self._bot._log('debug', f'Пропускаем розыгрыш "{giveaway_title}" (ID: {giveaway_id}) из-за нахождения коллекции "{collection_name}" в черном списке.', 'warning')
                continue

            # Проверка условий фильтрации на основе настроек
            is_boost_required = giveaway.get("isChanelBoostRequired", False)
            is_premium_required = giveaway.get("isForPremium", False)
            is_active_trader_required = giveaway.get("isForActiveTraders", False)

            # Если требуется буст канала и настройка GIVEAWAY_SKIP_CHANNEL_BOOST_REQUIRED включена, пропускаем
            if settings.GIVEAWAY_SKIP_CHANNEL_BOOST_REQUIRED and is_boost_required:
                self._bot._log('debug', f'Пропускаем розыгрыш "{giveaway_title}" (ID: {giveaway_id}) так как требуется буст канала и включена настройка пропуска.', 'warning')
                continue

            # Если участвуем только в бесплатных и розыгрыш не бесплатный (требует премиум или активного трейдера), пропускаем
            if settings.PARTICIPATE_IN_FREE_GIVEAWAYS and (is_premium_required or is_active_trader_required):
                 self._bot._log('debug', f'Пропускаем розыгрыш "{giveaway_title}" (ID: {giveaway_id}) так как он не является бесплатным (требует премиум/активного трейдера).', 'warning')
                 continue

            # Проверка на минимальное и максимальное количество участников
            participants_count = giveaway.get("participantsCount", 0)
            if participants_count < settings.GIVEAWAY_MIN_PARTICIPANTS:
                 self._bot._log('debug', f'Пропускаем розыгрыш "{giveaway_title}" (ID: {giveaway_id}) так как количество участников ({participants_count}) меньше минимального ({settings.GIVEAWAY_MIN_PARTICIPANTS}).', 'warning')
                 continue

            if participants_count > settings.GIVEAWAY_MAX_PARTICIPANTS:
                 self._bot._log('debug', f'Пропускаем розыгрыш "{giveaway_title}" (ID: {giveaway_id}) так как количество участников ({participants_count}) больше максимального ({settings.GIVEAWAY_MAX_PARTICIPANTS}).', 'warning')
                 continue


            filtered.append(giveaway)
            self._bot._log('debug', f'Найден розыгрыш, подходящий по фильтрам: "{giveaway_title}" (ID: {giveaway_id})', 'giveaway')
        return filtered

    async def _check_and_fulfill_channel_validation(
        self, giveaway_id: str, channel_name: str, current_is_member_status: str
    ) -> bool:
        session_name = getattr(self._bot._tg_client, "session_name", "unknown_session")
        if await self._channel_repository.is_subscribed(session_name, channel_name):
            self._bot._log('debug', f'Канал <y>{channel_name}</y> уже в базе, пропускаем подписку.', 'success')
            await self._channel_repository.update_channel_activity(session_name, channel_name)
            return True
        if current_is_member_status == "Validated":
            await self._channel_repository.add_channel(session_name, channel_name)
            await self._channel_repository.update_giveaway_participation_timestamp(
                session_name, channel_name
            )
            self._bot._log('info', f' Подписка на канал <y>{channel_name}</y> подтверждена.', 'success')
            await self._channel_repository.update_channel_activity(session_name, channel_name)
            return True

        if hasattr(settings, 'GIVEAWAY_SKIP_CHANNEL_SUBSCRIBE_REQUIRED') and settings.GIVEAWAY_SKIP_CHANNEL_SUBSCRIBE_REQUIRED:
            self._bot._log('debug', f'Пропускаем проверку подписки на канал <y>{channel_name}</y> по настройке.', 'info')
            return False

        self._bot._log('debug', f'Попытка подписаться на канал <y>{channel_name}</y>', 'debug')

        try:
            await self._bot._check_and_apply_rate_limit("subscribe")
            channel_join_success = await self._bot._tg_client.join_telegram_channel(
                {"additional_data": {"username": channel_name}}
            )
            if not channel_join_success:
                self._bot._log('info', f'Не удалось вступить в канал <y>{channel_name}</y>.', 'warning')
                return False

            self._bot._log('info', f' Вступление в канал <y>{channel_name}</y> успешно.', 'success')
            await self._channel_repository.add_channel(session_name, channel_name)

            if hasattr(settings, 'CHANNEL_SUBSCRIBE_DELAY'):
                 pass


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
                    await self._channel_repository.update_channel_activity(session_name, channel_name)
                    await self._channel_repository.update_giveaway_participation_timestamp(
                        session_name, channel_name
                    )
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

    async def _process_giveaway(self, giveaway: Dict[str, Any]) -> Dict[str, Any]:
        """Обрабатывает один розыгрыш, пытаясь к нему присоединиться и выполняя валидации каналов.
        Возвращает словарь с результатом обработки, включая success: bool и message: str.
        """
        giveaway_id = giveaway.get("id")
        giveaway_title = giveaway.get("previewGift", {}).get("title", "Неизвестно")
        session_name = getattr(self._bot._tg_client, "session_name", "unknown_session")

        try:
            self._bot._log('debug', f'Проверяем условия для розыгрыша <y>{giveaway_title}</y>', 'giveaway')
            validations = await self._bot.check_giveaway_validations(giveaway_id)
            can_join = True

            if settings.GIVEAWAY_REQUIRE_PREMIUM and not validations.get("isPremium", False):
                self._bot._log('info', f'Розыгрыш <y>{giveaway_title}</y> требует премиум, пользователь не премиум.', 'warning')
                can_join = False

            if can_join and settings.GIVEAWAY_REQUIRE_ACTIVE_TRADER and not validations.get("isActiveTrader", False):
                self._bot._log('info', f'Розыгрыш <y>{giveaway_title}</y> требует активного трейдера, пользователь не активный трейдер.', 'warning')
                can_join = False

            # Объединяем каналы из validations и из корневого объекта giveaway
            channels_to_process: List[Dict[str, Any]] = []
            if validations.get("channelValidations", []):
                channels_to_process.extend(validations.get("channelValidations", []))
            
            # Добавляем каналы из поля "chanels" корневого объекта giveaway, если их нет в channel_validations
            # Или если channel_validations вообще отсутствует/пуст
            giveaway_channels = giveaway.get("chanels", [])
            for gc_name in giveaway_channels:
                # Проверяем, есть ли этот канал уже в channels_to_process
                if not any(cv.get("channel") == gc_name for cv in channels_to_process):
                    channels_to_process.append({"channel": gc_name, "isMember": None, "isBoosted": None}) # isMember и isBoosted будут определены при проверке

            if can_join:
                for channel_validation in channels_to_process:
                    channel_name = channel_validation.get("channel")
                    if not channel_name:
                        continue

                    is_member = channel_validation.get("isMember")
                    is_boosted = channel_validation.get("isBoosted")

                    if settings.GIVEAWAY_REQUIRE_CHANNEL_BOOST and is_boosted != "Validated":
                        if not settings.GIVEAWAY_SKIP_CHANNEL_BOOST_REQUIRED:
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
                    if join_result.get("success"):
                        if giveaway.get("validationStatus") == "Validated":
                            self._bot._log('info', f'Присоединились к розыгрышу ⚡<y>{giveaway_title}</y>!', 'success')
                            for channel_validation in channels_to_process:
                                 channel_name = channel_validation.get("channel")
                                 if channel_name:
                                      await self._channel_repository.update_channel_activity(session_name, channel_name)
                                      self._bot._log('debug', f'Время активности для канала <y>{channel_name}</y> обновлено после присоединения к розыгрышу <y>{giveaway_title}</y>.', 'debug')
                            await self._channel_repository.remove_pending_giveaway(session_name, giveaway_id)
                            await self._channel_repository.add_processed_giveaway(giveaway_id)
                            return {"success": True, "message": f"Присоединились к розыгрышу {giveaway_title}"}
                        else:
                            message = f'Присоединились к розыгрышу <y>{giveaway_title}</y>, но его "validationStatus" не "Validated" (фактический статус: {giveaway.get("validationStatus")}).'
                            self._bot._log('warning', message, 'warning')
                            await self._channel_repository.remove_pending_giveaway(session_name, giveaway_id)
                            await self._channel_repository.add_processed_giveaway(giveaway_id)
                            return {"success": True, "message": message}
                    else:
                        message = f'Не удалось принять участие в розыгрыше <y>{giveaway_title}</y>: {join_result.get("message", "Ошибка")}'
                        self._bot._log('info', message, 'warning')
                        await self._channel_repository.remove_pending_giveaway(session_name, giveaway_id)
                        await self._channel_repository.add_processed_giveaway(giveaway_id)
                        return {"success": False, "message": message}
                else:
                    message = f'Условия для розыгрыша <y>{giveaway_title}</y> не выполнены. Пропускаем.'
                    self._bot._log('info', message, 'info')
                    await self._channel_repository.remove_pending_giveaway(session_name, giveaway_id)
                    await self._channel_repository.add_processed_giveaway(giveaway_id)
                    return {"success": False, "message": message}


        except Exception as e:
            message = f'Ошибка при обработке розыгрыша <y>{giveaway_title}</y>: {e}'
            self._bot._log('error', message, 'error')
            # В случае ошибки при обработке, удаляем из pending, но не добавляем в processed, чтобы можно было попробовать снова
            # await self._channel_repository.remove_pending_giveaway(session_name, giveaway_id)
            # await self._channel_repository.add_processed_giveaway(giveaway_id) # Не добавляем в processed, если это критическая ошибка
            return {"success": False, "message": message}

    async def _collect_and_filter_giveaways(self) -> List[Dict[str, Any]]:
        """Собирает уникальные розыгрыши постранично, фильтрует их и возвращает список подходящих."""
        self._bot._log('debug', 'Начинаем сбор и фильтрацию уникальных розыгрышей...', 'giveaway')
        collected_giveaways: List[Dict[str, Any]] = []
        collected_giveaway_ids_this_run: Set[str] = set()
        current_cursor = ""
        page_count = 0

        max_giveaways = getattr(settings, 'GIVEAWAY_MAX_PER_RUN', 100)

        while True:
            if len(collected_giveaways) >= max_giveaways:
                self._bot._log('info', f'Достигнут лимит ({max_giveaways})', 'giveaway')
                break

            page_count += 1
            self._bot._log('debug', f'Запрос страницы {page_count} с cursor="{current_cursor}" (Собрано: {len(collected_giveaways)})...', 'giveaway')
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
                    if len(collected_giveaways) + len(new_giveaways_on_page) >= max_giveaways:
                         self._bot._log('debug', f'Добавление следующего розыгрыша превысит лимит {max_giveaways}. Завершаем сбор на текущей странице.', 'debug')
                         break


                    giveaway_id = item.get("id")
                    if not giveaway_id:
                        continue

                    # Проверка 1: был ли этот розыгрыш обработан в ПРОШЛЫХ запусках?
                    if await self._channel_repository.is_giveaway_processed(giveaway_id):
                        self._bot._log('debug', f'Розыгрыш ID:{giveaway_id} уже был обработан ранее. Пропускаем сбор.', 'debug')
                        continue

                    # Проверка 2: был ли этот розыгрыш СОБРАН в ТЕКУЩЕМ цикле сбора?
                    if giveaway_id in collected_giveaway_ids_this_run:
                        self._bot._log('info', f'Обнаружен повторный розыгрыш ID: {giveaway_id} в текущем цикле сбора. Сбор уникальных розыгрышей завершен.', 'giveaway')
                        repeat_found = True
                        break

                    collected_giveaway_ids_this_run.add(giveaway_id)
                    new_giveaways_on_page.append(item)

                collected_giveaways.extend(new_giveaways_on_page)

                if repeat_found:
                    break

                next_cursor = giveaways_data.get("nextCursor")

                if not next_cursor:
                     self._bot._log('debug', 'Получен пустой или отсутствующий nextCursor. Сбор завершен.', 'giveaway')
                     break

                current_cursor = next_cursor
                self._bot._log('debug', f'Следующий cursor: "{current_cursor}".', 'giveaway')

                await self._bot._random_delay()

            except Exception as e:
                self._bot._log('error', f'Ошибка при сборе розыгрышей на странице {page_count}: {e}', 'error')
                break

        filtered_giveaways = await self._filter_giveaways(collected_giveaways)
        self._bot._log('info', f'Отфильтровано {len(filtered_giveaways)} подходящих розыгрышей.', 'giveaway')
        return filtered_giveaways

    async def _add_filtered_giveaways_to_pending_db(self, giveaways: List[Dict[str, Any]]) -> None:
        session_name = getattr(self._bot._tg_client, "session_name", "unknown_session")
        added_count = 0
        for giveaway in giveaways:
            giveaway_id = giveaway.get("id")
            if giveaway_id:
                if not await self._channel_repository.is_giveaway_processed(giveaway_id) and \
                   not await self._channel_repository.is_giveaway_pending(session_name, giveaway_id):
                    await self._channel_repository.add_pending_giveaway(session_name, giveaway_id, giveaway)
                    added_count += 1
        self._bot._log('info', f'Добавлено {added_count} новых розыгрышей в очередь.', 'giveaway')

    async def _process_all_pending_giveaways(self) -> Dict[str, int]:
        session_name = getattr(self._bot._tg_client, "session_name", "unknown_session")
        pending_giveaways = await self._channel_repository.get_pending_giveaways(session_name)
        self._bot._log('info', f'Начинаем обработку {len(pending_giveaways)} розыгрышей из очереди.', 'giveaway')

        successful_joins = 0
        failed_joins = 0

        for giveaway_data in pending_giveaways:
            result = await self._process_giveaway(giveaway_data)
            if result.get("success"):
                successful_joins += 1
            else:
                failed_joins += 1
            await self._bot._random_delay()

        self._bot._log('info', f'Обработка ожидающих розыгрышей завершена. Успешно присоединились: {successful_joins}, Не удалось: {failed_joins}.', 'giveaway')
        return {"successful_joins": successful_joins, "failed_joins": failed_joins}

    async def leave_inactive_channels(self) -> int:
        current_time = datetime.datetime.now()
        if current_time - self._last_leave_check_time < datetime.timedelta(seconds=self._check_interval_seconds):
            self._bot._log('debug', 'Время для проверки неактивных каналов еще не пришло.', 'debug')
            return 0

        self._bot._log('info', 'Начинаем проверку неактивных каналов для отписки...', 'info')
        session_name = getattr(self._bot._tg_client, "session_name", "unknown_session")

        channels_unsubscribed_count = 0
        try:
            channels_to_leave = await self._channel_repository.get_channels_to_leave(
                session_name, self._inactivity_threshold_hours
            )

            if not channels_to_leave:
                self._bot._log('info', 'Нет неактивных каналов', 'info')
                return 0

            self._bot._log('info', f'Найдено {len(channels_to_leave)} неактивных каналов', 'warning')

            for channel_id, channel_name in channels_to_leave:
                self._bot._log('debug', f'Попытка отписаться от канала <y>{channel_name}</y> (ID: {channel_id})...', 'warning')
                await self._bot._check_and_apply_rate_limit("unsubscribe")
                leave_success = await self._bot._tg_client.leave_telegram_channel(channel_name)

                if leave_success:
                    await self._channel_repository.remove_channel(channel_id)
                    self._bot._log('success', f'Успешно отписались от канала <y>{channel_name}</y>.', 'success')
                    channels_unsubscribed_count += 1
                else:
                    pass

                await asyncio.sleep(random.uniform(5, 15))

        except Exception as e:
            self._bot._log('error', f'Ошибка при проверке/отписке от неактивных каналов: {e}', 'error')

        finally:
            self._last_leave_check_time = datetime.datetime.now()
            return channels_unsubscribed_count


async def run_tapper(tg_client: Any) -> None:
    bot = BaseBot(tg_client)

    channel_repository = ChannelRepository()
    await channel_repository.initialize()
    session_name = getattr(tg_client, "session_name", "unknown_session")

    bot._log('debug', f'Очистка каналов без подтвержденного участия для сессии {session_name}...', 'info')
    await channel_repository.clear_unparticipated_channels_on_start(session_name)
    bot._log('debug', 'Очистка каналов без подтвержденного участия завершена.', 'info')

    if hasattr(settings, 'PROCESSED_GIVEAWAYS_DAYS_TO_KEEP') and settings.PROCESSED_GIVEAWAYS_DAYS_TO_KEEP is not None:
        bot._log('debug', f'Очистка старых записей об обработанных розыгрышах (старше {settings.PROCESSED_GIVEAWAYS_DAYS_TO_KEEP} дней)...', 'info')
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

    sleep_duration_initial = random.uniform(1, settings.SESSION_START_DELAY)
    bot._log('info', f' Сессия запустится через ⌚ <g>{int(sleep_duration_initial)} секунд...</g>', 'info')
    await asyncio.sleep(sleep_duration_initial)

    try:
        await bot.auth()

        giveaway_processor = GiveawayProcessor(bot, channel_repository)

        while True:
            successful_joins_cycle = 0
            failed_joins_cycle = 0
            channels_unsubscribed_cycle = 0

            try:
                bot._log('debug', 'Проверка баланса...', 'balance')
                await bot.check_balance()
                await bot._random_delay()

                collected_and_filtered_giveaways = await giveaway_processor._collect_and_filter_giveaways()
                await giveaway_processor._add_filtered_giveaways_to_pending_db(collected_and_filtered_giveaways)

                processing_results = await giveaway_processor._process_all_pending_giveaways()
                successful_joins_cycle = processing_results.get("successful_joins", 0)
                failed_joins_cycle = processing_results.get("failed_joins", 0)

                if settings.UNSUBSCRIBE_FROM_INACTIVE_CHANNELS:
                    channels_unsubscribed_cycle = await giveaway_processor.leave_inactive_channels()
                else:
                    bot._log('debug', 'Отписка от неактивных каналов отключена в настройках.', 'info')

                bot._log('info', f'⭐ Цикл завершен. Результаты сессии ({session_name}):'
                                 f' Успешно {successful_joins_cycle} розыгрыш.'
                                 f' Не удалось {failed_joins_cycle}.'
                                 f' Отписались {channels_unsubscribed_cycle}.', 'info')
                
                bot._log('debug', 'Проверка подарков...', 'giveaway')
                gifts_data = await bot.get_gifts()
                await bot._random_delay()

                if gifts_data.get("gifts"):
                    уведомление_о_подарке = f"Обнаружен подарок на ` {session_name} `"
                    if settings.get('NOTIFICATION_CHAT_ID'):
                        await bot._send_telegram_message(settings.NOTIFICATION_CHAT_ID, уведомление_о_подарке)
                    else:
                        bot._log('warning', 'NOTIFICATION_CHAT_ID не настроен. Уведомление о подарке не отправлено.', 'warning')

                bot._log('debug', 'Получение статистики подарков...', 'info')
                stats = await bot.get_gift_statistics()
                bot._log('debug', f'Статистика: {stats}', 'debug')
                await bot._random_delay()


                sleep_duration_cycle = getattr(settings, 'MAIN_LOOP_DELAY', 300) + random.uniform(0, 1000)
                bot._log('info', f'Уход на паузу перед следующим циклом на {int(sleep_duration_cycle)} секунд...', 'info')
                await asyncio.sleep(sleep_duration_cycle)

            except Exception as inner_e:
                status_code = getattr(inner_e, 'status', None)
                error_handler.handle_error(str(inner_e), error_code=status_code)

    except UnauthorizedError as auth_error:
        bot._log('warning', f'Обнаружена ошибка авторизации, остановка сессии: {auth_error}', 'warning')

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
        await channel_repository.close()
        await bot.close()
