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

    # Добавляем атрибуты для отслеживания количества действий с каналами в текущую минуту
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

    async def _reauthenticate(self) -> bool:
        """Попытка повторной авторизации."""
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
        """Универсальный метод для выполнения API запросов с обработкой 401 и повторной авторизацией."""
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
                        # Если 401 получен и попытки исчерпаны, или повторная авторизация не удалась
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
        """Получает список подарков для текущей сессии."""
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
        """Получает одну страницу розыгрышей."""
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
            self._log('info', f'Успешно присоединились к розыгрышу ⚡<y>{giveaway_title or giveaway_id}</y>!', 'success')
            await self._random_delay()
            return {"success": True, "result": result}
        except Exception as e:
            self._log('info', f'Не удалось присоединиться к розыгрышу {giveaway_title or giveaway_id}: {e}', 'warning')
            return {"success": False, "status": getattr(e, 'status', 0), "message": str(e)}

    async def _random_delay(self) -> None:
        """Добавляет случайную задержку между 1 и 3 секундами."""
        delay = random.uniform(1, 3)
        self._log('debug', f'Добавление случайной задержки: {delay:.2f} сек.', 'info')
        await asyncio.sleep(delay)

    async def _wait_for_next_minute(self) -> None:
        """Ожидает до начала следующей минуты для сброса лимитов действий."""
        now = datetime.datetime.now()
        seconds_to_next_minute = 60 - now.second
        # Добавляем небольшую случайную задержку, чтобы избежать одновременного сброса для всех сессий
        delay = seconds_to_next_minute + random.uniform(0, 1)
        if delay > 0:
            self._log('debug', f'Ожидание {delay:.2f} секунд до начала следующей минуты для сброса лимитов.', 'info')
            await asyncio.sleep(delay)
        # После ожидания убеждаемся, что мы действительно в следующей минуте или позже
        self._channel_action_window_start = datetime.datetime.now()


    async def _check_and_apply_rate_limit(self, action_type: str) -> None:
        """Проверяет и применяет ограничение частоты для подписки/отписки."""
        now = datetime.datetime.now()
        # Если прошло более 60 секунд с начала текущего окна, сбрасываем счетчики
        if (now - self._channel_action_window_start).total_seconds() >= 60:
            self._log('debug', 'Окно минуты для действий с каналами сброшено.', 'debug')
            self._channel_action_counts = {"subscribe": 0, "unsubscribe": 0}
            self._channel_action_window_start = now # Новое окно начинается сейчас

        current_count = self._channel_action_counts.get(action_type, 0)
        max_limit = 0
        if action_type == 'subscribe':
            # Используем MAX_SUBSCRIBE_PER_MINUTE из settings, по умолчанию 40
            max_limit = getattr(settings, 'MAX_SUBSCRIBE_PER_MINUTE', 40)
        elif action_type == 'unsubscribe':
             # Используем MAX_UNSUBSCRIBE_PER_MINUTE из settings, по умолчанию 40
            max_limit = getattr(settings, 'MAX_UNSUBSCRIBE_PER_MINUTE', 40)
        else:
             self._log('error', f'Неизвестный тип действия для ограничения частоты: {action_type}', 'error')
             return # Не обрабатываем неизвестные типы

        if current_count >= max_limit:
            self._log('info', f'Лимит на <y>{action_type}</y> ({max_limit} в минуту) достигнут. Ожидание до начала следующей минуты.', 'warning')
            await self._wait_for_next_minute()
            # После ожидания _wait_for_next_minute уже сбросил окно и счетчики

        # Увеличиваем счетчик для текущего действия в текущем окне
        self._channel_action_counts[action_type] += 1
        self._log('debug', f'Выполнено {self._channel_action_counts[action_type]}/{max_limit} <y>{action_type}</y> действий в текущей минуте.', 'debug')


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
        # Добавим настройку времени неактивности каналов для отписки (в часах)
        # По умолчанию 12 часов, как предложено пользователем
        self._inactivity_threshold_hours = getattr(settings, 'GIVEAWAY_CHANNEL_INACTIVITY_HOURS', 12)
        # Добавим настройку интервала проверки неактивных каналов (в секундах)
        # По умолчанию раз в час
        self._check_interval_seconds = getattr(settings, 'GIVEAWAY_CHANNEL_LEAVE_CHECK_INTERVAL', 3600)
        self._last_leave_check_time: datetime.datetime = datetime.datetime.now() - datetime.timedelta(
            seconds=self._check_interval_seconds) # Инициализируем так, чтобы первая проверка была сразу

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
            self._bot._log('info', f'Канал <y>{channel_name}</y> уже в базе, пропускаем подписку.', 'success')
            # Если канал уже в базе, обновляем время его активности, т.к. он связан с актуальным розыгрышем
            await self._channel_repository.update_channel_activity(session_name, channel_name)
            return True
        if current_is_member_status == "Validated":
            await self._channel_repository.add_channel(session_name, channel_name)
            # Обновляем метку времени участия после успешной валидации
            await self._channel_repository.update_giveaway_participation_timestamp(
                session_name, channel_name
            )
            self._bot._log('info', f' Подписка на канал <y>{channel_name}</y> подтверждена.', 'success')
            # Если подписка подтверждена сервером, добавляем канал с текущей активностью
            await self._channel_repository.update_channel_activity(session_name, channel_name)
            return True

        if hasattr(settings, 'GIVEAWAY_SKIP_CHANNEL_SUBSCRIBE_REQUIRED') and settings.GIVEAWAY_SKIP_CHANNEL_SUBSCRIBE_REQUIRED:
            self._bot._log('debug', f'Пропускаем проверку подписки на канал <y>{channel_name}</y> по настройке.', 'info')
            return False

        self._bot._log('debug', f'Попытка подписаться на канал <y>{channel_name}</y>', 'debug')

        try:
            # Применяем ограничение частоты перед подпиской
            await self._bot._check_and_apply_rate_limit("subscribe")
            # join_telegram_channel теперь сам управляет подключением/отключением и FloodWait
            channel_join_success = await self._bot._tg_client.join_telegram_channel(
                {"additional_data": {"username": channel_name}}
            )
            if not channel_join_success:
                self._bot._log('info', f'Не удалось вступить в канал <y>{channel_name}</y>.', 'warning')
                return False

            self._bot._log('info', f' Вступление в канал <y>{channel_name}</y> успешно.', 'success')
            # После успешного вступления, добавляем/обновляем канал с текущей активностью
            await self._channel_repository.add_channel(session_name, channel_name)

            if hasattr(settings, 'CHANNEL_SUBSCRIBE_DELAY'):
                 # Задержка после подписки перенесена в run_tapper после обработки розыгрыша
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
                    # Если валидация подтверждена после подписки, обновляем время активности
                    await self._channel_repository.update_channel_activity(session_name, channel_name)
                    # Обновляем метку времени участия после успешной валидации
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

    async def _process_giveaway(self, giveaway: Dict[str, Any]) -> None:
        giveaway_id = giveaway.get("id")
        giveaway_title = giveaway.get("previewGift", {}).get("title", "Неизвестно")
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
                        # Проверяем validationStatus из исходного объекта розыгрыша для "корректной" обработки
                        if giveaway.get("validationStatus") == "Validated":
                            self._bot._log('info', f'Успешно присоединились к розыгрышу ⚡<y>{giveaway_title}</y> (validationStatus: Validated)!', 'success')
                            session_name = getattr(self._bot._tg_client, "session_name", "unknown_session")
                            for channel_validation in channels_to_process:
                                 channel_name = channel_validation.get("channel")
                                 if channel_name:
                                      await self._channel_repository.update_channel_activity(session_name, channel_name)
                                      self._bot._log('debug', f'Время активности для канала <y>{channel_name}</y> обновлено после присоединения к розыгрышу <y>{giveaway_title}</y>.', 'debug')
                        else:
                            self._bot._log('warning', f'Присоединились к розыгрышу <y>{giveaway_title}</y>, но его "validationStatus" не "Validated" (фактический статус: {giveaway.get("validationStatus")}).', 'warning')
                    else:
                        self._bot._log('info', f'Не удалось принять участие в розыгрыше <y>{giveaway_title}</y>: {join_result.get("message", "Ошибка")}', 'warning')

                    # Всегда помечаем как обработанный после попытки (успешной или нет, с валидацией или без)
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
        """Собирает уникальные розыгрыши постранично до появления повтора или достижения лимита."""
        self._bot._log('debug', 'Начинаем сбор уникальных розыгрышей до появления повторов или достижения лимита...', 'giveaway')
        collected_giveaways: List[Dict[str, Any]] = []
        collected_giveaway_ids_this_run: Set[str] = set() # Сет для отслеживания ID в ТЕКУЩЕМ цикле сбора
        current_cursor = ""
        page_count = 0

        # Получаем максимальное количество розыгрышей из настроек
        max_giveaways = getattr(settings, 'GIVEAWAY_MAX_PER_RUN', 100)

        while True:
            # Проверяем, достигли ли мы максимального лимита перед запросом следующей страницы
            if len(collected_giveaways) >= max_giveaways:
                self._bot._log('info', f'Достигнут лимит ({max_giveaways}) на количество собираемых розыгрышей за проход. Сбор завершен.', 'giveaway')
                break

            page_count += 1
            self._bot._log('debug', f'Запрос страницы {page_count} с cursor="{current_cursor}" (Собрано: {len(collected_giveaways)})...', 'giveaway')
            try:
                giveaways_data = await self._bot.get_giveaways_page(
                    giveaway_type=getattr(settings, 'GIVEAWAY_LIST_TYPE', "Available"),
                    count=getattr(settings, 'GIVEAWAY_LIST_COUNT', 20), # Размер страницы
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
                    # Проверяем, не превысит ли добавление этого розыгрыша лимит
                    if len(collected_giveaways) + len(new_giveaways_on_page) >= max_giveaways:
                         self._bot._log('debug', f'Добавление следующего розыгрыша превысит лимит {max_giveaways}. Завершаем сбор на текущей странице.', 'debug')
                         break # Прерываем перебор элементов на текущей странице


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

        self._bot._log('info', f'Сбор уникальных розыгрышей завершен. Всего собрано {len(collected_giveaways)} для обработки (лимит: {max_giveaways}).', 'giveaway')
        return collected_giveaways


    async def _process_available_giveaways(self) -> None:
        self._bot._log('debug', 'Начинаем обработку доступных розыгрышей...', 'giveaway')
        try:
            # Собираем уникальные розыгрыши до появления повторов или пустого списка
            giveaway_list = await self._collect_giveaways_until_repeat()
            giveaway_list = await self._filter_giveaways(giveaway_list)

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
        # await self._channel_repository.clear_old_processed_giveaways(days_to_keep=settings.PROCESSED_GIVEAWAYS_DAYS_TO_KEEP)
        await self._process_available_giveaways()

    async def leave_inactive_channels(self) -> None:
        """Периодически проверяет и отписывается от неактивных каналов."""
        current_time = datetime.datetime.now()
        # Проверяем, прошло ли достаточно времени с последней проверки
        if current_time - self._last_leave_check_time < datetime.timedelta(seconds=self._check_interval_seconds):
            self._bot._log('debug', 'Время для проверки неактивных каналов еще не пришло.', 'debug')
            return

        self._bot._log('info', 'Начинаем проверку неактивных каналов для отписки...', 'info')
        session_name = getattr(self._bot._tg_client, "session_name", "unknown_session")

        try:
            channels_to_leave = await self._channel_repository.get_channels_to_leave(
                session_name, self._inactivity_threshold_hours
            )

            if not channels_to_leave:
                self._bot._log('info', 'Нет неактивных каналов для отписки.', 'info')
                self._last_leave_check_time = datetime.datetime.now() # Обновляем время проверки
                return

            self._bot._log('info', f'Найдено {len(channels_to_leave)} неактивных каналов для отписки.', 'warning')

            for channel_id, channel_name in channels_to_leave:
                self._bot._log('debug', f'Попытка отписаться от канала <y>{channel_name}</y> (ID: {channel_id})...', 'warning')
                # Применяем ограничение частоты перед отпиской
                await self._bot._check_and_apply_rate_limit("unsubscribe")
                leave_success = await self._bot._tg_client.leave_telegram_channel(channel_name)

                if leave_success:
                    # Удаляем канал из базы после успешной отписки
                    await self._channel_repository.remove_channel(channel_id)
                    self._bot._log('success', f'Успешно отписались от канала <y>{channel_name}</y>.', 'success')
                else:
                    # Если отписка не удалась (например, FloodWait), не удаляем из базы,
                    # чтобы попробовать позже. Логирование ошибки уже есть в leave_telegram_channel.
                    pass # Логика обработки ошибок отписки уже внутри leave_telegram_channel

                # Добавляем небольшую задержку между отписками
                await asyncio.sleep(random.uniform(5, 15)) # Задержка между отписками

        except Exception as e:
            self._bot._log('error', f'Ошибка при проверке/отписке от неактивных каналов: {e}', 'error')

        finally:
            # Обновляем время последней проверки независимо от успеха, чтобы не спамить проверками
            self._last_leave_check_time = datetime.datetime.now()


async def run_tapper(tg_client: Any) -> None:
    bot = BaseBot(tg_client)

    channel_repository = ChannelRepository()
    await channel_repository.initialize()
    # Очистка каналов без подтвержденного участия при старте
    bot._log('debug', f'Очистка каналов без подтвержденного участия для сессии {getattr(tg_client, "session_name", "unknown_session")}...', 'info')
    await channel_repository.clear_unparticipated_channels_on_start(getattr(tg_client, "session_name", "unknown_session"))
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

    sleep_duration = random.uniform(1, settings.SESSION_START_DELAY)
    bot._log('info', f' Сессия запустится через ⌚ <g>{int(sleep_duration)} секунд...</g>', 'info')
    await asyncio.sleep(sleep_duration)

    try:
        await bot.auth()

        try:
            giveaway_processor = GiveawayProcessor(bot, channel_repository)

            while True:
                try:
                    if settings.UNSUBSCRIBE_FROM_INACTIVE_CHANNELS:
                        await giveaway_processor.leave_inactive_channels()
                    else:
                        bot._log('info', 'Отписка от неактивных каналов отключена в настройках.', 'info')

                    bot._log('debug', 'Получение информации профиля...', 'info')
                    me = await bot.get_me()
                    bot._log('debug', f'Профиль: {me}', 'debug')
                    await bot._random_delay()

                    bot._log('debug', 'Проверка баланса...', 'balance')
                    await bot.check_balance()
                    await bot._random_delay()

                    bot._log('debug', 'Проверка подарков...', 'giveaway')
                    gifts_data = await bot.get_gifts()
                    await bot._random_delay()

                    if gifts_data.get("gifts"):
                        session_name = getattr(tg_client, "session_name", "Неизвестная сессия")
                        уведомление_о_подарке = f"Обнаружен подарок на ` {session_name} `"
                        if settings.get('NOTIFICATION_CHAT_ID'):
                            await bot._send_telegram_message(settings.NOTIFICATION_CHAT_ID, уведомление_о_подарке)
                        else:
                            bot._log('warning', 'NOTIFICATION_CHAT_ID не настроен. Уведомление о подарке не отправлено.', 'warning')

                    bot._log('debug', 'Получение статистики подарков...', 'info')
                    stats = await bot.get_gift_statistics()
                    bot._log('debug', f'Статистика: {stats}', 'debug')
                    await bot._random_delay()

                    await giveaway_processor.process_giveaways()

                    sleep_duration = settings.CHANNEL_SUBSCRIBE_DELAY + random.uniform(0, 3000)
                    bot._log('info', f'Уход на паузу перед следующим циклом на {int(sleep_duration)} секунд...', 'info')
                    await asyncio.sleep(sleep_duration)

                except Exception as inner_e:
                    status_code = getattr(inner_e, 'status', None)
                    error_handler.handle_error(str(inner_e), error_code=status_code)

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
