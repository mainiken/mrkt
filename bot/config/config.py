from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Dict, Tuple, Optional
from enum import Enum

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_ignore_empty=True)

    API_ID: int = None
    API_HASH: str = None
    GLOBAL_CONFIG_PATH: str = "TG_FARM"

    FIX_CERT: bool = False

    SESSION_START_DELAY: int = 360

    REF_ID: str = '252453226'
    SESSIONS_PER_PROXY: int = 1
    USE_PROXY: bool = True
    DISABLE_PROXY_REPLACE: bool = True

    DEVICE_PARAMS: bool = False

    DEBUG_LOGGING: bool = False

    AUTO_UPDATE: bool = True
    CHECK_UPDATE_INTERVAL: int = 60
    BLACKLISTED_SESSIONS: str = ""
    
    SUBSCRIBE_TELEGRAM: bool = True

    # Настройки задержки между подписками на каналы
    CHANNEL_SUBSCRIBE_DELAY: int = 20

    # Настройки для участия в бесплатных розыгрышах
    PARTICIPATE_IN_FREE_GIVEAWAYS: bool = True
    GIVEAWAY_MIN_PARTICIPANTS: int = 0
    GIVEAWAY_MAX_PARTICIPANTS: int = 100000
    GIVEAWAY_REQUIRE_PREMIUM: Optional[bool] = False
    GIVEAWAY_REQUIRE_ACTIVE_TRADER: bool = False
    GIVEAWAY_REQUIRE_CHANNEL_BOOST: bool = False
    GIVEAWAY_SKIP_CHANNEL_BOOST_REQUIRED: bool = True
    GIVEAWAY_SKIP_CHANNEL_SUBSCRIBE_REQUIRED: bool = False

    # Настройки для получения списка розыгрышей
    GIVEAWAY_LIST_TYPE: str = "Free" # e.g., "Available", "Joined", "Winning", "Free"
    GIVEAWAY_LIST_COUNT: int = 50
    GIVEAWAY_LIST_CURSOR: str = "" # Оставить пустым для первого запроса

    # Новая настройка: Максимальное количество розыгрышей для обработки за один проход сессии
    GIVEAWAY_MAX_PER_RUN: int = 100 # Значение по умолчанию, можно настроить

        # Настройки для отписки от неактивных каналов
    GIVEAWAY_CHANNEL_INACTIVITY_HOURS: int = 24 # Часов неактивности, после которых канал считается неактивным
    GIVEAWAY_CHANNEL_LEAVE_CHECK_INTERVAL: int = 3600 # Интервал (в секундах) между проверками неактивных каналов

    # Настройки для Telegram уведомлений
    NOTIFICATION_BOT_TOKEN: Optional[str] = None
    NOTIFICATION_CHAT_ID: Optional[int] = None

    ENABLE_NOTIFICATION_BOT: bool = False

    # Настройка для срока хранения записей об обработанных розыгрышах (в днях)
    PROCESSED_GIVEAWAYS_DAYS_TO_KEEP: Optional[int] = 3

    # Настройки лимитов действий с каналами в минуту
    MAX_SUBSCRIBE_PER_MINUTE: int = 40
    MAX_UNSUBSCRIBE_PER_MINUTE: int = 40

    @property
    def blacklisted_sessions(self) -> List[str]:
        return [s.strip() for s in self.BLACKLISTED_SESSIONS.split(',') if s.strip()]

settings = Settings()
