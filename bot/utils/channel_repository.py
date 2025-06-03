import aiosqlite
import datetime # Импортируем datetime для работы с датами
from typing import Optional, List

class ChannelRepository:
    def __init__(self, db_path: str = "channels.db"):
        self._db_path = db_path

    async def initialize(self) -> None:
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                "CREATE TABLE IF NOT EXISTS subscribed_channels ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "session_name TEXT NOT NULL, "
                "channel_name TEXT NOT NULL, "
                "UNIQUE(session_name, channel_name))"
            )
            # Новая таблица для обработанных розыгрышей
            await db.execute(
                "CREATE TABLE IF NOT EXISTS processed_giveaways ("
                "giveaway_id TEXT NOT NULL UNIQUE, "
                "processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            await db.commit()

    async def is_subscribed(self, session_name: str, channel_name: str) -> bool:
        async with aiosqlite.connect(self._db_path) as db:
            cursor = await db.execute(
                "SELECT 1 FROM subscribed_channels WHERE session_name = ? AND channel_name = ?",
                (session_name, channel_name)
            )
            result = await cursor.fetchone()
            await cursor.close()
            return result is not None

    async def add_channel(self, session_name: str, channel_name: str) -> None:
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                "INSERT OR IGNORE INTO subscribed_channels (session_name, channel_name) VALUES (?, ?)",
                (session_name, channel_name)
            )
            await db.commit()

    # Новые методы для работы с обработанными розыгрышами

    async def add_processed_giveaway(self, giveaway_id: str) -> None:
        """Добавляет ID обработанного розыгрыша в базу данных."""
        async with aiosqlite.connect(self._db_path) as db:
            try:
                await db.execute(
                    'INSERT INTO processed_giveaways (giveaway_id) VALUES (?)',
                    (giveaway_id,)
                )
                await db.commit()
            except aiosqlite.IntegrityError:
                # Розыгрыш уже в базе, игнорируем ошибку
                pass

    async def is_giveaway_processed(self, giveaway_id: str) -> bool:
        """Проверяет, был ли уже обработан розыгрыш по ID."""
        async with aiosqlite.connect(self._db_path) as db:
            cursor = await db.execute(
                'SELECT 1 FROM processed_giveaways WHERE giveaway_id = ?',
                (giveaway_id,)
            )
            row = await cursor.fetchone()
            await cursor.close()
            return row is not None

    async def clear_old_processed_giveaways(self, days_to_keep: int) -> None:
        """Удаляет записи об обработанных розыгрышах старше заданного количества дней."""
        async with aiosqlite.connect(self._db_path) as db:
            # Используем функцию SQLite date() для работы с датами
            await db.execute(
                '''DELETE FROM processed_giveaways WHERE processed_at < date('now', ?)''',
                (f'-{days_to_keep} days',)
            )
            await db.commit() 