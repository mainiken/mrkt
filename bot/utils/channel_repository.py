import aiosqlite
import datetime # Импортируем datetime для работы с датами
from typing import Optional, List, Tuple # Добавляем Tuple для подсказки типов

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
                "last_activity_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                "giveaway_participation_at TIMESTAMP NULL, "
                "UNIQUE(session_name, channel_name))"
            )
            # Новая таблица для обработанных розыгрышей
            await db.execute(
                "CREATE TABLE IF NOT EXISTS processed_giveaways ("
                "giveaway_id TEXT NOT NULL UNIQUE, "
                "processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            # Новая таблица для ожидающих обработки розыгрышей
            await db.execute(
                "CREATE TABLE IF NOT EXISTS pending_giveaways ("
                "session_name TEXT NOT NULL, "
                "giveaway_id TEXT NOT NULL, "
                "giveaway_data TEXT NOT NULL, "
                "added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                "PRIMARY KEY (session_name, giveaway_id))"
            )
            await db.commit()
            # Новая таблица для каналов в статусе timeout
            await db.execute(
                "CREATE TABLE IF NOT EXISTS channel_timeouts ("
                "session_name TEXT NOT NULL, "
                "channel_name TEXT NOT NULL, "
                "giveaway_id TEXT NOT NULL, "
                "timeout_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                "giveaway_end_at TIMESTAMP NOT NULL, "
                "PRIMARY KEY (session_name, channel_name, giveaway_id))"
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
                "INSERT OR REPLACE INTO subscribed_channels (session_name, channel_name, last_activity_at, giveaway_participation_at) VALUES (?, ?, CURRENT_TIMESTAMP, NULL)",
                (session_name, channel_name)
            )
            await db.commit()

    async def update_channel_activity(self, session_name: str, channel_name: str) -> None:
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                "UPDATE subscribed_channels SET last_activity_at = CURRENT_TIMESTAMP WHERE session_name = ? AND channel_name = ?",
                (session_name, channel_name)
            )
            await db.commit()

    async def update_giveaway_participation_timestamp(
        self, session_name: str, channel_name: str
    ) -> None:
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                "UPDATE subscribed_channels SET giveaway_participation_at = CURRENT_TIMESTAMP WHERE session_name = ? AND channel_name = ?",
                (session_name, channel_name)
            )
            await db.commit()

    async def get_channels_to_leave(self, session_name: str, inactivity_hours: int) -> List[Tuple[int, str]]:
        threshold_time = datetime.datetime.now() - datetime.timedelta(hours=inactivity_hours)
        async with aiosqlite.connect(self._db_path) as db:
            cursor = await db.execute(
                "SELECT id, channel_name FROM subscribed_channels WHERE session_name = ? AND last_activity_at < ? AND giveaway_participation_at IS NOT NULL",
                (session_name, threshold_time.strftime('%Y-%m-%d %H:%M:%S'))
            )
            channels_to_leave = await cursor.fetchall()
            await cursor.close()
            return channels_to_leave

    async def remove_channel(self, channel_id: int) -> None:
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                "DELETE FROM subscribed_channels WHERE id = ?",
                (channel_id,)
            )
            await db.commit()

    async def add_processed_giveaway(self, giveaway_id: str) -> None:
        async with aiosqlite.connect(self._db_path) as db:
            try:
                await db.execute(
                    'INSERT INTO processed_giveaways (giveaway_id) VALUES (?)',
                    (giveaway_id,)
                )
                await db.commit()
            except aiosqlite.IntegrityError:
                pass

    async def is_giveaway_processed(self, giveaway_id: str) -> bool:
        async with aiosqlite.connect(self._db_path) as db:
            cursor = await db.execute(
                'SELECT 1 FROM processed_giveaways WHERE giveaway_id = ?',
                (giveaway_id,)
            )
            row = await cursor.fetchone()
            await cursor.close()
            return row is not None

    async def clear_old_processed_giveaways(self, days_to_keep: int) -> None:
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                '''DELETE FROM processed_giveaways WHERE processed_at < date('now', ?)''',
                (f'-{days_to_keep} days',)
            )
            await db.commit()

    async def add_pending_giveaway(self, session_name: str, giveaway_id: str, giveaway_data: dict) -> None:
        async with aiosqlite.connect(self._db_path) as db:
            try:
                import json
                await db.execute(
                    "INSERT INTO pending_giveaways (session_name, giveaway_id, giveaway_data) VALUES (?, ?, ?)",
                    (session_name, giveaway_id, json.dumps(giveaway_data))
                )
                await db.commit()
            except aiosqlite.IntegrityError:
                pass

    async def is_giveaway_pending(self, session_name: str, giveaway_id: str) -> bool:
        async with aiosqlite.connect(self._db_path) as db:
            cursor = await db.execute(
                "SELECT 1 FROM pending_giveaways WHERE session_name = ? AND giveaway_id = ?",
                (session_name, giveaway_id)
            )
            result = await cursor.fetchone()
            await cursor.close()
            return result is not None

    async def get_pending_giveaways(self, session_name: str) -> List[dict]:
        async with aiosqlite.connect(self._db_path) as db:
            cursor = await db.execute(
                "SELECT giveaway_data FROM pending_giveaways WHERE session_name = ? ORDER BY added_at ASC",
                (session_name,)
            )
            rows = await cursor.fetchall()
            await cursor.close()
            import json
            return [json.loads(row[0]) for row in rows]

    async def remove_pending_giveaway(self, session_name: str, giveaway_id: str) -> None:
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                "DELETE FROM pending_giveaways WHERE session_name = ? AND giveaway_id = ?",
                (session_name, giveaway_id)
            )
            await db.commit()

    async def clear_unparticipated_channels_on_start(
        self, session_name: str
    ) -> None:
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                "DELETE FROM subscribed_channels WHERE session_name = ? AND giveaway_participation_at IS NULL",
                (session_name,)
            )
            await db.commit()

    async def mark_channel_timeout(self, session_name: str, channel_name: str, giveaway_id: str, giveaway_end_at: str) -> None:
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO channel_timeouts (session_name, channel_name, giveaway_id, timeout_at, giveaway_end_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?)",
                (session_name, channel_name, giveaway_id, giveaway_end_at)
            )
            await db.commit()

    async def is_channel_timeout(self, session_name: str, channel_name: str, giveaway_id: str) -> bool:
        async with aiosqlite.connect(self._db_path) as db:
            cursor = await db.execute(
                "SELECT 1 FROM channel_timeouts WHERE session_name = ? AND channel_name = ? AND giveaway_id = ?",
                (session_name, channel_name, giveaway_id)
            )
            result = await cursor.fetchone()
            await cursor.close()
            return result is not None

    async def remove_channel_timeout(self, session_name: str, channel_name: str, giveaway_id: str) -> None:
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                "DELETE FROM channel_timeouts WHERE session_name = ? AND channel_name = ? AND giveaway_id = ?",
                (session_name, channel_name, giveaway_id)
            )
            await db.commit()

    async def clear_expired_timeouts(self) -> None:
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                "DELETE FROM channel_timeouts WHERE giveaway_end_at < CURRENT_TIMESTAMP"
            )
            await db.commit()

    async def close(self) -> None:
        pass 