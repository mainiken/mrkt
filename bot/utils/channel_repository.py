import aiosqlite
from typing import Optional

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
            await db.commit()

    async def is_subscribed(self, session_name: str, channel_name: str) -> bool:
        async with aiosqlite.connect(self._db_path) as db:
            cursor = await db.execute(
                "SELECT 1 FROM subscribed_channels WHERE session_name = ? AND channel_name = ?",
                (session_name, channel_name)
            )
            result = await cursor.fetchone()
            return result is not None

    async def add_channel(self, session_name: str, channel_name: str) -> None:
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                "INSERT OR IGNORE INTO subscribed_channels (session_name, channel_name) VALUES (?, ?)",
                (session_name, channel_name)
            )
            await db.commit() 