import asyncio
import sqlite3
import threading
from collections.abc import Callable
from pathlib import Path
from uuid import uuid4

from .base import Stored


class SqliteStore:
    """SQLite persistence and atomic leases, with all disk I/O off the event loop.

    Each coordinator should own its own store connection. Multiple connections
    to the same local file coordinate through SQLite transactions and CAS.
    """

    def __init__(self, path: str | Path) -> None:
        self._path = str(path)
        self._connection: sqlite3.Connection | None = None
        self._lock = threading.Lock()

    def _connect(self) -> sqlite3.Connection:
        if self._connection is None:
            connection = sqlite3.connect(self._path, check_same_thread=False, timeout=10)
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA busy_timeout=10000")
            connection.execute(
                "CREATE TABLE IF NOT EXISTS wakeline_records "
                "(key TEXT PRIMARY KEY, version TEXT NOT NULL, value TEXT NOT NULL)"
            )
            connection.commit()
            self._connection = connection
        return self._connection

    def _locked[T](self, operation: Callable[[sqlite3.Connection], T]) -> T:
        with self._lock:
            connection = self._connect()
            with connection:
                return operation(connection)

    async def open(self) -> None:
        await asyncio.to_thread(self._locked, lambda connection: None)

    async def close(self) -> None:
        def close() -> None:
            with self._lock:
                if self._connection is not None:
                    self._connection.close()
                    self._connection = None

        await asyncio.to_thread(close)

    async def get(self, key: str) -> Stored | None:
        def get(connection: sqlite3.Connection) -> Stored | None:
            row = connection.execute(
                "SELECT key, version, value FROM wakeline_records WHERE key = ?", (key,)
            ).fetchone()
            return None if row is None else Stored(*row)

        return await asyncio.to_thread(lambda: self._locked(get))

    async def list(self) -> list[Stored]:
        def read(connection: sqlite3.Connection) -> list[Stored]:
            return [
                Stored(*row)
                for row in connection.execute(
                    "SELECT key, version, value FROM wakeline_records"
                ).fetchall()
            ]

        return await asyncio.to_thread(lambda: self._locked(read))

    async def put(self, key: str, value: str, *, expected: str | None) -> Stored | None:
        record = Stored(key, uuid4().hex, value)

        def put(connection: sqlite3.Connection) -> Stored | None:
            if expected is None:
                cursor = connection.execute(
                    "INSERT OR IGNORE INTO wakeline_records(key, version, value) VALUES (?, ?, ?)",
                    (key, record.version, value),
                )
            else:
                cursor = connection.execute(
                    "UPDATE wakeline_records SET version = ?, value = ? "
                    "WHERE key = ? AND version = ?",
                    (record.version, value, key, expected),
                )
            return record if cursor.rowcount == 1 else None

        return await asyncio.to_thread(lambda: self._locked(put))

    async def delete(self, key: str, *, expected: str) -> bool:
        def delete(connection: sqlite3.Connection) -> bool:
            return (
                connection.execute(
                    "DELETE FROM wakeline_records WHERE key = ? AND version = ?", (key, expected)
                ).rowcount
                == 1
            )

        return await asyncio.to_thread(lambda: self._locked(delete))
