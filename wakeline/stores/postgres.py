from uuid import uuid4

from .base import Stored


class PostgresStore:
    """Optional asyncpg-backed CAS store. Importing wakeline never imports asyncpg."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        # The optional driver's untyped pool is contained within this adapter.
        self._pool = None

    async def open(self) -> None:
        if self._pool is not None:
            return
        try:
            import asyncpg
        except ImportError as error:
            raise ImportError("Install wakeline[postgres] to use PostgresStore") from error
        pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=5)
        try:
            await pool.execute(
                "CREATE TABLE IF NOT EXISTS wakeline_records "
                "(key TEXT PRIMARY KEY, version TEXT NOT NULL, value TEXT NOT NULL)"
            )
        except BaseException:
            await pool.close()
            raise
        self._pool = pool

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def get(self, key: str) -> Stored | None:
        await self.open()
        assert self._pool is not None
        row = await self._pool.fetchrow(
            "SELECT key, version, value FROM wakeline_records WHERE key = $1", key
        )
        return None if row is None else Stored(row["key"], row["version"], row["value"])

    async def list(self) -> list[Stored]:
        await self.open()
        assert self._pool is not None
        rows = await self._pool.fetch("SELECT key, version, value FROM wakeline_records")
        return [Stored(row["key"], row["version"], row["value"]) for row in rows]

    async def put(self, key: str, value: str, *, expected: str | None) -> Stored | None:
        await self.open()
        assert self._pool is not None
        record = Stored(key, uuid4().hex, value)
        if expected is None:
            result = await self._pool.fetchval(
                "INSERT INTO wakeline_records(key, version, value) VALUES ($1, $2, $3) "
                "ON CONFLICT DO NOTHING RETURNING version",
                key,
                record.version,
                value,
            )
        else:
            result = await self._pool.fetchval(
                "UPDATE wakeline_records SET version=$2, value=$3 "
                "WHERE key=$1 AND version=$4 RETURNING version",
                key,
                record.version,
                value,
                expected,
            )
        return record if result is not None else None

    async def delete(self, key: str, *, expected: str) -> bool:
        await self.open()
        assert self._pool is not None
        result = await self._pool.execute(
            "DELETE FROM wakeline_records WHERE key=$1 AND version=$2", key, expected
        )
        return result == "DELETE 1"
