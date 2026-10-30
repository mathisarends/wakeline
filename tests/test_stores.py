import asyncio
import os
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest

from wakeline.stores import MemoryStore, PostgresStore, SqliteStore, Store


@pytest.fixture(params=["memory", "sqlite", "postgres"])
async def store(request: pytest.FixtureRequest, tmp_path: Path) -> AsyncGenerator[Store]:
    match request.param:
        case "sqlite":
            result = SqliteStore(tmp_path / "wake.db")
        case "postgres":
            dsn = os.environ.get("WAKELINE_POSTGRES_DSN")
            if not dsn:
                pytest.skip("WAKELINE_POSTGRES_DSN is not set")
            result = PostgresStore(dsn)
        case _:
            result = MemoryStore()
    await result.open()
    for row in await result.list():
        await result.delete(row.key, expected=row.version)
    yield result
    await result.close()


async def test_store_cas_and_aba_protection(store: Store) -> None:
    assert await store.get("job") is None
    first = await store.put("job", '{"n": 1}', expected=None)
    assert first is not None
    assert await store.put("job", "{}", expected=None) is None
    assert await store.put("job", "{}", expected="stale") is None
    updated = await store.put("job", '{"n": 2}', expected=first.version)
    assert updated is not None and updated.version != first.version
    assert not await store.delete("job", expected=first.version)
    assert await store.delete("job", expected=updated.version)
    recreated = await store.put("job", "{}", expected=None)
    assert recreated is not None and recreated.version != updated.version
    assert await store.put("job", "{}", expected=updated.version) is None


async def test_only_one_concurrent_writer_wins(store: Store) -> None:
    first = await store.put("job", "{}", expected=None)
    assert first is not None
    writes = await asyncio.gather(
        *(store.put("job", str(i), expected=first.version) for i in range(20))
    )
    assert sum(write is not None for write in writes) == 1


async def test_sqlite_connections_coordinate_and_reopen(tmp_path: Path) -> None:
    path = tmp_path / "wake.db"
    left, right = SqliteStore(path), SqliteStore(path)
    await left.open()
    await right.open()
    writes = await asyncio.gather(
        left.put("job", "{}", expected=None), right.put("job", "{}", expected=None)
    )
    assert sum(write is not None for write in writes) == 1
    assert await left.list() == await right.list()
    await left.close()
    await right.close()
    await left.open()
    assert len(await left.list()) == 1
    await left.close()
