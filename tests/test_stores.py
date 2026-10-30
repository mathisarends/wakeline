import asyncio
import os
from collections.abc import AsyncGenerator
from pathlib import Path
from uuid import uuid4

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
    yield result
    await result.close()


async def test_store_cas_and_aba_protection(store: Store) -> None:
    key = f"test-store/{uuid4().hex}"
    assert await store.get(key) is None
    first = await store.put(key, '{"n": 1}', expected=None)
    assert first is not None
    assert await store.put(key, "{}", expected=None) is None
    assert await store.put(key, "{}", expected="stale") is None
    updated = await store.put(key, '{"n": 2}', expected=first.version)
    assert updated is not None and updated.version != first.version
    assert not await store.delete(key, expected=first.version)
    assert await store.delete(key, expected=updated.version)
    recreated = await store.put(key, "{}", expected=None)
    assert recreated is not None and recreated.version != updated.version
    assert await store.put(key, "{}", expected=updated.version) is None
    assert await store.delete(key, expected=recreated.version)


async def test_only_one_concurrent_writer_wins(store: Store) -> None:
    key = f"test-store/{uuid4().hex}"
    first = await store.put(key, "{}", expected=None)
    assert first is not None
    writes = await asyncio.gather(
        *(store.put(key, str(i), expected=first.version) for i in range(20))
    )
    assert sum(write is not None for write in writes) == 1
    winner = next(write for write in writes if write is not None)
    assert await store.delete(key, expected=winner.version)


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
