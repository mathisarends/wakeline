import asyncio
import os
from pathlib import Path
from uuid import uuid4

import pytest

from wakeline import Coordinator, FakeClock, Outcome, Rearm, every, once
from wakeline.stores import PostgresStore, SqliteStore, Store


@pytest.fixture(params=["sqlite", pytest.param("postgres", marks=pytest.mark.postgres)])
def store_pair(request: pytest.FixtureRequest, tmp_path: Path) -> tuple[Store, Store]:
    if request.param == "postgres":
        dsn = os.environ.get("WAKELINE_POSTGRES_DSN")
        if not dsn:
            pytest.skip("WAKELINE_POSTGRES_DSN is not set")
        return PostgresStore(dsn), PostgresStore(dsn)
    return SqliteStore(tmp_path / "wake.db"), SqliteStore(tmp_path / "wake.db")


async def test_competing_connections_claim_once(store_pair: tuple[Store, Store]) -> None:
    first, second = store_pair
    clock = FakeClock()
    left = Coordinator(clock=clock, store=first)
    right = Coordinator(clock=clock, store=second)
    calls = []
    release = asyncio.Event()
    key = f"test-cluster/{uuid4().hex}"

    async def job(ctx):
        calls.append(ctx)
        await release.wait()

    left.kind("job")(job)
    right.kind("job")(job)
    try:
        await left.ensure(kind="job", key=key, trigger=once(after="1s"))
        # Deliberately drive each event loop decision concurrently rather than
        # using FakeClock's deterministic sequential coordinator traversal.
        clock.detach(left._drain)
        clock.detach(right._drain)
        await clock.advance("1s")
        await asyncio.gather(left.run_pending(), right.run_pending())
        assert len(calls) == 1
        release.set()
        await asyncio.gather(left.run_pending(), right.run_pending())
        await right.status()
        assert [run.outcome for run in right.ledger.history(key)] == [Outcome.OK]
    finally:
        await asyncio.gather(left.stop(), right.stop())
        row = await second.get(key)
        if row is not None:
            await second.delete(key, expected=row.version)
        await second.close()


async def test_remote_management_fences_old_result(store_pair: tuple[Store, Store]) -> None:
    first, second = store_pair
    clock = FakeClock()
    left = Coordinator(clock=clock, store=first)
    right = Coordinator(clock=clock, store=second)
    release = asyncio.Event()
    key = f"test-cluster/{uuid4().hex}"

    async def job(ctx):
        await release.wait()
        return Rearm(after="1s")

    left.kind("job")(job)
    right.kind("job")(job)
    try:
        await left.ensure(kind="job", key=key, trigger=every(minutes=1))
        await clock.advance("1m")
        await right.update(key, trigger=every(minutes=10))
        expected = (await right.status(key))[0].next_fire
        release.set()
        await left.run_pending()
        assert (await right.status(key))[0].next_fire == expected
    finally:
        await right.remove(key)
        await asyncio.gather(left.stop(), right.stop())
        row = await second.get(key)
        if row is not None:
            await second.delete(key, expected=row.version)
        await second.close()
