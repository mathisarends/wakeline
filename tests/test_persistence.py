import asyncio
from datetime import timedelta
from pathlib import Path

from wakeline import Coordinator, FakeClock, Outcome, Rearm, Sel, State, every, once
from wakeline.core.codec import decode, encode
from wakeline.core.state import Lease
from wakeline.stores import MemoryStore, SqliteStore


async def test_sqlite_restart_catches_up_and_preserves_ledger(tmp_path: Path) -> None:
    clock = FakeClock()
    path = tmp_path / "wake.db"
    first = Coordinator(clock=clock, store=SqliteStore(path))

    @first.kind("job")
    async def job(ctx):
        pass

    await first.ensure(kind="job", key="j", trigger=every(minutes=1))
    await clock.advance("1m")
    last_success = (await first.status())[0].last_success
    await first.stop()
    await clock.advance("10m")
    second = Coordinator(clock=clock, store=SqliteStore(path))
    calls = []

    @second.kind("job")
    async def restored(ctx):
        calls.append(ctx)

    assert (await second.status())[0].last_success == last_success
    await second.run_pending()
    assert len(calls) == 1
    assert len(second.ledger.history("j")) == 2
    assert calls[0].scheduled_for == clock.now() - timedelta(minutes=9)
    await second.stop()


async def test_unregistered_persisted_kind_is_orphaned_and_can_be_recovered() -> None:
    clock = FakeClock()
    store = MemoryStore()
    first = Coordinator(clock=clock, store=store)

    @first.kind("job")
    async def job(ctx):
        pass

    await first.ensure(kind="job", key="j", trigger=once(after="1m"))
    await first.stop()
    second = Coordinator(clock=clock, store=store)
    assert (await second.status())[0].state == State.ORPHANED
    calls = []

    @second.kind("job")
    async def restored(ctx):
        calls.append(ctx)

    await clock.advance("1m")
    assert len(calls) == 1
    await second.stop()


async def test_two_coordinators_claim_same_slot_once() -> None:
    clock = FakeClock()
    store = MemoryStore()
    left = Coordinator(clock=clock, store=store, instance_id="left")
    right = Coordinator(clock=clock, store=store, instance_id="right")
    calls = []

    async def job(ctx):
        calls.append(ctx)

    left.kind("job")(job)
    right.kind("job")(job)
    await left.ensure(kind="job", key="j", trigger=every(minutes=1))
    await clock.advance("1m")
    assert len(calls) == 1
    await asyncio.gather(left.stop(), right.stop())


async def test_lease_renewal_and_loss_cancel_handler() -> None:
    clock = FakeClock()
    store = MemoryStore()
    coord = Coordinator(clock=clock, store=store, lease_ttl="9s")
    cancelled = []

    @coord.kind("job")
    async def job(ctx):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.append(ctx.run_id)
            raise

    await coord.ensure(kind="job", key="j", trigger=every(minutes=1))
    await clock.advance("1m")
    for _ in range(5):
        await clock.advance("3s")
    assert cancelled == []
    stored = await store.get("j")
    cell = decode(stored.value)
    cell.lease = Lease("replacement-owner", clock.now() + timedelta(minutes=1))
    assert await store.put("j", encode(cell), expected=stored.version) is not None
    await clock.advance("3s")
    assert len(cancelled) == 1
    assert coord.ledger.last("j").outcome == Outcome.LEASE_LOST
    await coord.stop()


async def test_removed_or_updated_instance_is_not_overwritten_by_stale_handler() -> None:
    clock = FakeClock()
    coord = Coordinator(clock=clock)
    release = asyncio.Event()

    @coord.kind("job")
    async def job(ctx):
        await release.wait()
        return Rearm(after="1s")

    await coord.ensure(kind="job", key="j", trigger=every(minutes=1))
    await clock.advance("1m")
    await coord.update("j", trigger=every(minutes=10))
    release.set()
    await coord.run_pending()
    assert (await coord.status())[0].next_fire == clock.now() + timedelta(minutes=10)
    await coord.remove("j")
    await coord.run_pending()
    assert await coord.count() == 0
    await coord.stop()


async def test_retention_keeps_latest_runs_and_prunes_removed_instances() -> None:
    clock = FakeClock()
    store = MemoryStore()
    coord = Coordinator(clock=clock, store=store, ledger_keep=2, ledger_max_age="1m")

    @coord.kind("job")
    async def job(ctx):
        pass

    await coord.ensure(kind="job", key="j", trigger=every(seconds=1))
    for _ in range(5):
        await clock.advance("1s")
    assert len(coord.ledger.history("j")) == 2
    await coord.remove(Sel())
    await clock.advance("2m")
    assert coord.ledger.history("j") == ()
    assert await store.get("j") is None
    await coord.stop()


async def test_expired_claim_replays_abandoned_one_shot() -> None:
    clock = FakeClock()
    store = MemoryStore()
    left = Coordinator(clock=clock, store=store, lease_ttl="3s")
    calls = []

    @left.kind("job")
    async def abandoned(ctx):
        await asyncio.Event().wait()

    await left.ensure(kind="job", key="j", trigger=once(after="1s"))
    await clock.advance("1s")
    clock.detach(left._drain)  # Simulate a dead process: no further lease renewals.
    right = Coordinator(clock=clock, store=store, lease_ttl="3s")

    @right.kind("job")
    async def takeover(ctx):
        calls.append(ctx)

    await clock.advance("4s")
    assert len(calls) == 1
    outcomes = [run.outcome for run in right.ledger.history("j")]
    assert outcomes == [Outcome.LEASE_LOST, Outcome.OK]
    await left.stop()
    await right.stop()


async def test_graceful_stop_does_not_lose_in_flight_once() -> None:
    clock = FakeClock()
    store = MemoryStore()
    first = Coordinator(clock=clock, store=store)

    @first.kind("job")
    async def blocked(ctx):
        await asyncio.Event().wait()

    await first.ensure(kind="job", key="j", trigger=once(after="1s"))
    await clock.advance("1s")
    await first.stop()
    second = Coordinator(clock=clock, store=store)
    calls = []

    @second.kind("job")
    async def restored(ctx):
        calls.append(ctx)

    await second.run_pending()
    assert len(calls) == 1
    await second.stop()
