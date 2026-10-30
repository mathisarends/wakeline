import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from wakeline import (
    Catchup,
    Coordinator,
    FakeClock,
    Outcome,
    Pause,
    State,
    cron,
    every,
    once,
)


async def test_cron_catchup_uses_local_timezone_and_window() -> None:
    clock = FakeClock(datetime(2026, 10, 30, 0, tzinfo=UTC))
    coord = Coordinator(clock=clock)
    calls = []

    @coord.kind("job", catchup=Catchup.window(hours=1))
    async def job(ctx):
        calls.append(ctx)

    await coord.ensure(kind="job", key="j", trigger=cron("0 3 * * *", tz="Europe/Berlin"))
    await clock.advance("2h 30m")
    assert len(calls) == 1
    assert calls[0].scheduled_for == datetime(2026, 10, 30, 2, tzinfo=UTC)
    await clock.advance("25h")
    assert len(calls) == 1
    await coord.stop()


async def test_cron_none_runs_exact_current_slot_after_sleep() -> None:
    clock = FakeClock()
    coord = Coordinator(clock=clock)
    calls = []

    @coord.kind("job", catchup=Catchup.NONE)
    async def job(ctx):
        calls.append(ctx)

    await coord.ensure(kind="job", key="j", trigger=cron("* * * * *"))
    await clock.advance("5m")
    assert len(calls) == 1
    assert calls[0].scheduled_for == clock.now()
    await coord.stop()


async def test_cron_catchup_across_dst_gap() -> None:
    clock = FakeClock(datetime(2026, 3, 27, 0, tzinfo=UTC))
    coord = Coordinator(clock=clock)
    calls = []

    @coord.kind("job", catchup=Catchup.window(hours=1))
    async def job(ctx):
        calls.append(ctx)

    await coord.ensure(kind="job", key="j", trigger=cron("30 2 * * *", tz="Europe/Berlin"))
    await clock.advance("2d 1h 15m")
    assert len(calls) == 1
    assert calls[0].scheduled_for == datetime(2026, 3, 29, 1, tzinfo=UTC)
    await coord.stop()


async def test_absolute_once_tracks_wall_clock_and_relative_once_does_not() -> None:
    clock = FakeClock()
    coord = Coordinator(clock=clock)
    calls = []

    @coord.kind("job")
    async def job(ctx):
        calls.append(ctx.key)

    await coord.ensure(
        kind="job", key="absolute", trigger=once(at=clock.now() + timedelta(hours=1))
    )
    await coord.ensure(kind="job", key="relative", trigger=once(after="1h"))
    await clock.jump_wall(timedelta(hours=2))
    assert calls == ["absolute"]
    await clock.advance("1h")
    assert calls == ["absolute", "relative"]
    await coord.stop()


async def test_quiet_hours_apply_to_poke_and_do_not_consume_max_defer() -> None:
    clock = FakeClock(datetime(2026, 10, 30, 22, 30, tzinfo=UTC))
    coord = Coordinator(clock=clock)
    calls = []

    @coord.kind("job")
    async def job(ctx):
        calls.append(ctx)

    await coord.ensure(
        kind="job",
        key="j",
        trigger=every(hours=1, quiet=("23:00", "07:00"), tz="Europe/Berlin"),
        max_defer=0,
    )
    await coord.poke("j")
    await coord.run_pending()
    assert calls == []
    assert "Quiet hours" in (await coord.status())[0].reason
    await clock.advance("7h 30m")
    assert len(calls) == 1
    await coord.stop()


async def test_jitter_is_stable_across_idempotent_ensure_and_restart() -> None:
    from wakeline.stores import MemoryStore

    clock = FakeClock()
    store = MemoryStore()
    first = Coordinator(clock=clock, store=store)

    async def job(ctx):
        pass

    first.kind("job")(job)
    expected = (
        await first.ensure(kind="job", key="j", trigger=every(minutes=1, jitter="10s"))
    ).next_fire
    assert clock.now() + timedelta(minutes=1) <= expected < clock.now() + timedelta(seconds=70)
    await first.stop()
    second = Coordinator(clock=clock, store=store)
    second.kind("job")(job)
    assert (await second.status())[0].next_fire == expected
    await second.stop()


async def test_hooks_can_read_status_and_failure_is_isolated() -> None:
    clock = FakeClock()
    events = []

    async def on_run(run):
        events.append((run.outcome, await coord.count()))
        raise ValueError("broken metrics exporter")

    async def on_defer(status):
        events.append((status.state, await coord.count()))

    coord = Coordinator(clock=clock, on_run=on_run, on_defer=on_defer)

    @coord.kind("job")
    async def job(ctx):
        pass

    await coord.ensure(kind="job", key="j", trigger=once(after="1s"), yield_to=["busy"])
    async with coord.busy("busy", reason="testing"):
        await clock.advance("1s")
    await coord.run_pending()
    assert events == [(State.DEFERRED, 1), (Outcome.OK, 0)]
    await coord.stop()


async def test_pause_return_from_once_remains_visible() -> None:
    clock = FakeClock()
    coord = Coordinator(clock=clock)
    paused = True
    calls = []

    @coord.kind("job")
    async def job(ctx):
        calls.append(ctx)
        return Pause("manual decision needed") if paused else None

    await coord.ensure(kind="job", key="j", trigger=once(after="1s"))
    await clock.advance("1s")
    assert (await coord.status())[0].state == State.PAUSED
    paused = False
    await coord.resume("j")
    await coord.run_pending()
    assert len(calls) == 2
    assert await coord.count() == 0
    await coord.stop()


async def test_background_loop_lifecycle_and_wakeup_from_ensure() -> None:
    completed = asyncio.Event()
    coord = Coordinator()

    @coord.kind("job")
    async def job(ctx):
        completed.set()

    async with coord:
        await coord.ensure(kind="job", key="j", trigger=once(after=0))
        async with asyncio.timeout(2):
            await completed.wait()
        await coord.run_pending()
        assert coord.ledger.last("j").outcome == Outcome.OK


async def test_many_instances_create_no_idle_job_tasks() -> None:
    clock = FakeClock()
    coord = Coordinator(clock=clock)

    @coord.kind("job")
    async def job(ctx):
        pass

    before = set(asyncio.all_tasks())
    for i in range(1000):
        await coord.ensure(kind="job", key=str(i), trigger=every(hours=1))
    assert asyncio.all_tasks() == before
    assert await coord.count() == 1000
    await coord.stop()


@pytest.mark.parametrize("bad", [0, -1, "-2s", float("inf")])
def test_invalid_lease_duration_is_rejected(bad) -> None:
    with pytest.raises(ValueError):
        Coordinator(lease_ttl=bad)
