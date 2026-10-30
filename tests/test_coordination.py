import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest

from wakeline import (
    Alive,
    Catchup,
    Cause,
    Coordinator,
    Ctx,
    FakeClock,
    Outcome,
    Overlap,
    Pause,
    Poke,
    Rearm,
    Remove,
    Sel,
    Spec,
    State,
    every,
    once,
)


async def test_ensure_is_idempotent_and_update_preserves_health() -> None:
    clock = FakeClock()
    coord = Coordinator(clock=clock)
    calls = []

    @coord.kind("beat")
    async def beat(ctx):
        calls.append(ctx)

    spec = Spec("beat", "beat/a", every(minutes=1), labels={"agent": "a"})
    first = await coord.ensure(spec)
    await clock.advance("30s")
    assert (await coord.ensure(spec)).next_fire == first.next_fire
    await clock.advance("30s")
    assert len(calls) == 1
    assert coord.ledger.last("beat/a").outcome == Outcome.OK
    assert await coord.update(Sel(agent="a"), trigger=every(minutes=2)) == 1
    updated = (await coord.status("beat/a"))[0]
    assert updated.last_success == clock.now()
    assert updated.next_fire == clock.now() + timedelta(minutes=2)
    await coord.stop()


async def test_counted_lane_holders_and_release() -> None:
    clock = FakeClock()
    coord = Coordinator(clock=clock)
    calls = []

    @coord.kind("beat")
    async def beat(ctx):
        calls.append(ctx)

    await coord.ensure(
        kind="beat",
        key="beat/s",
        trigger=every(minutes=1),
        labels={"session": "s"},
        lane="session/{session}",
        yield_to=["session/{session}"],
    )
    async with coord.busy("session/s", reason="user_turn"):
        async with coord.busy("session/s", reason="cron"):
            await clock.advance("1m")
            status = (await coord.status())[0]
            assert status.state == State.DEFERRED
            assert {h.reason for h in status.holders["session/s"]} == {"user_turn", "cron"}
        await coord.run_pending()
        assert calls == []
        assert "user_turn" in (await coord.status())[0].reason
    await coord.run_pending()
    assert len(calls) == 1
    await coord.stop()


async def test_max_defer_overrides_yield_only() -> None:
    clock = FakeClock()
    coord = Coordinator(clock=clock)
    calls = []

    @coord.kind("job")
    async def job(ctx):
        calls.append(ctx)

    await coord.ensure(
        kind="job", key="j", trigger=once(after="1m"), yield_to=["user"], max_defer="2m"
    )
    async with coord.busy("user", reason="long turn"):
        await clock.advance("1m")
        assert calls == []
        await clock.advance("2m")
        assert len(calls) == 1
    await coord.stop()


async def test_requires_blocks_until_success_and_expires() -> None:
    clock = FakeClock()
    coord = Coordinator(clock=clock)
    calls = []

    @coord.kind("beat")
    async def beat(ctx):
        pass

    @coord.kind("dream")
    async def dream(ctx):
        calls.append(ctx)

    await coord.ensure(kind="beat", key="beat/a/s", trigger=every(minutes=5))
    await coord.ensure(
        kind="dream",
        key="dream/a",
        trigger=every(minutes=1),
        labels={"agent": "a"},
        requires=[Alive("beat/{agent}/*", within="2m")],
    )
    await clock.advance("1m")
    assert (await coord.status("dream/a"))[0].state == State.BLOCKED
    await clock.advance("4m")
    assert len(calls) == 1
    await clock.advance("3m")
    assert (await coord.status("dream/a"))[0].state == State.BLOCKED
    await coord.stop()


@pytest.mark.parametrize(
    "policy,expected", [(Catchup.ONCE, 1), (Catchup.ALL, 5), (Catchup.NONE, 0)]
)
async def test_catchup_after_sleep(policy, expected) -> None:
    clock = FakeClock()
    coord = Coordinator(clock=clock)
    calls = []

    @coord.kind("job", catchup=policy, overlap=Overlap.BUFFER_ALL)
    async def job(ctx):
        calls.append(ctx)

    await coord.ensure(kind="job", key="j", trigger=every(minutes=1))
    await clock.advance("5m 10s")
    assert len(calls) == expected
    assert (await coord.status())[0].next_fire == datetime(2026, 1, 1, 0, 6, tzinfo=UTC)
    await coord.stop()


async def test_catchup_none_fires_on_time() -> None:
    clock = FakeClock()
    coord = Coordinator(clock=clock)

    @coord.kind("job", catchup=Catchup.NONE)
    async def job(ctx):
        pass

    await coord.ensure(kind="job", key="j", trigger=once(after="1s"))
    await clock.advance("1s")
    assert coord.ledger.last("j").outcome == Outcome.OK
    await coord.stop()


async def test_poke_consumes_upcoming_slot_and_coalesces() -> None:
    clock = FakeClock()
    coord = Coordinator(clock=clock)
    calls = []

    @coord.kind("job")
    async def job(ctx):
        calls.append(ctx)

    await coord.ensure(kind="job", key="j", trigger=every(minutes=10))
    await clock.advance("1m")
    coord.poke("j", reason="message")
    coord.poke("j", reason="another")
    await coord.run_pending()
    assert len(calls) == 1 and calls[0].cause == Cause.POKE
    await clock.advance("9m")
    assert len(calls) == 1
    await clock.advance("10m")
    assert len(calls) == 2
    await coord.stop()


@pytest.mark.parametrize("policy,expected", [(Poke.BUFFER_ONE, 2), (Poke.COALESCE, 1)])
async def test_poke_during_handler_is_buffered_once(policy, expected) -> None:
    clock = FakeClock()
    coord = Coordinator(clock=clock, lease_ttl="1h")
    release = asyncio.Event()
    calls = []

    @coord.kind("job")
    async def job(ctx):
        calls.append(ctx)
        await release.wait()

    await coord.ensure(kind="job", key="j", trigger=every(minutes=1), poke=policy)
    await clock.advance("1m")
    assert len(calls) == 1
    await coord.poke("j")
    await coord.poke("j")
    await coord.run_pending()
    release.set()
    await coord.run_pending()
    assert len(calls) == expected
    await coord.stop()


@pytest.mark.parametrize(
    "policy,expected",
    [
        (Overlap.SKIP, 1),
        (Overlap.BUFFER_ONE, 2),
        (Overlap.BUFFER_ALL, 4),
        (Overlap.ALLOW_ALL, 4),
    ],
)
async def test_overlap_policies(policy, expected) -> None:
    clock = FakeClock()
    coord = Coordinator(clock=clock, lease_ttl="1h")
    release = asyncio.Event()
    calls = []

    @coord.kind("job", overlap=policy, catchup=Catchup.ALL)
    async def job(ctx):
        calls.append(ctx)
        await release.wait()

    await coord.ensure(kind="job", key="j", trigger=every(minutes=1))
    await clock.advance("1m")
    await clock.advance("3m")
    release.set()
    await coord.run_pending()
    assert len(calls) == expected
    await coord.stop()


async def test_cancel_other_waits_for_cooperative_cancellation() -> None:
    clock = FakeClock()
    coord = Coordinator(clock=clock, lease_ttl="1h")
    calls = []
    cancelled = []

    @coord.kind("job", overlap=Overlap.CANCEL_OTHER)
    async def job(ctx):
        calls.append(ctx)
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.append(ctx.run_id)
            raise

    await coord.ensure(kind="job", key="j", trigger=every(minutes=1))
    await clock.advance("1m")
    await clock.advance("1m")
    assert len(calls) == 2 and len(cancelled) == 1
    assert coord.ledger.history("j")[0].outcome == Outcome.CANCELLED
    await coord.stop()


async def test_limit_is_shared_between_instances() -> None:
    clock = FakeClock()
    coord = Coordinator(clock=clock)
    coord.limit("llm", max_concurrent=1)
    release = asyncio.Event()
    calls = []

    @coord.kind("job", limits=["llm"])
    async def job(ctx):
        calls.append(ctx)
        await release.wait()

    for key in ("a", "b"):
        await coord.ensure(kind="job", key=key, trigger=once(after="1s"))
    await clock.advance("1s")
    assert len(calls) == 1
    assert any(s.state == State.DEFERRED and "llm" in s.reason for s in await coord.status())
    release.set()
    await coord.run_pending()
    assert len(calls) == 2
    await coord.stop()


async def test_rearm_is_one_shot_and_earlier_deadline_wins() -> None:
    clock = FakeClock()
    coord = Coordinator(clock=clock)
    calls = []

    @coord.kind("job")
    async def job(ctx):
        calls.append(ctx)
        if len(calls) == 1:
            return Rearm(after="2m")
        if len(calls) == 2:
            return Rearm(after="20m")

    await coord.ensure(kind="job", key="j", trigger=every(minutes=10))
    await clock.advance("10m")
    await clock.advance("2m")
    assert len(calls) == 2
    await clock.advance("8m")
    assert len(calls) == 3
    await clock.advance("12m")
    assert len(calls) == 4  # Regular 30-minute slot only, no leftover rearm at 32.
    await coord.stop()


async def test_handler_controls_pause_failures_and_remove() -> None:
    clock = FakeClock()
    coord = Coordinator(clock=clock)
    mode = "pause"

    @coord.kind("job", pause_on_failure=2)
    async def job(ctx):
        if mode == "pause":
            return Pause("waiting for user")
        if mode == "error":
            raise ValueError("broken")
        return Remove()

    await coord.ensure(kind="job", key="j", trigger=every(minutes=1))
    await clock.advance("1m")
    assert (await coord.status())[0].reason == "waiting for user"
    mode = "error"
    await coord.resume("j")
    await clock.advance("1m")
    await clock.advance("1m")
    assert (await coord.status())[0].state == State.PAUSED
    assert (await coord.status())[0].failures == 2
    mode = "remove"
    await coord.resume("j")
    await clock.advance("1m")
    assert await coord.count() == 0
    await coord.stop()


async def test_reconcile_scope_and_preflight_validation() -> None:
    clock = FakeClock()
    coord = Coordinator(clock=clock)

    @coord.kind("job")
    async def job(ctx):
        pass

    config = {"agent": "a", "source": "config"}
    await coord.ensure(Spec("job", "old", every(minutes=1), labels=config))
    await coord.ensure(Spec("job", "agent-created", every(minutes=1), labels={"agent": "a"}))
    desired = [Spec("job", "new", every(minutes=1), labels=config)]
    result = await coord.reconcile(scope=Sel(**config), desired=desired)
    assert result.created == 1 and result.removed == 1
    assert await coord.count() == 2
    assert (await coord.reconcile(scope=Sel(**config), desired=desired)).unchanged == 1
    with pytest.raises(ValueError):
        await coord.reconcile(
            scope=Sel(**config),
            desired=[Spec("job", "agent-created", every(minutes=1), labels=config)],
        )
    assert await coord.count() == 2
    await coord.stop()


@dataclass
class Params:
    target: str


async def test_dataclass_params_and_monotonic_intervals() -> None:
    clock = FakeClock()
    coord = Coordinator(clock=clock)
    calls = []

    @coord.kind("job")
    async def job(ctx: Ctx[Params]):
        assert isinstance(ctx.params, Params)
        calls.append(ctx.params.target)

    await coord.ensure(kind="job", key="j", trigger=every(minutes=1), params=Params("hello"))
    await clock.jump_wall(timedelta(days=1))
    assert calls == []
    await clock.advance("1m")
    assert calls == ["hello"]
    await coord.stop()


async def test_static_once_is_not_resurrected_and_many_awaits_drain() -> None:
    clock = FakeClock()
    coord = Coordinator(clock=clock)
    calls = []

    @coord.wakeup("static", once(after="1s"))
    async def job(ctx):
        for _ in range(200):
            await asyncio.sleep(0)
        calls.append(ctx.key)

    await coord.run_pending()
    await clock.advance("1s")
    await clock.advance("1s")
    assert calls == ["static"]
    await coord.stop()
