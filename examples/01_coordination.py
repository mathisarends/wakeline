import asyncio

from wakeline import Coordinator, Ctx, FakeClock, Sel, State, every


async def main() -> None:
    clock = FakeClock()
    coord = Coordinator(clock=clock)
    completed: list[str] = []
    coord.limit("llm", max_concurrent=4)

    @coord.kind("heartbeat", limits=["llm"])
    async def heartbeat(ctx: Ctx[None]) -> None:
        completed.append(ctx.key)

    await coord.ensure(
        kind="heartbeat",
        key="heartbeat/alice/s9",
        trigger=every(minutes=30),
        labels={"agent": "alice", "session": "s9"},
        lane="session/{session}",
        yield_to=["session/{session}"],
        max_defer="10m",
    )
    async with coord.busy("session/s9", reason="user_turn"):
        await clock.advance("30m")
        status = (await coord.status(Sel(session="s9")))[0]
        assert status.state == State.DEFERRED
        assert "user_turn" in (status.reason or "")
        assert completed == []
        print(status.key, status.state, status.reason)
    await coord.run_pending()
    assert completed == ["heartbeat/alice/s9"]
    run = coord.ledger.last(completed[0])
    assert run is not None and run.outcome == "ok"
    print(run.key, run.outcome)
    await coord.stop()


if __name__ == "__main__":
    asyncio.run(main())
