import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory

from wakeline import Coordinator, Ctx, FakeClock, every
from wakeline.stores import SqliteStore


async def main() -> None:
    clock = FakeClock()
    calls: list[str] = []

    async def check(ctx: Ctx[None]) -> None:
        calls.append(ctx.key)

    with TemporaryDirectory(prefix="wakeline-example-") as directory:
        database = Path(directory) / "wake.db"
        first = Coordinator(clock=clock, store=SqliteStore(database))
        first.kind("check")(check)
        await first.ensure(kind="check", key="check/one", trigger=every(minutes=10))
        await clock.advance("10m")
        await first.stop()
        await clock.advance("30m")
        second = Coordinator(clock=clock, store=SqliteStore(database))
        second.kind("check")(check)
        await second.run_pending()
        assert calls == ["check/one", "check/one"]
        assert len(second.ledger.history("check/one")) == 2
        print("SQLite restart: one catchup, two retained runs")
        await second.stop()


if __name__ == "__main__":
    asyncio.run(main())
