# wakeline

[![CI](https://github.com/mathisarends/wakeline/actions/workflows/tests.yml/badge.svg)](https://github.com/mathisarends/wakeline/actions/workflows/tests.yml)

A small, typed asyncio coordination layer for scheduled wakeups. Python 3.12–3.14.

Register behavior once, create instances at runtime, and coordinate them through
explicit lanes, dependencies, concurrency limits, and policies. The core has a
single timing loop; idle instances do not own asyncio tasks.

```sh
pip install wakeline
# Optional PostgreSQL persistence:
pip install 'wakeline[postgres]'
```

```python
import asyncio

from wakeline import Coordinator, Ctx, Rearm, Sel, every
from wakeline.stores import SqliteStore


async def main() -> None:
    coord = Coordinator(store=SqliteStore("wake.db"))
    coord.limit("llm", max_concurrent=4)

    @coord.kind("heartbeat", limits=["llm"])
    async def heartbeat(ctx: Ctx[None]) -> Rearm | None:
        print(ctx.key, ctx.scheduled_for, ctx.fired_at)
        return None

    async with coord:
        await coord.ensure(
            kind="heartbeat",
            key="heartbeat/alice/session-1",
            trigger=every(minutes=30, jitter="1m"),
            labels={"agent": "alice", "session": "session-1"},
            lane="session/{session}",
            yield_to=["session/{session}"],
        )
        async with coord.busy("session/session-1", reason="user_turn"):
            await asyncio.sleep(0)  # Handle the user turn here.
        await coord.poke(Sel(kind="heartbeat", session="session-1"), reason="activity")
        await coord.run_pending()


asyncio.run(main())
```

- Triggers: `every`, `cron` with IANA timezones, and `once`; jitter, quiet hours,
  expiry, and handler-directed `Rearm`, `Pause`, or `Remove`.
- Coordination: counted lane holders, `yield_to`, `max_defer`, `Alive`, named
  concurrency limits, all five overlap policies, and catchup policies.
- Runtime instances: idempotent `ensure`, indexed selectors, scoped `reconcile`,
  buffered pokes, readable status, and retained run history.
- Persistence: MemoryStore, SQLite from the standard library, and optional
  PostgreSQL. Atomic per-key leases renew while handlers run and fence stale results.
- Testing: FakeClock simulates sleep, wall-clock changes, and DST without delays.

Lanes and limits coordinate one process. Shared stores prevent simultaneous
claims of the same key across processes; they do not distribute work or guarantee
exactly-once external effects.

[Documentation](docs/index.md) · [API reference](docs/reference.md) ·
[Examples](examples/) · [Contributing](CONTRIBUTING.md)
