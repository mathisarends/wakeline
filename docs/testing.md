# Testing and time

```python
from datetime import datetime
from zoneinfo import ZoneInfo
from wakeline import Coordinator, FakeClock

clock = FakeClock(start=datetime(2026, 10, 30, 8, tzinfo=ZoneInfo("Europe/Berlin")))
coord = Coordinator(clock=clock)
```

`await clock.advance("30m")` jumps directly to the target time, simulating
sleep/downtime, then drains attached coordinators. Catchup policies decide how
many invocations run. It does not visit every intermediate deadline. To simulate
continuous operation, advance in the desired increments.

Advance returns when due scheduling decisions and runnable handlers have
settled. Handlers blocked on an unset event or external I/O remain running, so
tests can inspect overlap, lane holders, and limits. Zero-delay async delegation
is drained, including handlers that await `asyncio.sleep(0)` repeatedly.
Use `await coord.run_pending()` after releasing a test event.

`await clock.jump_wall(timedelta(...))` changes wall time independently of
monotonic time. Every, relative once, and relative rearm retain their in-process
monotonic deadlines. Absolute once and cron follow wall time. After process
restart, intervals are reconstructed from their persisted wall deadline; a
monotonic clock cannot be transferred between process lifetimes.

Cron parsing and DST behavior are delegated to cronsim and zoneinfo. Fixed local
cron times follow cronsim's Debian-cron rules: the spring gap shifts a missed
fixed time forward, and the repeated autumn hour does not duplicate that fixed
daily invocation. All stored comparisons use UTC.

Quiet windows use `("23:00", "07:00")` local HH:MM values and a trigger timezone.
They defer execution until outside the window, including during DST transitions.
Jitter is nonnegative and deterministic for a key and nominal deadline, so
idempotent ensure, reloads, and competing coordinators do not resample it. For
intervals, jitter must be smaller than the interval. `until` bounds recurring
nominal slots; a late catchup can still run after that bound.

The suite covers sleep, DST, wall changes, all overlap policies, counted holders,
pokes during a run, self-removal, rearm, pause-on-failure, leases, shutdown,
restart, scoped reconcile, and the installed wheel. PostgreSQL tests skip
locally without `WAKELINE_POSTGRES_DSN`; CI always runs them against a service.
