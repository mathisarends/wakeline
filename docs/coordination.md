# Coordination and policies

## Lanes

An instance can hold a named `lane` while its handler runs. Holding a lane does
not by itself serialize jobs. Explicit `yield_to` rules say which lane holders
to wait for. Both fields accept label templates, for example `session/{session}`.
Yield patterns also accept shell-style globs.

```python
async with coord.busy("session/s9", reason="user_turn"):
    await handle_message()
```

Busy contexts count holders. Releasing one holder does not release the others.
Deferred status includes each matching lane, holder ID, and reason.
`max_defer="10m"` measures time since the first lane deferral. On expiry, only
the yield gate is bypassed. Requires, overlap, limits, leases, and quiet hours
still apply. Deferred work is reconsidered when a holder or handler finishes,
and on the coordinator poll.

## Dependencies and limits

`Alive("heartbeat/{agent}/*", within="2h")` requires at least one matching,
registered, unpaused instance with a successful completion within that window.
An absent or unhealthy dependency gives `blocked` status with a readable reason.
It is a health requirement, not an ordering graph or automatic dependency creator.

Register limits with `coord.limit("llm", max_concurrent=4)` and attach names at
kind or instance level with `limits=["llm"]`. Multiple attached limits all have
to be available. A missing limit is visibly blocked. Limits and lanes are local
to one coordinator/process, including when the store is shared.

## Overlap

Overlap applies to another running invocation of the **same instance key**.

| Policy | While another invocation runs |
| --- | --- |
| `Overlap.SKIP` | Consume the due invocation and record `skipped` in the ledger. |
| `Overlap.BUFFER_ONE` | Keep one pending invocation. |
| `Overlap.BUFFER_ALL` | Keep all pending invocations. |
| `Overlap.CANCEL_OTHER` | Request cancellation and wait for the old handler to finish before running its replacement. |
| `Overlap.ALLOW_ALL` | Start another invocation, still subject to lanes and limits. |

The default is `SKIP`. Cancellation is cooperative; handlers must propagate
`asyncio.CancelledError` and avoid blocking the event loop. A handler that
suppresses cancellation continues to occupy its lane and limits until it exits.

## Catchup

Advancing time or restarting can reveal several missed trigger slots.

| Policy | Missed slots |
| --- | --- |
| `Catchup.ONCE` | One invocation retaining the earliest due `scheduled_for`. |
| `Catchup.ALL` | Every missed slot in order; overlap policy still applies. |
| `Catchup.NONE` | Skip late slots; a slot within 1 ms of its deadline is on time. |
| `Catchup.window("1h")` | One invocation for the latest slot, if it lies inside the window. |

Every and once default to `ONCE`; cron defaults to a one-hour window. This
window policy coalesces eligible slots; use `ALL` to replay each slot. Buffered
requests retain their original timestamps. Large `ALL` backlogs intentionally
consume memory; choose a coalescing policy when unbounded replay is unsuitable.

## Poke and rearm

`coord.poke(selector, reason="activity")` immediately queues a management task.
Await its returned task when the caller needs the write to be durable before
continuing. `run_pending`, FakeClock, and stop also drain outstanding pokes.

A poke consumes the next planned trigger slot, replacing it with an immediate
invocation; the later cadence stays anchored. For a ten-minute interval poked at
minute one, the poke replaces minute ten and the following slot is minute twenty.
Repeated pending pokes coalesce. During a running handler, `Poke.BUFFER_ONE`
retains one follow-up independently of the default overlap `SKIP` policy;
`Poke.COALESCE` merges the event into the current invocation. Pokes respect pause,
quiet hours, requires, lanes, limits, and leases.

`Rearm(after="5m")` or `Rearm(at=aware_datetime)` adds a one-shot deadline.
The earlier of it and the normal trigger wins. That invocation consumes the
rearm; later invocations follow the trigger again. A rearm does not shift the
recurring cadence. Returning `Pause(reason)` keeps a visible paused instance;
returning `Remove()` deletes the instance and retains its ledger.
