# API reference

## Coordinator

`Coordinator(*, store=None, clock=None, instance_id=None, lease_ttl="30s",
poll_interval="1s", ledger_keep=50, ledger_max_age="30d", on_run=None,
on_defer=None, on_block=None)`

| Operation | Contract |
| --- | --- |
| `kind(name, overlap=Overlap.SKIP, catchup=None, limits=(), pause_on_failure=None, decode_params=None)` | Decorate an async `handler(ctx)`; duplicate names are rejected. |
| `wakeup(name, trigger, lane=None, overlap=..., catchup=None, limits=())` | Register a kind and a single static instance using the same name. |
| `limit(name, max_concurrent=...)` | Register or replace a positive process-local limit. |
| `await start()` / `await stop()` | Start the timing task; cancel and persist running invocations on shutdown. Also usable as an async context manager. |
| `await ensure(Spec(...))` | Idempotently create/update an instance and return Status. Also accepts the Spec fields as keyword arguments. |
| `await update(selector, **fields)` | Update matched specs and return the number changed. Kind/key are identities; use ensure to change kinds. |
| `await pause(selector, reason=...)` / `await resume(selector)` | Persist pause state; existing handlers are allowed to finish. Return number matched. |
| `await remove(selector)` | Delete matched instances and cancel their local handlers. Return number matched. |
| `poke(selector, reason="poke")` | Return an asyncio Task yielding number matched; awaiting confirms persistence. Paused instances are ignored. |
| `await count(selector=None)` | Count instances, including paused, blocked, and orphaned instances. |
| `await status(selector=None)` | Refresh shared state and return Status objects ordered by key. |
| `busy(lane, reason=...)` | Async context manager adding a counted lane holder. |
| `await reconcile(scope=Sel(...), desired=[Spec(...)])` | Preflight scope and duplicate keys, then create/update/remove within scope; return ReconcileResult. |
| `await run_pending()` | Drain due decisions and runnable handlers without waiting for external I/O. |

Selectors are exact string keys or `Sel(kind="heartbeat", agent="a1")`. Omitted
status/count selectors match all. `Sel(...) & Sel(...)` intersects constraints;
conflicting labels or kinds raise ValueError. Kind/label selection uses inverted
indexes. Alive and lane patterns use shell-style glob matching separately.

Ensure hashes the resolved spec. Equal specs preserve the next deadline and
health; changed specs reset trigger/rearm/pending state but preserve last-run,
last-success, consecutive-failure count, and explicit pause. Omitted instance
policies inherit kind defaults. Reconcile validates the complete desired list
before mutation, protects out-of-scope keys, and is idempotent. It is a sequence
of atomic per-key writes, not a cross-key transaction.

## Spec and triggers

`Spec(kind, key, trigger, labels={}, params=None, lane=None, yield_to=(),
max_defer="10m", requires=(), overlap=None, catchup=None, limits=None,
poke=Poke.BUFFER_ONE, pause_on_failure=None)`

Duration inputs accept timedelta, numeric seconds, or strings such as `"1h 30m"`.
Datetime inputs must be timezone-aware. Invalid templates, non-string labels,
non-finite durations, sync handlers, and unserializable parameters are rejected.

- `every(duration=None, seconds=0, minutes=0, hours=0, days=0, jitter=0,
  quiet=None, tz="UTC", until=None)` returns Every.
- `cron(expression, tz="UTC", jitter=0, quiet=None, until=None)` returns Cron.
- `once(at=None, after=None, quiet=None, tz="UTC")` returns Once; exactly one
  of at/after is required.

Handler return values are `None`, `Rearm(after=... | at=...)`, `Remove()`, and
`Pause(reason)`. Only one Rearm field is allowed. `pause_on_failure=n` pauses
after n consecutive errors; a successful completion resets the count. Errors
are logged and recorded; wakeline does not implement retries/backoff.
An exhausted one-shot paused by its handler retains the invocation so resume
can run it again. High write contention can raise `wakeline.errors.ConflictError`
after 100 CAS attempts; callers can retry the management operation.

## Context, status, and observability

`Ctx[ParamsT]` exposes `key`, `kind`, immutable `labels`, decoded `params`,
`scheduled_for`, `fired_at`, `cause`, `reason`, and `run_id`. It does not expose
application concepts or provide access to other instances.

Status exposes `key`, `kind`, `labels`, `state`, `reason`, `next_fire`, `last_run`,
`last_success`, `failures`, and matching `holders`. States are scheduled,
deferred, blocked, running, paused, and orphaned. Paused takes precedence in
management status, including while an already-started invocation finishes.

`coord.ledger.last(key)` returns a Run or None; `history(key)` returns an immutable
tuple. This is a synchronous view of the last loaded state. Call status first
for a fresh shared-store view. Runs expose their IDs, scheduled/fired/finished
times, monotonic `duration` in seconds, cause, reason, outcome, and error text.
Outcomes are running, ok, error, cancelled, lease_lost, and skipped.

Hooks are async callables: `on_run(Run)`, `on_defer(Status)`, and
`on_block(Status)`. Status hooks fire on state/reason changes. They run outside
the coordinator lock, can read management state, and cannot corrupt scheduling
if they raise. Hooks should complete promptly; hook failures are logged.
