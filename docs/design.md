# Design and validation

The implementation follows the October 30, 2026 specification: a process-local,
domain-neutral coordination layer with runtime instances and optional shared
per-key leases. It does not contain agent sessions, a task queue, distributed
worker placement, workflow replay, or automatic backoff.

## Boundaries

| Package/module | Responsibility |
| --- | --- |
| `coordinator.py` | Public registration, lifecycle, selectors, management, reconciliation, busy contexts. |
| `models.py`, `policies.py`, `triggers.py`, `selectors.py` | Public scheduling contracts and validation. |
| `clock.py` | SystemClock/FakeClock and deterministic draining. |
| `core/runtime.py` | Central heap, atomic decisions, handler tasks, lane/limit accounting, lease recovery, hooks, retention. |
| `core/schedule.py` | Trigger cursors, monotonic deadlines, jitter, and catchup. |
| `core/state.py`, `core/codec.py`, `core/repository.py` | Persisted domain state, explicit JSON format, and CAS retries. |
| `stores/` | Atomic storage adapters; no scheduling decisions. |

No coordinator lock is held while executing application handlers or hooks.
Management and dispatch decisions are serialized on one event loop. A shared
store claim atomically writes trigger progress, lease ownership, and the running
ledger entry before the handler starts. Application changes increment a
generation; stale handler control results cannot replace newer desired state.

## Scheduling dependencies

[cronsim](https://github.com/cuu508/cronsim) was selected for its explicit
zoneinfo/DST semantics, small pure-Python implementation, and use in
Healthchecks. The checked release is 2.7 (October 21, 2025), not a claim that
weekly releases are necessary. It is the only core scheduling dependency;
Windows also installs tzdata for IANA zones.

[pyncette](https://github.com/tibordp/pyncette) already handles dynamic recurring
tasks and shared-store execution. Its scheduler/repository lifecycle would
duplicate ownership of wake deadlines, while lanes, poke coalescing, and handler
rearm still require central decisions. This is the reason for keeping a small
dedicated loop; it is an architectural assessment, not a benchmark comparison.

The referenced cronoq package could not be verified from accessible primary
package/repository sources during this implementation. It was not adopted or
claimed to be fully evaluated. Revisit it if an authoritative repository URL is
provided. This does not affect the public API or optional store protocol.

## Validation state

- Automated: catchup after sleep, both DST transitions, wall-clock jumps,
  poke during an invocation, all overlap policies, lane holders, requires,
  named limits, lease renewal/loss, abandoned one-shots, stale results,
  shutdown/restart, retention, reconcile, and 1,000 idle instances.
- Real SQLite connection races and reopen behavior run locally and in CI.
- PostgreSQL CAS/coordination run in the dedicated CI service job; local runs
  require a DSN. A Docker executable was present locally, but no running daemon.
- Installed-wheel smoke checks run outside the checkout with optional imports
  absent, in addition to metadata and typed-marker checks.
- Real cara and Maximus migrations remain open; see [integration](integration.md).

This is an initial 0.1.0 implementation. The two-application migration criterion
must be completed before treating the API as a stable cross-application contract.
