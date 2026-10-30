# Releases and compatibility

## 0.1.0 — initial implementation

- Kinds and runtime instances, idempotent ensure, indexed selectors, scoped reconcile.
- Cron/every/once triggers, stable jitter, quiet hours, expiry, and handler control results.
- Counted lane holders, explicit yield with max_defer, Alive requirements, named limits.
- Overlap/catchup policies, buffered poke coalescing, readable status, and bounded ledger retention.
- Memory, SQLite, and optional PostgreSQL stores with atomic version checks and per-key leases.
- Lease renewal, fencing, replay of abandoned claims, and orderly shutdown/restart.
- Clock protocol/FakeClock, offline examples, typed package, and Python 3.12–3.14 CI.

This version is pre-stable. Store format 1 is explicit; no compatibility layer
for older unreleased formats is included. Application migrations and validation
status are documented in [design](design.md).
