# Application integration

Keep sessions, prompts, tools, and agent policy outside wakeline. Instances
provide labels and serializable parameters; handlers close over application
dependencies. A scoped scheduler validates horizons, quotas, and allowed kinds
before calling ensure. Never hand an unrestricted coordinator to an agent tool.

The repository includes executable offline examples:

| File | Demonstrates |
| --- | --- |
| `examples/01_coordination.py` | A user turn holds a session lane; heartbeat waits, then runs. |
| `examples/02_session_scheduler.py` | A session-scoped application facade, label isolation, quotas, and scheduling guards. |
| `examples/03_persistence.py` | SQLite restart with catchup and retained ledger. |

Run them with `uv run python examples/01_coordination.py` and similarly for the
other files. They use FakeClock and do not contact external services.

## Mapping existing cara code

The local cara scheduler separates At/Cron/Interval triggers and a runner,
but creates one task per job. Its automation adapter preserves the host's
`next_run_at`; its alarm adapter invents fresh IDs because scheduling the same
job ID cancels a running handler.

The wakeline mapping is:

- At → `once(at=...)`; Interval → `every(...)`; Cron → `cron(..., tz=...)`.
- Automation kind → an async handler calling the automation application service.
- Alarm kind → a handler returning Rearm for the next domain alarm phase, or
  Remove when the domain alarm is finished. Generations prevent a stale running
  handler from overwriting a changed schedule, so fresh synthetic IDs are unnecessary.
- `run_immediately=True` → ensure the recurring trigger, then poke its key.
- Host startup schedule restoration → a persistent store plus idempotent ensure;
  application-owned `next_run_at` can instead be represented by a one-shot
  initial trigger and a handler rearm.

The existing cara scheduling port has synchronous arm/disarm methods. An actual
migration must make that boundary async to await persistence; this is a real
integration change, not just different configuration. The alarm domain decides
its next phase; wakeline does not replace that domain logic.

These mappings and the offline examples validate API shapes, not deployed
application migrations. Neither neighboring cara nor harnessed is modified.
Maximus source was not available in the workspace. Completing both real
migrations remains the validation gate before calling this abstraction stable.
