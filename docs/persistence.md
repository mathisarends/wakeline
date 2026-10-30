# Persistence and leases

```python
from wakeline import Coordinator
from wakeline.stores import MemoryStore, SqliteStore, PostgresStore

coord = Coordinator(store=SqliteStore("wake.db"))
# pip install 'wakeline[postgres]'
cluster = Coordinator(
    store=PostgresStore("postgresql://localhost/wake"), instance_id="node-a", lease_ttl="30s"
)
```

MemoryStore is the default. SQLite uses the standard-library sqlite3 driver with
WAL and a busy timeout. Disk operations run through `asyncio.to_thread`, so they
do not block the coordinator. Use a local SQLite file, not a network filesystem.
Each coordinator should own a separate store connection. PostgresStore uses an
optional asyncpg pool; importing wakeline does not load that driver.

## Atomic record protocol

`Store` is a small async protocol: `open`, `close`, `get`, `list`, `put`, and
`delete`. Each `Stored` envelope contains a key, an opaque unique version, and
versioned JSON. `put(..., expected=version)` atomically compares the version;
`expected=None` inserts only if absent. Conflicts return `None`. Delete also
requires a matching version. Versions cannot be recycled after recreation.

An instance, its lease, and its ledger share one record. Claiming a run advances
the trigger, acquires the lease, and records the running invocation in the same
CAS write. Completion, handler control results, and lease release also commit
together. This avoids partial lease/ledger transitions. PostgreSQL uses atomic
conditional updates rather than a worker queue or `SKIP LOCKED` work distribution.

Store adapters own their connections. Coordinator stop closes its store. All
coordinators must share compatible clocks and register the kinds they execute.
External writes and completions become visible on polling (`poll_interval="1s"`
by default); management reads refresh immediately. Registering a previously
absent kind lets an orphaned instance become executable again.

## Lease guarantees

Leases are per instance key, never per lane. The owner contains a user-readable
instance ID and a unique process incarnation. The loop renews active leases at
one-third of `lease_ttl`. Expired leases cannot be renewed. The old owner requests
handler cancellation and fences its completion so stale `Rearm`, `Pause`, and
`Remove` results cannot overwrite the replacement state.

Expired running claims are recorded as `lease_lost` and replayed using their
original due timestamp. Graceful stop records cancellation and leaves an
interrupted invocation pending for restart. Removing an instance cancels its
local handlers without resurrecting it. Updating a spec increments its
generation; the old handler can finish, but its control result cannot change
the new spec.

External effects can happen twice when a process dies after the effect but
before completion commits. Use idempotent application operations or downstream
fencing for payments, messages, and other effects that require it. A library
lease cannot stop blocking code or revoke an external request already sent.
Cross-process lanes and global distributed limits are outside this library.

## Parameters and retention

Parameters persist as JSON. Primitive JSON values and dataclass fields are
supported. With a module-level annotation `Ctx[MyDataclass]`, the top-level
dataclass is reconstructed automatically. For nested models or values needing
conversion, register `decode_params=` explicitly. Pydantic callers can pass
`model.model_dump(mode="json")` and register `Model.model_validate`; Pydantic is
not required by the library. Stored data never imports or unpickles a type.

The ledger keeps the last 50 completed runs per instance and a maximum age of
30 days. Configure `ledger_keep` and `ledger_max_age` on Coordinator. Active runs
are retained until completed or recovered. Pruning runs on completion and in
the timing loop, at most 60 seconds apart. Removed instances retain history
until it expires; then their empty records are deleted.

Store format 1 is the initial unreleased format. Future persisted-format changes
require an explicit compatibility decision and migration.
