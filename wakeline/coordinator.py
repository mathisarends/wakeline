import asyncio
import inspect
from collections.abc import AsyncGenerator, Callable, Iterable
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import Self, Unpack, overload
from uuid import uuid4

from ._time import Duration, seconds
from .clock import Clock, FakeClock, SystemClock
from .core.codec import decode_spec, spec_data, spec_hash
from .core.registry import Decoder, Handler, Kind, decoder_for
from .core.runtime import RunHook, Runtime, StatusHook
from .core.schedule import deadline
from .core.state import Cell, Instance, Request
from .fields import PartialSpecFields, SpecFields, UpdateFields, build_spec
from .ledger import Ledger
from .models import Cause, Holder, Outcome, ReconcileResult, Spec, State, Status
from .policies import Catchup, CatchupWindow, Overlap, Poke
from .selectors import Sel
from .stores import MemoryStore, Store
from .triggers import Cron, Trigger, first, following


class Coordinator:
    """Coordinate async wakeups in one event loop, optionally sharing durable leases.

    Register kinds before entering the async context. ``ensure`` is available
    before start. FakeClock can drive a coordinator without a background loop.
    """

    def __init__(
        self,
        *,
        store: Store | None = None,
        clock: Clock | None = None,
        instance_id: str | None = None,
        lease_ttl: Duration = "30s",
        poll_interval: Duration = "1s",
        ledger_keep: int = 50,
        ledger_max_age: Duration = "30d",
        on_run: RunHook | None = None,
        on_defer: StatusHook | None = None,
        on_block: StatusHook | None = None,
    ) -> None:
        if ledger_keep < 1:
            raise ValueError("ledger_keep must be positive")
        self._clock = clock if clock is not None else SystemClock()
        owner = f"{instance_id or 'coordinator'}:{uuid4().hex}"
        self._runtime = Runtime(
            store=store if store is not None else MemoryStore(),
            clock=self._clock,
            owner=owner,
            lease_ttl=seconds(lease_ttl),
            poll_interval=seconds(poll_interval),
            ledger_keep=ledger_keep,
            ledger_age=seconds(ledger_max_age),
            on_run=on_run,
            on_defer=on_defer,
            on_block=on_block,
        )
        self._static: list[Spec] = []
        self._materialized: set[str] = set()
        self._operations: set[asyncio.Task[int]] = set()
        if isinstance(self._clock, FakeClock):
            self._clock.attach(self._drain)

    @property
    def ledger(self) -> Ledger:
        return self._runtime.ledger

    def limit(self, name: str, *, max_concurrent: int) -> None:
        if not name or max_concurrent < 1:
            raise ValueError("A named limit must allow at least one concurrent run")
        self._runtime.limits[name] = max_concurrent
        self._runtime.reconsider()

    def kind(
        self,
        name: str,
        *,
        overlap: Overlap = Overlap.SKIP,
        catchup: Catchup | CatchupWindow | None = None,
        limits: Iterable[str] = (),
        pause_on_failure: int | None = None,
        decode_params: Decoder | None = None,
    ) -> Callable[[Handler], Handler]:
        if not name:
            raise ValueError("Kind must not be empty")
        if pause_on_failure is not None and pause_on_failure < 1:
            raise ValueError("pause_on_failure must be positive")

        def register(handler: Handler) -> Handler:
            if name in self._runtime.kinds:
                raise ValueError(f"Kind {name!r} is already registered")
            if not inspect.iscoroutinefunction(handler):
                raise TypeError("Wakeup handlers must be async functions")
            self._runtime.kinds[name] = Kind(
                handler,
                overlap,
                catchup,
                tuple(limits),
                pause_on_failure,
                decode_params or decoder_for(handler),
            )
            for key, entry in self._runtime.entries.items():
                if entry.instance.spec.kind == name:
                    self._runtime.accept(
                        key, Cell(entry.instance, runs=list(self.ledger.history(key)))
                    )
            self._runtime.wake()
            return handler

        return register

    def wakeup(
        self,
        name: str,
        trigger: Trigger,
        *,
        lane: str | None = None,
        overlap: Overlap = Overlap.SKIP,
        catchup: Catchup | CatchupWindow | None = None,
        limits: Iterable[str] = (),
    ) -> Callable[[Handler], Handler]:
        def register(handler: Handler) -> Handler:
            registered = self.kind(name, overlap=overlap, catchup=catchup, limits=limits)(handler)
            self._static.append(Spec(name, name, trigger, lane=lane))
            return registered

        return register

    async def start(self) -> None:
        if isinstance(self._clock, FakeClock):
            self._clock.attach(self._drain)
        await self._prepare_static()
        await self._runtime.start()

    async def stop(self) -> None:
        if self._operations:
            await asyncio.gather(*self._operations)
        await self._runtime.stop()
        if isinstance(self._clock, FakeClock):
            self._clock.detach(self._drain)

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    def _resolve(self, spec: Spec) -> Spec:
        kind = self._runtime.kinds.get(spec.kind)
        if kind is None:
            raise ValueError(f"Register kind {spec.kind!r} before ensuring instances")
        catchup = spec.catchup if spec.catchup is not None else kind.catchup
        if catchup is None:
            catchup = Catchup.window(hours=1) if isinstance(spec.trigger, Cron) else Catchup.ONCE
        spec = replace(
            spec,
            overlap=spec.overlap if spec.overlap is not None else kind.overlap,
            catchup=catchup,
            limits=spec.limits if spec.limits is not None else kind.limits,
            pause_on_failure=spec.pause_on_failure
            if spec.pause_on_failure is not None
            else kind.pause_on_failure,
        )
        # Round-trip parameters here: invalid persistence fails before any mutation,
        # and memory behaves exactly like durable stores.
        return decode_spec(spec_data(spec))

    @overload
    async def ensure(self, spec: Spec, /) -> Status: ...

    @overload
    async def ensure(self, /, **fields: Unpack[SpecFields]) -> Status: ...

    async def ensure(
        self, spec: Spec | None = None, /, **fields: Unpack[PartialSpecFields]
    ) -> Status:
        if spec is not None and fields:
            raise TypeError("Pass either Spec or keyword fields")
        desired = self._resolve(spec if spec is not None else build_spec(fields))
        async with self._runtime.lock:
            await self._runtime.initialize()
            await self._ensure(desired)
            return self._runtime.status(desired.key)

    async def _ensure(self, spec: Spec) -> bool:
        fingerprint = spec_hash(spec)
        now = self._clock.now()

        def change(cell: Cell) -> bool:
            old = cell.instance
            if old is not None and old.spec_hash == fingerprint:
                return False
            nominal = first(spec.trigger, now)
            instance = Instance(
                spec, fingerprint, uuid4().hex, nominal, deadline(spec.trigger, nominal, spec.key)
            )
            if old is not None:
                instance.last_run = old.last_run
                instance.last_success = old.last_success
                instance.failures = old.failures
                instance.pause_reason = old.pause_reason
            cell.instance = instance
            return True

        cell, changed = await self._runtime.repository.change(spec.key, change)
        self._runtime.accept(spec.key, cell)
        self._runtime.wake()
        return changed

    async def _selected(self, selector: Sel | str) -> list[str]:
        await self._runtime.initialize()
        await self._runtime.refresh()
        if isinstance(selector, str):
            return [selector] if selector in self._runtime.entries else []
        return sorted(self._runtime.select(selector.kind, dict(selector.labels)))

    async def status(self, selector: Sel | str | None = None) -> list[Status]:
        async with self._runtime.lock:
            keys = await self._selected(selector if selector is not None else Sel())
            return [self._runtime.status(key) for key in keys]

    async def count(self, selector: Sel | str | None = None) -> int:
        return len(await self.status(selector))

    async def update(self, selector: Sel | str, **fields: Unpack[UpdateFields]) -> int:
        async with self._runtime.lock:
            keys = await self._selected(selector)
            desired = [
                self._resolve(replace(self._runtime.entries[key].instance.spec, **fields))
                for key in keys
            ]
            return sum([await self._ensure(spec) for spec in desired])

    async def pause(self, selector: Sel | str, *, reason: str) -> int:
        if not reason:
            raise ValueError("A pause needs a reason")
        return await self._pause(selector, reason)

    async def resume(self, selector: Sel | str) -> int:
        return await self._pause(selector, None)

    async def _pause(self, selector: Sel | str, reason: str | None) -> int:
        async with self._runtime.lock:
            keys = await self._selected(selector)
            for key in keys:

                def change(cell: Cell) -> None:
                    if cell.instance is not None:
                        cell.instance.pause_reason = reason
                        cell.instance.state = (
                            State.PAUSED if reason is not None else State.SCHEDULED
                        )
                        cell.instance.reason = reason

                cell, _ = await self._runtime.repository.change(key, change)
                self._runtime.accept(key, cell)
            self._runtime.reconsider()
            return len(keys)

    async def remove(self, selector: Sel | str) -> int:
        async with self._runtime.lock:
            keys = await self._selected(selector)
            for key in keys:
                await self._remove(key)
            return len(keys)

    async def _remove(self, key: str) -> None:
        def change(cell: Cell) -> None:
            cell.instance = None

        cell, _ = await self._runtime.repository.change(key, change)
        self._runtime.accept(key, cell)
        self._runtime.cancel(key)
        self._runtime.reconsider()

    def poke(self, selector: Sel | str, *, reason: str = "poke") -> asyncio.Task[int]:
        """Queue a poke immediately; optionally await the returned task for durability."""
        task = asyncio.create_task(self._poke(selector, reason), name="wakeline:poke")
        self._operations.add(task)
        task.add_done_callback(self._operations.discard)
        return task

    async def _poke(self, selector: Sel | str, reason: str) -> int:
        async with self._runtime.lock:
            keys = await self._selected(selector)
            for key in keys:

                def change(cell: Cell) -> None:
                    instance = cell.instance
                    if instance is None or instance.pause_reason is not None:
                        return
                    running = (
                        cell.lease is not None
                        and cell.lease.until > self._clock.now()
                        and any(run.outcome == Outcome.RUNNING for run in cell.runs)
                    )
                    if running and instance.spec.poke == Poke.COALESCE:
                        return
                    if instance.pending:
                        return
                    instance.pending.append(Request(self._clock.now(), Cause.POKE, reason))
                    if instance.nominal is not None:
                        instance.nominal = following(instance.spec.trigger, instance.nominal)
                        instance.next_trigger = deadline(
                            instance.spec.trigger, instance.nominal, instance.spec.key
                        )
                    instance.rearm_at = None

                cell, _ = await self._runtime.repository.change(key, change)
                self._runtime.accept(key, cell)
            self._runtime.wake()
            return len(keys)

    @asynccontextmanager
    async def busy(self, lane: str, *, reason: str) -> AsyncGenerator[None]:
        if not lane or not reason:
            raise ValueError("A lane and a readable holder reason are required")
        holder = Holder(uuid4().hex, reason)
        async with self._runtime.lock:
            self._runtime.lanes.setdefault(lane, {})[holder.id] = holder
        try:
            yield
        finally:
            async with self._runtime.lock:
                holders = self._runtime.lanes.get(lane, {})
                holders.pop(holder.id, None)
                if not holders:
                    self._runtime.lanes.pop(lane, None)
                self._runtime.reconsider()

    async def reconcile(self, *, scope: Sel, desired: Iterable[Spec]) -> ReconcileResult:
        specs = [self._resolve(spec) for spec in desired]
        if any(not scope.matches(spec.kind, spec.labels) for spec in specs):
            raise ValueError("Every desired instance must match the reconciliation scope")
        wanted = {spec.key for spec in specs}
        if len(wanted) != len(specs):
            raise ValueError("Desired keys must be unique")
        async with self._runtime.lock:
            existing = set(await self._selected(scope))
            if any(
                spec.key in self._runtime.entries and spec.key not in existing for spec in specs
            ):
                raise ValueError("Reconcile cannot overwrite an instance outside its scope")
            created = updated = unchanged = 0
            for spec in specs:
                changed = await self._ensure(spec)
                if spec.key not in existing:
                    created += 1
                elif changed:
                    updated += 1
                else:
                    unchanged += 1
            for key in existing - wanted:
                await self._remove(key)
            return ReconcileResult(created, updated, len(existing - wanted), unchanged)

    async def run_pending(self) -> None:
        """Run currently due work to quiescence without waiting for blocked handlers."""
        await self._drain()

    async def _drain(self) -> int:
        await self._prepare_static()
        if self._operations:
            await asyncio.gather(*self._operations)
        return await self._runtime.drain()

    async def _prepare_static(self) -> None:
        for spec in self._static:
            if spec.key not in self._materialized:
                await self.ensure(spec)
                self._materialized.add(spec.key)
