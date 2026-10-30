import asyncio
import heapq
import inspect
import logging
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import timedelta
from fnmatch import fnmatchcase
from types import MappingProxyType
from uuid import uuid4

from wakeline._time import seconds
from wakeline.clock import Clock
from wakeline.ledger import Ledger
from wakeline.models import (
    Cause,
    Ctx,
    Holder,
    Outcome,
    Pause,
    Rearm,
    Remove,
    Result,
    Run,
    State,
    Status,
)
from wakeline.policies import Overlap
from wakeline.stores import Store
from wakeline.triggers import outside_quiet

from .codec import decode
from .registry import Kind
from .repository import Repository
from .schedule import Timeline
from .state import Cell, Instance, Lease, Request

logger = logging.getLogger(__name__)

type RunHook = Callable[[Run], Awaitable[None]]
type StatusHook = Callable[[Status], Awaitable[None]]


@dataclass
class Entry:
    instance: Instance
    timeline: Timeline
    ticket: int = 0


@dataclass
class Execution:
    run: Run
    generation: str
    kind: Kind
    limits: tuple[str, ...]
    lane: str | None
    started_at: float
    task: asyncio.Task[None] | None = None
    cancellation: Outcome | None = None
    started: bool = False
    retry_on_cancel: bool = False


@dataclass(frozen=True)
class Completion:
    execution: Execution
    result: Result
    outcome: Outcome
    error: str | None


class Runtime:
    def __init__(
        self,
        *,
        store: Store,
        clock: Clock,
        owner: str,
        lease_ttl: float,
        poll_interval: float,
        ledger_keep: int,
        ledger_age: float,
        on_run: RunHook | None,
        on_defer: StatusHook | None,
        on_block: StatusHook | None,
    ) -> None:
        self.repository = Repository(store)
        self.clock = clock
        self.owner = owner
        self.lease_ttl = lease_ttl
        self.poll_interval = min(poll_interval, lease_ttl / 3)
        self.ledger_keep = ledger_keep
        self.ledger_age = ledger_age
        self.ledger = Ledger()
        self.kinds: dict[str, Kind] = {}
        self.limits: dict[str, int] = {}
        self.entries: dict[str, Entry] = {}
        self.lanes: dict[str, dict[str, Holder]] = {}
        self.lock = asyncio.Lock()
        self._heap: list[tuple[float, int, str]] = []
        self._ticket = 0
        self._runs: dict[str, Execution] = {}
        self._completions: deque[Completion] = deque()
        self._waiting: set[str] = set()
        self._versions: dict[str, str] = {}
        self._by_kind: dict[str, set[str]] = {}
        self._by_label: dict[tuple[str, str], set[str]] = {}
        self._wake = asyncio.Event()
        self._loop_task: asyncio.Task[None] | None = None
        self._opened = False
        self._closing = False
        self._next_poll = 0.0
        self._next_prune = 0.0
        self._next_renew = 0.0
        self._activity = 0
        self._on_run = on_run
        self._on_defer = on_defer
        self._on_block = on_block
        self._run_notifications: deque[tuple[RunHook, Run]] = deque()
        self._status_notifications: deque[tuple[StatusHook, Status]] = deque()

    async def initialize(self) -> None:
        if not self._opened:
            await self.repository.store.open()
            self._opened = True
            await self.refresh()

    def wake(self) -> None:
        self._wake.set()

    def active(self, key: str) -> list[Execution]:
        return [run for run in self._runs.values() if run.run.key == key]

    def select(self, kind: str | None, labels: dict[str, str]) -> set[str]:
        candidates = []
        if kind is not None:
            candidates.append(self._by_kind.get(kind, set()))
        candidates.extend(self._by_label.get(item, set()) for item in labels.items())
        return set.intersection(*candidates) if candidates else set(self.entries)

    def _index(self, key: str, instance: Instance, *, remove: bool = False) -> None:
        kind = instance.spec.kind
        if remove:
            keys = self._by_kind.get(kind, set())
            keys.discard(key)
            if not keys:
                self._by_kind.pop(kind, None)
        else:
            self._by_kind.setdefault(kind, set()).add(key)
        for item in instance.spec.labels.items():
            if remove:
                keys = self._by_label.get(item, set())
                keys.discard(key)
                if not keys:
                    self._by_label.pop(item, None)
            else:
                self._by_label.setdefault(item, set()).add(key)

    def accept(self, key: str, cell: Cell, *, retry_at: float | None = None) -> None:
        self.ledger.replace(key, cell.runs)
        old = self.entries.get(key)
        if old is not None:
            self._index(key, old.instance, remove=True)
        instance = cell.instance
        if instance is None:
            self.entries.pop(key, None)
            self._waiting.discard(key)
            return
        if old is not None and old.instance.generation == instance.generation:
            old.timeline.sync(instance, self.clock)
            entry = Entry(instance, old.timeline)
        else:
            entry = Entry(instance, Timeline.build(instance, self.clock))
        self.entries[key] = entry
        self._index(key, instance)
        self._enqueue(key, entry, retry_at=retry_at)

    def _enqueue(self, key: str, entry: Entry, *, retry_at: float | None = None) -> None:
        self._ticket += 1
        entry.ticket = self._ticket
        instance = entry.instance
        if instance.pause_reason is not None or instance.spec.kind not in self.kinds:
            return
        deadline = entry.timeline.next(instance, self.clock)
        if deadline is not None:
            if retry_at is not None:
                deadline = max(deadline, retry_at)
            heapq.heappush(self._heap, (deadline, entry.ticket, key))
        if len(self._heap) > max(128, len(self.entries) * 3):
            self._heap = [item for item in self._heap if self._valid(item)]
            heapq.heapify(self._heap)

    def _valid(self, item: tuple[float, int, str]) -> bool:
        entry = self.entries.get(item[2])
        return entry is not None and entry.ticket == item[1]

    async def refresh(self) -> None:
        records = await self.repository.store.list()
        seen = set()
        for record in records:
            seen.add(record.key)
            cell = decode(record.value)
            if cell.instance is None and self.active(record.key):
                self.cancel(record.key)
            expired = cell.lease is not None and cell.lease.until <= self.clock.now()
            if expired and not self.active(record.key):
                cell, _ = await self.repository.change(record.key, self._recover)
                self.accept(record.key, cell)
                self._versions.pop(record.key, None)
            elif self._versions.get(record.key) != record.version:
                self.accept(record.key, cell)
                self._versions[record.key] = record.version
        for key in (set(self.entries) | set(self._versions)) - seen:
            self.accept(key, Cell())
            self._versions.pop(key, None)
            self.cancel(key)
        # Reproject absolute cron/once deadlines when wall time changes.
        for key, entry in self.entries.items():
            entry.timeline.sync(entry.instance, self.clock)
            if key not in self._waiting:
                self._enqueue(key, entry)
        self._next_poll = self.clock.monotonic() + self.poll_interval

    def _recover(self, cell: Cell) -> None:
        now = self.clock.now()
        if cell.lease is None or cell.lease.until > now:
            return
        instance = cell.instance
        for index, run in enumerate(cell.runs):
            if run.outcome == Outcome.RUNNING:
                cell.runs[index] = replace(
                    run, outcome=Outcome.LEASE_LOST, finished_at=now, error="Owner lease expired"
                )
                if instance is not None and run.generation == instance.generation:
                    instance.pending.append(Request(run.scheduled_for, run.cause, run.reason))
                    instance.state, instance.reason = State.SCHEDULED, None
        cell.lease = None
        self._retain(cell)

    def reconsider(self) -> None:
        for key in tuple(self._waiting):
            if key in self.entries:
                self._enqueue(key, self.entries[key])
        self._waiting.clear()
        self.wake()

    def holders(self, instance: Instance) -> dict[str, tuple[Holder, ...]]:
        patterns = [p.format_map(instance.spec.labels) for p in instance.spec.yield_to]
        return {
            lane: tuple(holders.values())
            for lane, holders in self.lanes.items()
            if holders and any(fnmatchcase(lane, pattern) for pattern in patterns)
        }

    def status(self, key: str) -> Status:
        instance = self.entries[key].instance
        state, reason = instance.state, instance.reason
        if instance.pause_reason is not None:
            state, reason = State.PAUSED, instance.pause_reason
        elif instance.spec.kind not in self.kinds:
            state, reason = State.ORPHANED, f"Kind {instance.spec.kind!r} is not registered"
        elif self.active(key):
            state, reason = State.RUNNING, None
        dates = [d for d in (instance.next_trigger, instance.rearm_at) if d is not None]
        dates.extend(request.scheduled_for for request in instance.pending)
        return Status(
            key,
            instance.spec.kind,
            instance.spec.labels,
            state,
            reason,
            min(dates) if dates else None,
            instance.last_run,
            instance.last_success,
            instance.failures,
            MappingProxyType(self.holders(instance)),
        )

    def cancel(
        self, key: str, outcome: Outcome = Outcome.CANCELLED, *, retry: bool = False
    ) -> None:
        for execution in self.active(key):
            if execution.cancellation is None:
                execution.cancellation = outcome
                execution.retry_on_cancel = retry
                if execution.task is not None:
                    execution.task.cancel()

    async def start(self) -> None:
        async with self.lock:
            await self.initialize()
            self._closing = False
            if self._loop_task is None:
                self._loop_task = asyncio.create_task(self._loop(), name="wakeline:coordinator")

    async def stop(self) -> None:
        self._closing = True
        if self._loop_task is not None:
            self._loop_task.cancel()
            await asyncio.gather(self._loop_task, return_exceptions=True)
            self._loop_task = None
        for execution in tuple(self._runs.values()):
            self.cancel(execution.run.key, retry=True)
        tasks = [execution.task for execution in self._runs.values() if execution.task is not None]
        await asyncio.gather(*tasks, return_exceptions=True)
        async with self.lock:
            while self._completions:
                await self._finish(self._completions[0])
                self._completions.popleft()
        await self._emit()
        async with self.lock:
            if self._opened:
                await self.repository.store.close()
                self._opened = False

    async def _loop(self) -> None:
        while True:
            self._wake.clear()
            try:
                await self.drive()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Coordinator tick failed; retrying after the poll interval")
                await self.clock.sleep(self.poll_interval)
                continue
            while self._heap and not self._valid(self._heap[0]):
                heapq.heappop(self._heap)
            until = min(self._heap[0][0] if self._heap else float("inf"), self._next_poll)
            sleeper = asyncio.create_task(self.clock.sleep(max(0, until - self.clock.monotonic())))
            wake = asyncio.create_task(self._wake.wait())
            try:
                await asyncio.wait((sleeper, wake), return_when=asyncio.FIRST_COMPLETED)
            finally:
                sleeper.cancel()
                wake.cancel()
                await asyncio.gather(sleeper, wake, return_exceptions=True)

    async def drain(self) -> int:
        before = self._activity
        stable = 0
        for _ in range(10000):
            previous = self._activity
            await self.drive(force_refresh=stable == 0)
            await asyncio.sleep(0)
            runnable = any(e.task is not None and _runnable(e.task) for e in self._runs.values())
            if self._activity == previous and not self._completions and not runnable:
                stable += 1
                if stable == 2:
                    return self._activity - before
            else:
                stable = 0
        raise RuntimeError(
            "Coordinator did not become idle; check for an immediate scheduling cycle"
        )

    async def drive(self, *, force_refresh: bool = False) -> None:
        async with self.lock:
            await self.initialize()
            if force_refresh or self.clock.monotonic() >= self._next_poll:
                await self.refresh()
                self.reconsider()
            await self._renew()
            while self._completions:
                await self._finish(self._completions[0])
                self._completions.popleft()
            if self._closing:
                return
            processed = 0
            while self._heap and self._heap[0][0] <= self.clock.monotonic():
                item = heapq.heappop(self._heap)
                if not self._valid(item):
                    continue
                await self._dispatch(item[2])
                processed += 1
                if processed >= 256:
                    self.wake()
                    break
            if self.clock.monotonic() >= self._next_prune:
                await self._prune()
        await self._emit()

    async def _emit(self) -> None:
        while self._run_notifications:
            hook, run = self._run_notifications.popleft()
            await _hook(hook, run)
        while self._status_notifications:
            hook, status = self._status_notifications.popleft()
            await _hook(hook, status)

    def _dependencies(self, instance: Instance) -> str | None:
        for requirement in instance.spec.requires:
            pattern = requirement.pattern.format_map(instance.spec.labels)
            cutoff = self.clock.now() - timedelta(seconds=seconds(requirement.within))
            alive = any(
                fnmatchcase(key, pattern)
                and entry.instance.pause_reason is None
                and entry.instance.spec.kind in self.kinds
                and entry.instance.last_success is not None
                and entry.instance.last_success >= cutoff
                for key, entry in self.entries.items()
            )
            if not alive:
                return (
                    f"Requires Alive({pattern!r}, within={requirement.within}s): "
                    + "no healthy instance"
                )
        return None

    async def _dispatch(self, key: str) -> None:
        entry = self.entries[key]
        now = self.clock.now()
        active = self.active(key)

        def change(cell: Cell) -> tuple[Run | None, bool]:
            instance = cell.instance
            if instance is None:
                return None, False
            if instance.pause_reason is not None or instance.spec.kind not in self.kinds:
                return None, False
            timeline = (
                replace(entry.timeline)
                if entry.timeline.generation == instance.generation
                else Timeline.build(instance, self.clock)
            )
            timeline.sync(instance, self.clock)
            timeline.collect(instance, self.clock)
            if not instance.pending:
                if instance.next_trigger is None and instance.rearm_at is None and not active:
                    cell.instance = None
                return None, False
            allowed = outside_quiet(now, instance.spec.trigger.quiet, instance.spec.trigger.tz)
            if allowed > now:
                instance.state = State.DEFERRED
                instance.reason = f"Quiet hours until {allowed.isoformat()}"
                return None, False
            reason = self._dependencies(instance)
            if reason is not None:
                instance.state, instance.reason = State.BLOCKED, reason
                return None, False
            holders = self.holders(instance)
            if holders:
                instance.defer_since = instance.defer_since or now
                elapsed = (now - instance.defer_since).total_seconds()
                if elapsed < seconds(instance.spec.max_defer, allow_zero=True):
                    details = "; ".join(
                        f"{lane}: " + ", ".join(f"{h.id} ({h.reason})" for h in hs)
                        for lane, hs in holders.items()
                    )
                    instance.state, instance.reason = State.DEFERRED, f"Yielding to {details}"
                    return None, False
            overlap = instance.spec.overlap
            if active and overlap != Overlap.ALLOW_ALL:
                if overlap == Overlap.SKIP:
                    cell.runs.extend(
                        Run(
                            uuid4().hex,
                            key,
                            request.scheduled_for,
                            now,
                            now,
                            Outcome.SKIPPED,
                            request.cause,
                            "overlap",
                            duration=0,
                        )
                        for request in instance.pending
                        if request.cause != Cause.POKE
                    )
                    instance.pending[:] = [r for r in instance.pending if r.cause == Cause.POKE][:1]
                    instance.state = State.DEFERRED if instance.pending else State.RUNNING
                    instance.reason = (
                        "Poke buffered until handler completes" if instance.pending else None
                    )
                    self._retain(cell)
                    return None, False
                if overlap in (Overlap.BUFFER_ONE, Overlap.CANCEL_OTHER):
                    instance.pending[:] = instance.pending[:1]
                instance.state, instance.reason = (
                    State.DEFERRED,
                    f"Overlap policy {overlap}: waiting for running handler",
                )
                return None, overlap == Overlap.CANCEL_OTHER
            for name in instance.spec.limits or ():
                if name not in self.limits:
                    instance.state, instance.reason = (
                        State.BLOCKED,
                        f"Limit {name!r} is not configured",
                    )
                    return None, False
                count = sum(name in execution.limits for execution in self._runs.values())
                if count >= self.limits[name]:
                    instance.state, instance.reason = (
                        State.DEFERRED,
                        f"Limit {name!r} is busy ({count}/{self.limits[name]})",
                    )
                    return None, False
            if cell.lease is not None and cell.lease.until > now and cell.lease.owner != self.owner:
                instance.state, instance.reason = (
                    State.DEFERRED,
                    f"Lease held by {cell.lease.owner} until {cell.lease.until.isoformat()}",
                )
                return None, False
            if active and any(e.cancellation == Outcome.LEASE_LOST for e in active):
                instance.state, instance.reason = (
                    State.DEFERRED,
                    "Waiting for handler after lease loss",
                )
                return None, False
            for index, run in enumerate(cell.runs):
                if run.outcome == Outcome.RUNNING and not any(e.run.id == run.id for e in active):
                    cell.runs[index] = replace(
                        run,
                        outcome=Outcome.LEASE_LOST,
                        finished_at=now,
                        error="Lease expired before completion",
                    )
            cell.lease = Lease(self.owner, now + timedelta(seconds=self.lease_ttl))
            request = instance.pending.pop(0)
            run = Run(
                uuid4().hex,
                key,
                request.scheduled_for,
                now,
                None,
                Outcome.RUNNING,
                request.cause,
                request.reason,
                generation=instance.generation,
            )
            cell.runs.append(run)
            instance.last_run = now
            instance.defer_since = None
            instance.state, instance.reason = State.RUNNING, None
            return run, False

        previous_state = entry.instance.state, entry.instance.reason
        cell, (run, cancel) = await self.repository.change(key, change)
        instance = cell.instance
        if instance != entry.instance or run is not None:
            self._activity += 1
        if cancel:
            self.cancel(key)
        retry = None
        if run is None and instance is not None and instance.pending:
            self._waiting.add(key)
            retry = self.clock.monotonic() + self.poll_interval
        self.accept(key, cell, retry_at=retry)
        if run is not None and instance is not None:
            self._waiting.discard(key)
            self._launch(run, instance)
        elif instance is not None and previous_state != (instance.state, instance.reason):
            hook = self._on_block if instance.state == State.BLOCKED else self._on_defer
            if hook is not None and instance.state in (State.BLOCKED, State.DEFERRED):
                self._status_notifications.append((hook, self.status(key)))

    def _launch(self, run: Run, instance: Instance) -> None:
        kind = self.kinds[instance.spec.kind]
        lane = (
            None
            if instance.spec.lane is None
            else instance.spec.lane.format_map(instance.spec.labels)
        )
        execution = Execution(
            run,
            instance.generation,
            kind,
            tuple(instance.spec.limits or ()),
            lane,
            self.clock.monotonic(),
        )
        self._runs[run.id] = execution
        if lane is not None:
            self.lanes.setdefault(lane, {})[run.id] = Holder(run.id, f"wakeup:{run.key}")
        context = Ctx(
            run.key,
            instance.spec.kind,
            instance.spec.labels,
            instance.spec.params,
            run.scheduled_for,
            run.fired_at,
            run.cause,
            run.reason,
            run.id,
        )

        async def execute() -> None:
            execution.started = True
            result: Result = None
            outcome, error = Outcome.OK, None
            try:
                decoded = replace(context, params=kind.decode(context.params))
                result = await kind.handler(decoded)
                if result is not None and not isinstance(result, (Rearm, Remove, Pause)):
                    raise TypeError("Handlers must return None, Rearm, Remove, or Pause")
            except asyncio.CancelledError:
                outcome = execution.cancellation or Outcome.CANCELLED
            except Exception as exception:
                outcome, error = Outcome.ERROR, f"{type(exception).__name__}: {exception}"
                logger.exception("Wakeup %s failed", run.key)
            finally:
                outcome = execution.cancellation or outcome
                self._completions.append(Completion(execution, result, outcome, error))
                self.wake()

        execution.task = asyncio.create_task(execute(), name=f"wakeline:run:{run.key}")

        def done(task: asyncio.Task[None]) -> None:
            if not execution.started:
                self._completions.append(
                    Completion(execution, None, execution.cancellation or Outcome.CANCELLED, None)
                )
                self.wake()

        execution.task.add_done_callback(done)
        if len(self._runs) == 1:
            self._next_renew = self.clock.monotonic() + self.lease_ttl / 3

    async def _finish(self, completion: Completion) -> None:
        execution = completion.execution
        run = execution.run
        if run.id not in self._runs:
            return
        remaining = [e for e in self.active(run.key) if e.run.id != run.id]
        now = self.clock.now()
        result = completion.result
        finished = replace(
            run,
            outcome=completion.outcome,
            error=completion.error,
            finished_at=now,
            duration=max(0, self.clock.monotonic() - execution.started_at),
        )

        def change(cell: Cell) -> Run:
            nonlocal finished
            fenced = (
                cell.lease is not None and cell.lease.owner == self.owner and cell.lease.until > now
            )
            if not fenced:
                finished = replace(
                    finished, outcome=Outcome.LEASE_LOST, error="Lease lost before completion"
                )
            instance = cell.instance
            original = next((item for item in cell.runs if item.id == run.id), None)
            replay = (
                not fenced
                and original is not None
                and original.outcome == Outcome.RUNNING
                or fenced
                and finished.outcome == Outcome.CANCELLED
                and execution.retry_on_cancel
            )
            if replay and instance is not None and instance.generation == execution.generation:
                instance.pending.append(Request(run.scheduled_for, run.cause, run.reason))
            cell.runs[:] = [finished if item.id == run.id else item for item in cell.runs]
            if fenced and instance is not None:
                if finished.outcome == Outcome.OK:
                    instance.last_success = now
                    instance.failures = 0
                elif finished.outcome == Outcome.ERROR:
                    instance.failures += 1
                if instance.generation == execution.generation:
                    if finished.outcome == Outcome.OK:
                        if isinstance(result, Remove):
                            cell.instance = None
                        elif isinstance(result, Pause):
                            instance.pause_reason = result.reason
                        elif isinstance(result, Rearm):
                            at = (
                                result.at
                                if result.at is not None
                                else now + timedelta(seconds=seconds(result.after or 0))
                            )
                            instance.rearm_at = outside_quiet(
                                at, instance.spec.trigger.quiet, instance.spec.trigger.tz
                            )
                            instance.rearm_relative = result.after is not None
                    if (
                        instance.spec.pause_on_failure is not None
                        and instance.failures >= instance.spec.pause_on_failure
                    ):
                        instance.pause_reason = f"{instance.failures} consecutive handler failures"
                if cell.instance is not None:
                    instance.state = (
                        State.PAUSED if instance.pause_reason is not None else State.SCHEDULED
                    )
                    instance.reason = instance.pause_reason
                    if (
                        instance.pause_reason is None
                        and not remaining
                        and not instance.pending
                        and instance.next_trigger is None
                        and instance.rearm_at is None
                    ):
                        cell.instance = None
            if fenced and not remaining:
                cell.lease = None
            self._retain(cell)
            return finished

        cell, finished = await self.repository.change(run.key, change)
        self._runs.pop(run.id, None)
        if execution.lane is not None:
            holders = self.lanes.get(execution.lane, {})
            holders.pop(run.id, None)
            if not holders:
                self.lanes.pop(execution.lane, None)
        self.accept(run.key, cell)
        if cell.instance is None:
            self.cancel(run.key)
        self._activity += 1
        self.reconsider()
        if self._on_run is not None:
            self._run_notifications.append((self._on_run, finished))

    async def _renew(self) -> None:
        if not self._runs or self.clock.monotonic() < self._next_renew:
            return
        now = self.clock.now()
        for key in {e.run.key for e in self._runs.values()}:

            def change(cell: Cell) -> bool:
                if cell.lease is None or cell.lease.owner != self.owner or cell.lease.until <= now:
                    return False
                cell.lease.until = now + timedelta(seconds=self.lease_ttl)
                return True

            cell, renewed = await self.repository.change(key, change)
            if not renewed or cell.instance is None:
                self.cancel(key, Outcome.LEASE_LOST if not renewed else Outcome.CANCELLED)
            self.ledger.replace(key, cell.runs)
        self._next_renew = self.clock.monotonic() + self.lease_ttl / 3

    def _retain(self, cell: Cell) -> None:
        cutoff = self.clock.now() - timedelta(seconds=self.ledger_age)
        complete = [
            run
            for run in cell.runs
            if run.outcome != Outcome.RUNNING and (run.finished_at or run.fired_at) >= cutoff
        ][-self.ledger_keep :]
        kept = {run.id for run in complete}
        cell.runs[:] = [
            run for run in cell.runs if run.outcome == Outcome.RUNNING or run.id in kept
        ]

    async def _prune(self) -> None:
        for record in await self.repository.store.list():
            cell, _ = await self.repository.change(record.key, self._retain)
            self.ledger.replace(record.key, cell.runs)
            if (
                cell.instance is None
                and not cell.runs
                and (cell.lease is None or cell.lease.until <= self.clock.now())
            ):
                latest = await self.repository.store.get(record.key)
                if latest is not None and decode(latest.value) == cell:
                    await self.repository.store.delete(record.key, expected=latest.version)
        self._next_prune = self.clock.monotonic() + min(self.ledger_age, 60)


async def _hook[T](hook: Callable[[T], Awaitable[None]], value: T) -> None:
    try:
        await hook(value)
    except Exception:
        logger.exception("Observability hook failed")


def _runnable(task: asyncio.Task[None]) -> bool:
    """Inspect public coroutine delegation to distinguish sleep(0) from blocked I/O."""
    if task.done():
        return False
    current = task.get_coro()
    while True:
        if inspect.iscoroutine(current):
            waiting = current.cr_await
        elif inspect.isgenerator(current):
            waiting = current.gi_yieldfrom
        else:
            return False
        if waiting is None:
            return True
        current = waiting
