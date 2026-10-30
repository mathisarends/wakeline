from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType

from ._time import Duration, aware, seconds
from .policies import Alive, Catchup, CatchupWindow, Overlap, Poke
from .triggers import Trigger


class State(StrEnum):
    SCHEDULED = "scheduled"
    DEFERRED = "deferred"
    BLOCKED = "blocked"
    RUNNING = "running"
    PAUSED = "paused"
    ORPHANED = "orphaned"


class Outcome(StrEnum):
    RUNNING = "running"
    OK = "ok"
    ERROR = "error"
    CANCELLED = "cancelled"
    LEASE_LOST = "lease_lost"
    SKIPPED = "skipped"


class Cause(StrEnum):
    TRIGGER = "trigger"
    REARM = "rearm"
    POKE = "poke"


@dataclass(frozen=True)
class Rearm:
    after: Duration | None = None
    at: datetime | None = None

    def __post_init__(self) -> None:
        if (self.after is None) == (self.at is None):
            raise ValueError("Specify exactly one of after and at")
        if self.after is not None:
            object.__setattr__(self, "after", seconds(self.after))
        if self.at is not None:
            object.__setattr__(self, "at", aware(self.at))


@dataclass(frozen=True)
class Remove:
    pass


@dataclass(frozen=True)
class Pause:
    reason: str

    def __post_init__(self) -> None:
        if not self.reason:
            raise ValueError("A pause needs a reason")


type Result = Rearm | Remove | Pause | None


@dataclass(frozen=True)
class Ctx[ParamsT]:
    key: str
    kind: str
    labels: Mapping[str, str]
    params: ParamsT
    scheduled_for: datetime
    fired_at: datetime
    cause: Cause
    reason: str | None
    run_id: str


@dataclass(frozen=True)
class Spec:
    kind: str
    key: str
    trigger: Trigger
    labels: Mapping[str, str] = field(default_factory=dict)
    params: object = None
    lane: str | None = None
    yield_to: Sequence[str] = ()
    max_defer: Duration = "10m"
    requires: Sequence[Alive] = ()
    overlap: Overlap | None = None
    catchup: Catchup | CatchupWindow | None = None
    limits: Sequence[str] | None = None
    poke: Poke = Poke.BUFFER_ONE
    pause_on_failure: int | None = None

    def __post_init__(self) -> None:
        if not self.kind or not self.key:
            raise ValueError("Kind and key must not be empty")
        labels = dict(self.labels)
        if any(not isinstance(k, str) or not isinstance(v, str) for k, v in labels.items()):
            raise TypeError("Labels must map strings to strings")
        object.__setattr__(self, "labels", MappingProxyType(labels))
        object.__setattr__(self, "yield_to", tuple(self.yield_to))
        object.__setattr__(self, "requires", tuple(self.requires))
        object.__setattr__(self, "max_defer", seconds(self.max_defer, allow_zero=True))
        if self.limits is not None:
            object.__setattr__(self, "limits", tuple(self.limits))
        if self.pause_on_failure is not None and self.pause_on_failure < 1:
            raise ValueError("pause_on_failure must be a positive failure count")
        for template in (self.lane, *self.yield_to, *(r.pattern for r in self.requires)):
            if template is not None:
                try:
                    template.format_map(labels)
                except (KeyError, ValueError) as error:
                    raise ValueError(f"Invalid label template {template!r}: {error}") from error


@dataclass(frozen=True)
class Holder:
    id: str
    reason: str


@dataclass(frozen=True)
class Status:
    key: str
    kind: str
    labels: Mapping[str, str]
    state: State
    reason: str | None
    next_fire: datetime | None
    last_run: datetime | None
    last_success: datetime | None
    failures: int
    holders: Mapping[str, tuple[Holder, ...]] = field(default_factory=dict)


@dataclass(frozen=True)
class Run:
    id: str
    key: str
    scheduled_for: datetime
    fired_at: datetime
    finished_at: datetime | None
    outcome: Outcome
    cause: Cause
    reason: str | None = None
    error: str | None = None
    duration: float | None = None
    generation: str | None = None


@dataclass(frozen=True)
class ReconcileResult:
    created: int = 0
    updated: int = 0
    removed: int = 0
    unchanged: int = 0
