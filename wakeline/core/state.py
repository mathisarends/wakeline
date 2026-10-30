from dataclasses import dataclass, field
from datetime import datetime

from wakeline.models import Cause, Run, Spec, State


@dataclass(frozen=True)
class Request:
    scheduled_for: datetime
    cause: Cause
    reason: str | None = None


@dataclass
class Instance:
    spec: Spec
    spec_hash: str
    generation: str
    nominal: datetime | None
    next_trigger: datetime | None
    rearm_at: datetime | None = None
    rearm_relative: bool = False
    pending: list[Request] = field(default_factory=list)
    pause_reason: str | None = None
    last_run: datetime | None = None
    last_success: datetime | None = None
    failures: int = 0
    defer_since: datetime | None = None
    state: State = State.SCHEDULED
    reason: str | None = None


@dataclass
class Lease:
    owner: str
    until: datetime


@dataclass
class Cell:
    instance: Instance | None = None
    lease: Lease | None = None
    runs: list[Run] = field(default_factory=list)
