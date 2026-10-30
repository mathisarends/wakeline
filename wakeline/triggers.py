from dataclasses import dataclass
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from cronsim import CronSim

from ._time import Duration, aware, seconds

type Quiet = tuple[str, str]


def _validate_quiet(quiet: Quiet | None) -> None:
    if quiet is not None:
        start, end = (time.fromisoformat(part) for part in quiet)
        if start == end or start.tzinfo is not None or end.tzinfo is not None:
            raise ValueError("Quiet hours require two different local times without offsets")


def outside_quiet(value: datetime, quiet: Quiet | None, tz: str) -> datetime:
    """Move a deadline to the first allowed local minute, including across DST."""
    if quiet is None:
        return aware(value)
    start, end = (time.fromisoformat(part) for part in quiet)
    zone = ZoneInfo(tz)
    candidate = aware(value)
    # Iterate absolute minutes: nonexistent and repeated local hours remain well-defined.
    for _ in range(24 * 60 + 181):
        local = candidate.astimezone(zone).time().replace(tzinfo=None)
        inside = start <= local < end if start < end else local >= start or local < end
        if not inside:
            return candidate
        candidate = candidate.replace(second=0, microsecond=0) + timedelta(minutes=1)
    raise ValueError("Quiet hours leave no reachable execution time")


@dataclass(frozen=True)
class Every:
    interval: float
    jitter: float = 0
    quiet: Quiet | None = None
    tz: str = "UTC"
    until: datetime | None = None

    def __post_init__(self) -> None:
        seconds(self.interval)
        seconds(self.jitter, allow_zero=True)
        if self.jitter >= self.interval:
            raise ValueError("Jitter must be smaller than the interval")
        _validate_quiet(self.quiet)
        ZoneInfo(self.tz)
        if self.until is not None:
            object.__setattr__(self, "until", aware(self.until))


@dataclass(frozen=True)
class Cron:
    expression: str
    tz: str = "UTC"
    jitter: float = 0
    quiet: Quiet | None = None
    until: datetime | None = None

    def __post_init__(self) -> None:
        CronSim(self.expression, datetime(2026, 1, 1, tzinfo=ZoneInfo(self.tz)))
        seconds(self.jitter, allow_zero=True)
        _validate_quiet(self.quiet)
        if self.until is not None:
            object.__setattr__(self, "until", aware(self.until))


@dataclass(frozen=True)
class Once:
    at: datetime | None = None
    after: float | None = None
    quiet: Quiet | None = None
    tz: str = "UTC"

    def __post_init__(self) -> None:
        if (self.at is None) == (self.after is None):
            raise ValueError("Specify exactly one of at and after")
        if self.at is not None:
            object.__setattr__(self, "at", aware(self.at))
        if self.after is not None:
            seconds(self.after, allow_zero=True)
        _validate_quiet(self.quiet)
        ZoneInfo(self.tz)


type Trigger = Every | Cron | Once


def every(
    duration: Duration | None = None,
    *,
    seconds: float = 0,
    minutes: float = 0,
    hours: float = 0,
    days: float = 0,
    jitter: Duration = 0,
    quiet: Quiet | None = None,
    tz: str = "UTC",
    until: datetime | None = None,
) -> Every:
    value = (
        duration
        if duration is not None
        else timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
    )
    return Every(_seconds(value), _seconds(jitter, allow_zero=True), quiet, tz, until)


def cron(
    expression: str,
    *,
    tz: str = "UTC",
    jitter: Duration = 0,
    quiet: Quiet | None = None,
    until: datetime | None = None,
) -> Cron:
    return Cron(expression, tz, _seconds(jitter, allow_zero=True), quiet, until)


def once(
    *,
    at: datetime | None = None,
    after: Duration | None = None,
    quiet: Quiet | None = None,
    tz: str = "UTC",
) -> Once:
    return Once(at, None if after is None else _seconds(after, allow_zero=True), quiet, tz)


_seconds = seconds


def following(trigger: Trigger, previous: datetime) -> datetime | None:
    """Return the next nominal deadline; all comparisons use UTC."""
    match trigger:
        case Every(interval=interval):
            result = aware(previous) + timedelta(seconds=interval)
        case Cron(expression=expression, tz=tz):
            try:
                result = aware(next(CronSim(expression, previous.astimezone(ZoneInfo(tz)))))
            except StopIteration:
                return None
        case Once():
            return None
    if trigger.until is not None and result > trigger.until:
        return None
    return result


def first(trigger: Trigger, now: datetime) -> datetime | None:
    if isinstance(trigger, Once):
        return trigger.at if trigger.at is not None else now + timedelta(seconds=trigger.after or 0)
    return following(trigger, now)
