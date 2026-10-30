from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum

from ._time import Duration, seconds


class Overlap(StrEnum):
    SKIP = "skip"
    BUFFER_ONE = "buffer_one"
    BUFFER_ALL = "buffer_all"
    CANCEL_OTHER = "cancel_other"
    ALLOW_ALL = "allow_all"


class Poke(StrEnum):
    COALESCE = "coalesce"
    BUFFER_ONE = "buffer_one"


@dataclass(frozen=True)
class CatchupWindow:
    seconds: float

    def __post_init__(self) -> None:
        seconds(self.seconds)


class Catchup(StrEnum):
    ONCE = "once"
    ALL = "all"
    NONE = "none"

    @staticmethod
    def window(
        duration: Duration | None = None,
        *,
        hours: float = 0,
        minutes: float = 0,
        seconds: float = 0,
    ) -> CatchupWindow:
        value = (
            duration
            if duration is not None
            else timedelta(hours=hours, minutes=minutes, seconds=seconds)
        )
        return CatchupWindow(_duration_seconds(value))


_duration_seconds = seconds


@dataclass(frozen=True)
class Alive:
    pattern: str
    within: Duration

    def __post_init__(self) -> None:
        if not self.pattern:
            raise ValueError("Alive pattern must not be empty")
        object.__setattr__(self, "within", seconds(self.within))
