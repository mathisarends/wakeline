import math
import re
from datetime import UTC, datetime, timedelta

type Duration = str | float | int | timedelta

_PART = re.compile(r"(\d+(?:\.\d+)?)\s*(ms|s|m|h|d|w)")
_UNITS = {"ms": 0.001, "s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


def seconds(value: Duration, *, allow_zero: bool = False) -> float:
    """Parse seconds, a timedelta, or a duration such as ``'1h 30m'``."""
    if isinstance(value, timedelta):
        result = value.total_seconds()
    elif isinstance(value, str):
        remaining = value.strip()
        result = 0.0
        if not remaining:
            raise ValueError("Duration must not be empty")
        while remaining:
            match = _PART.match(remaining)
            if match is None:
                raise ValueError(f"Invalid duration: {value!r}")
            result += float(match[1]) * _UNITS[match[2]]
            remaining = remaining[match.end() :].strip()
    else:
        result = float(value)
    if not math.isfinite(result) or result < 0 or (result == 0 and not allow_zero):
        raise ValueError(
            "Duration must be finite and positive"
            if not allow_zero
            else "Duration must be finite and nonnegative"
        )
    return result


def aware(value: datetime) -> datetime:
    if value.utcoffset() is None:
        raise ValueError("Datetime must include a timezone")
    return value.astimezone(UTC)
