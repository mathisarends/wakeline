from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from wakeline._time import seconds
from wakeline.triggers import cron, every, first, following, once, outside_quiet


def test_duration_validation() -> None:
    assert seconds("1h 30m 250ms") == 5400.25
    for value in ("", "10garbage", "-1s", -1, float("nan"), float("inf")):
        with pytest.raises(ValueError):
            seconds(value)


def test_trigger_validation_and_expiry() -> None:
    now = datetime(2026, 10, 30, 8, tzinfo=UTC)
    assert first(once(after="5m"), now) == now + timedelta(minutes=5)
    assert following(every(minutes=1, until=now), now) is None
    with pytest.raises(ValueError):
        once(at=datetime(2026, 1, 1))
    with pytest.raises(ValueError):
        once(at=now, after="1m")
    with pytest.raises(ValueError):
        every(seconds=1, jitter="2s")


def test_cron_dst_skips_nonexistent_hour_and_does_not_repeat_fixed_time() -> None:
    berlin = ZoneInfo("Europe/Berlin")
    trigger = cron("30 2 * * *", tz="Europe/Berlin")
    spring = datetime(2026, 3, 29, 0, tzinfo=UTC)
    result = following(trigger, spring)
    assert result is not None
    assert result.astimezone(berlin).hour == 3
    autumn = datetime(2026, 10, 25, 0, tzinfo=UTC)
    result = following(trigger, autumn)
    assert result is not None
    assert following(trigger, result).astimezone(berlin).day == 26


def test_quiet_window_crosses_midnight_and_dst() -> None:
    at = datetime(2026, 3, 28, 23, 30, tzinfo=UTC)
    assert outside_quiet(at, ("23:00", "07:00"), "Europe/Berlin") == datetime(
        2026, 3, 29, 5, tzinfo=UTC
    )
