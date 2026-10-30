import asyncio
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Protocol

from ._time import Duration, aware, seconds


class Clock(Protocol):
    def now(self) -> datetime: ...
    def monotonic(self) -> float: ...
    async def sleep(self, delay: float) -> None: ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)

    def monotonic(self) -> float:
        return time.monotonic()

    async def sleep(self, delay: float) -> None:
        await asyncio.sleep(max(0, delay))


class FakeClock:
    """Advance directly to simulate sleep; drain attached coordinators to quiescence.

    Blocked async handlers remain running. ``advance`` never waits for external I/O.
    """

    def __init__(self, start: datetime | None = None) -> None:
        self._now = aware(start or datetime(2026, 1, 1, tzinfo=UTC))
        self._monotonic = 0.0
        self._sleepers: set[tuple[float, asyncio.Future[None]]] = set()
        self._drainers: list[Callable[[], Awaitable[int]]] = []

    def now(self) -> datetime:
        return self._now

    def monotonic(self) -> float:
        return self._monotonic

    async def sleep(self, delay: float) -> None:
        if delay <= 0:
            await asyncio.sleep(0)
            return
        future = asyncio.get_running_loop().create_future()
        sleeper = (self._monotonic + delay, future)
        self._sleepers.add(sleeper)
        try:
            await future
        finally:
            self._sleepers.discard(sleeper)

    def attach(self, drain: Callable[[], Awaitable[int]]) -> None:
        if drain not in self._drainers:
            self._drainers.append(drain)

    def detach(self, drain: Callable[[], Awaitable[int]]) -> None:
        if drain in self._drainers:
            self._drainers.remove(drain)

    async def advance(self, amount: Duration) -> None:
        elapsed = seconds(amount, allow_zero=True)
        self._monotonic += elapsed
        self._now += timedelta(seconds=elapsed)
        await self._settle()

    async def jump_wall(self, amount: timedelta) -> None:
        """Simulate an NTP/manual clock change without advancing interval time."""
        self._now += amount
        await self._settle()

    async def _settle(self) -> None:
        for deadline, future in tuple(self._sleepers):
            if deadline <= self._monotonic and not future.done():
                future.set_result(None)
        for _ in range(10000):
            progress = sum([await drain() for drain in tuple(self._drainers)])
            if not progress:
                return
        raise RuntimeError("FakeClock coordinators did not become idle")
