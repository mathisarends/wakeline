"""An application-owned facade: agent concepts and guardrails stay outside wakeline."""

import asyncio
import builtins
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import uuid4

from wakeline import Coordinator, Ctx, FakeClock, Sel, Status, once


@dataclass(frozen=True)
class WakeParams:
    note: str


@dataclass(frozen=True)
class WakePolicy:
    max_per_session: int = 20
    min_delay: timedelta = timedelta(minutes=5)
    max_horizon: timedelta = timedelta(days=90)


class SessionScheduler:
    def __init__(
        self,
        coord: Coordinator,
        clock: FakeClock,
        *,
        agent: str,
        session: str,
        policy: WakePolicy | None = None,
    ) -> None:
        self._coord = coord
        self._clock = clock
        self._policy = policy or WakePolicy()
        self._labels = {"agent": agent, "session": session, "source": "agent"}
        self._scope = Sel(kind="agent_wakeup", **self._labels)
        # Serialize check-and-create operations in this application facade.
        self._lock = asyncio.Lock()

    async def schedule(self, note: str, *, at: datetime) -> str:
        if at.utcoffset() is None:
            raise ValueError("Include a timezone in the reminder time.")
        delay = at - self._clock.now()
        if not self._policy.min_delay <= delay <= self._policy.max_horizon:
            raise ValueError("Choose a reminder between five minutes and 90 days from now.")
        if not note.strip():
            raise ValueError("Include what the reminder should say.")
        async with self._lock:
            if await self._coord.count(self._scope) >= self._policy.max_per_session:
                raise ValueError("Cancel an existing reminder before creating another one.")
            wid = uuid4().hex[:12]
            await self._coord.ensure(
                kind="agent_wakeup",
                key=f"agent/{self._labels['agent']}/{self._labels['session']}/{wid}",
                trigger=once(at=at),
                labels={**self._labels, "wid": wid},
                params=WakeParams(note),
                lane="session/{session}",
                yield_to=["session/{session}"],
            )
            return wid

    async def list(self) -> builtins.list[Status]:
        return await self._coord.status(self._scope)

    async def cancel(self, wid: str) -> bool:
        return await self._coord.remove(self._scope & Sel(wid=wid)) > 0


async def main() -> None:
    clock = FakeClock()
    coord = Coordinator(clock=clock)
    messages: list[str] = []

    @coord.kind("agent_wakeup")
    async def wakeup(ctx: Ctx[WakeParams]) -> None:
        messages.append(ctx.params.note)

    alice = SessionScheduler(coord, clock, agent="a", session="alice")
    bob = SessionScheduler(coord, clock, agent="a", session="bob")
    wid = await alice.schedule("Check the parcel", at=clock.now() + timedelta(minutes=10))
    assert not await bob.cancel(wid)
    assert len(await alice.list()) == 1
    await clock.advance("10m")
    assert messages == ["Check the parcel"]
    assert await alice.list() == []
    print(messages[0])
    await coord.stop()


if __name__ == "__main__":
    asyncio.run(main())
