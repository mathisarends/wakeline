from dataclasses import dataclass
from typing import assert_type

from wakeline import Coordinator, Ctx, FakeClock, Rearm, Sel, Spec, Status, every
from wakeline.stores import MemoryStore, PostgresStore, SqliteStore, Store


@dataclass
class Params:
    note: str


async def contracts() -> None:
    store: Store = MemoryStore()
    store = SqliteStore(":memory:")
    store = PostgresStore("postgresql://localhost/wake")
    coord = Coordinator(store=store, clock=FakeClock())

    @coord.kind("job")
    async def job(ctx: Ctx[Params]) -> Rearm | None:
        assert_type(ctx.params.note, str)
        return None

    assert_type(
        await coord.ensure(
            kind="job", key="j", trigger=every(minutes=1), params=Params("hello"), yield_to=["user"]
        ),
        Status,
    )
    spec = Spec("job", "j", every(minutes=1), limits=["llm"])
    assert_type(await coord.ensure(spec), Status)
    assert_type(await coord.status(Sel(kind="job")), list[Status])
    assert_type(await coord.update("j", trigger=every(minutes=2)), int)
    assert_type(await coord.poke("j"), int)
