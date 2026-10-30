import asyncio
from collections.abc import Callable

from wakeline.errors import ConflictError
from wakeline.stores import Store, Stored

from .codec import decode, encode
from .state import Cell


class Repository:
    def __init__(self, store: Store) -> None:
        self.store = store

    async def read(self, key: str) -> tuple[Stored | None, Cell]:
        stored = await self.store.get(key)
        return stored, Cell() if stored is None else decode(stored.value)

    async def change[T](self, key: str, change: Callable[[Cell], T]) -> tuple[Cell, T]:
        """Apply a pure synchronous mutation atomically, retrying CAS conflicts.

        Instance state, lease fencing, and ledger are one record, so claiming or
        completing a run never publishes only part of that transition.
        """
        for _ in range(100):
            stored, cell = await self.read(key)
            result = change(cell)
            value = encode(cell)
            if stored is not None and value == stored.value:
                return cell, result
            saved = await self.store.put(
                key, value, expected=None if stored is None else stored.version
            )
            if saved is not None:
                return cell, result
            await asyncio.sleep(0)
        raise ConflictError(f"Concurrent updates to {key!r} did not settle")
