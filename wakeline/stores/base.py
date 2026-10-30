import builtins
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class Stored:
    key: str
    version: str
    value: str


class Store(Protocol):
    """Atomic versioned records containing instance, lease, and run ledger.

    ``put`` is compare-and-swap: ``expected=None`` means insert only. A successful
    write returns a new, globally unique version; a conflict returns None. Stores
    must not recycle versions after deletion (ABA protection). Values are JSON.
    """

    async def open(self) -> None: ...
    async def close(self) -> None: ...
    async def get(self, key: str) -> Stored | None: ...
    async def list(self) -> builtins.list[Stored]: ...
    async def put(self, key: str, value: str, *, expected: str | None) -> Stored | None: ...
    async def delete(self, key: str, *, expected: str) -> bool: ...
