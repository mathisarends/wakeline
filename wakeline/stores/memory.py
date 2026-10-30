import builtins
from uuid import uuid4

from .base import Stored


class MemoryStore:
    """Share this object between coordinators to exercise lease coordination."""

    def __init__(self) -> None:
        self._records: dict[str, Stored] = {}

    async def open(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def get(self, key: str) -> Stored | None:
        return self._records.get(key)

    async def list(self) -> builtins.list[Stored]:
        return list(self._records.values())

    async def put(self, key: str, value: str, *, expected: str | None) -> Stored | None:
        current = self._records.get(key)
        if (current.version if current is not None else None) != expected:
            return None
        record = Stored(key, uuid4().hex, value)
        self._records[key] = record
        return record

    async def delete(self, key: str, *, expected: str) -> bool:
        current = self._records.get(key)
        if current is None or current.version != expected:
            return False
        del self._records[key]
        return True
