from collections.abc import Mapping
from types import MappingProxyType
from typing import Self


class Sel:
    """An exact-match kind/label selector. ``&`` intersects selectors."""

    def __init__(self, *, kind: str | None = None, **labels: str) -> None:
        self.kind = kind
        self.labels: Mapping[str, str] = MappingProxyType(dict(labels))

    def matches(self, kind: str, labels: Mapping[str, str]) -> bool:
        return (self.kind is None or self.kind == kind) and all(
            labels.get(name) == value for name, value in self.labels.items()
        )

    def __and__(self, other: Self) -> Self:
        if self.kind is not None and other.kind is not None and self.kind != other.kind:
            raise ValueError("Cannot intersect different kinds")
        labels = dict(self.labels)
        for name, value in other.labels.items():
            if name in labels and labels[name] != value:
                raise ValueError(f"Conflicting selector label: {name}")
            labels[name] = value
        return type(self)(kind=self.kind or other.kind, **labels)
