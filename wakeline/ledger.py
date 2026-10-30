from .models import Run


class Ledger:
    """Synchronous view of the latest loaded ledger; status() refreshes shared stores."""

    def __init__(self) -> None:
        self._runs: dict[str, tuple[Run, ...]] = {}

    def replace(self, key: str, runs: list[Run]) -> None:
        if runs:
            self._runs[key] = tuple(runs)
        else:
            self._runs.pop(key, None)

    def last(self, key: str) -> Run | None:
        runs = self._runs.get(key, ())
        return runs[-1] if runs else None

    def history(self, key: str) -> tuple[Run, ...]:
        return self._runs.get(key, ())
