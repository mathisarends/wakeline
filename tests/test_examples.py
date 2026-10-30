import runpy
from pathlib import Path

import pytest

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"


@pytest.mark.parametrize("path", sorted(EXAMPLES.glob("[0-9]*.py")), ids=lambda path: path.stem)
def test_offline_example(path: Path) -> None:
    runpy.run_path(str(path), run_name="__main__")
