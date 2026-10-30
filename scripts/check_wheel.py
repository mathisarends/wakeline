import os
import subprocess
import sys
import tarfile
from pathlib import Path
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[1]
SMOKE = r"""
import asyncio
import sys
from importlib.metadata import distribution
from pathlib import Path

import wakeline
from wakeline import Coordinator, Ctx, FakeClock, every
from wakeline.stores import SqliteStore

package = Path(wakeline.__file__).resolve()
assert package.is_relative_to(Path(sys.prefix).resolve()), package
installed = distribution('wakeline')
assert installed.metadata['Name'] == 'wakeline'
assert installed.metadata['License-Expression'] == 'MIT'
assert installed.metadata.get_all('License-File') == ['LICENSE']
assert (package.parent / 'py.typed').is_file()
assert not any(str(file).startswith(('tests/', 'docs/', 'examples/'))
               for file in installed.files or ())
assert not any(name in sys.modules for name in ('asyncpg', 'pytest', 'mkdocs', 'pydantic'))
for name in wakeline.__all__:
    assert getattr(wakeline, name) is not None

async def main():
    clock = FakeClock()
    coord = Coordinator(clock=clock, store=SqliteStore(':memory:'))
    seen = []
    @coord.kind('smoke')
    async def smoke(ctx: Ctx[None]) -> None:
        seen.append(ctx.key)
    await coord.ensure(kind='smoke', key='one', trigger=every(minutes=1))
    await clock.advance('1m')
    assert seen == ['one']
    assert coord.ledger.last('one').outcome == 'ok'
    await coord.stop()

asyncio.run(main())
print('Installed wakeline wheel, typed API, SQLite execution, and metadata verified.')
"""


def main() -> None:
    uv = os.environ.get("UV_BIN", "uv")
    wheels = tuple((ROOT / "dist").glob("*.whl"))
    if len(wheels) != 1:
        raise SystemExit("Build exactly one wheel with `uv build --no-sources` first.")
    sources = tuple((ROOT / "dist").glob("*.tar.gz"))
    if len(sources) != 1:
        raise SystemExit("Build exactly one source archive first.")
    with tarfile.open(sources[0]) as archive:
        paths = {name.split("/", 1)[1] for name in archive.getnames() if "/" in name}
    assert {
        "README.md",
        "LICENSE",
        "pyproject.toml",
        "uv.lock",
        "mkdocs.yml",
        "docs/index.md",
        "examples/01_coordination.py",
        "scripts/check_wheel.py",
        "tests/test_examples.py",
        "wakeline/py.typed",
    } <= paths
    with TemporaryDirectory(prefix="wakeline-wheel-") as directory:
        temporary = Path(directory)
        environment = temporary / "venv"
        requirements = temporary / "requirements.txt"
        subprocess.run(
            [
                uv,
                "export",
                "--locked",
                "--no-dev",
                "--no-emit-project",
                "--output-file",
                str(requirements),
            ],
            cwd=ROOT,
            check=True,
            stdout=subprocess.DEVNULL,
        )
        subprocess.run([uv, "venv", "--python", sys.executable, str(environment)], check=True)
        python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        subprocess.run(
            [
                uv,
                "pip",
                "install",
                "--python",
                str(python),
                "--require-hashes",
                "--requirements",
                str(requirements),
            ],
            check=True,
        )
        subprocess.run(
            [uv, "pip", "install", "--python", str(python), "--no-deps", str(wheels[0])],
            check=True,
        )
        subprocess.run([str(python), "-I", "-c", SMOKE], cwd=temporary, check=True)


if __name__ == "__main__":
    main()
