# Contributing

Use Python 3.12–3.14 and uv. The project follows the package and tooling conventions
of [harnessed](https://github.com/mathisarends/harnessed).

```sh
uv sync --locked --all-extras
uv run pre-commit install
uv run ruff check .
uv run ruff format --check .
uv run ty check --error-on-warning
uv run pytest
uv run mkdocs build --strict
uv build --no-sources
uv run python scripts/check_wheel.py
uv run twine check --strict dist/*
```

Use FakeClock for timing rules. Prefer tests of public behavior and competing
coordinators over mocks of the event loop. Tests marked `postgres` and the
PostgreSQL store parametrizations require `WAKELINE_POSTGRES_DSN`. CI supplies an
isolated PostgreSQL service. Keep optional driver imports inside their store.

See [development](docs/development.md) for the CI and release workflow, and
[AGENTS.md](AGENTS.md) for repository boundaries.
