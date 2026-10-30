# Development

The layout and tooling follow harnessed: a root Python package, typed public
facades, optional adapters, focused tests, offline examples, strict MkDocs,
locked uv dependencies, and pre-commit checks. See the repository CONTRIBUTING.md.

```sh
uv sync --locked --all-extras
uv run ruff check .
uv run ruff format --check .
uv run ty check --error-on-warning
uv run pytest --cov=wakeline --cov-report=term-missing
uv run mkdocs build --strict
uv build --no-sources
uv run python scripts/check_wheel.py
uv run twine check --strict dist/*
```

CI runs Python 3.12, 3.13, and 3.14 on Linux, Windows, and macOS. Each matrix
entry checks types for its target platform, runs tests/examples, builds a wheel,
and verifies it in a fresh environment outside the source checkout. A separate
quality job runs pre-commit and builds the docs. The PostgreSQL job supplies
`WAKELINE_POSTGRES_DSN` from an isolated service and exercises the same store
contract plus competing coordinators.

The release workflow validates main ancestry and `v{project.version}` tags,
calls CI, builds and verifies distributions, then uses a configured PyPI
Trusted Publisher through the `pypi` GitHub environment. Manual workflow runs
only validate/build. No package is published merely by committing this code;
repository environment and Trusted Publisher configuration are external setup.
