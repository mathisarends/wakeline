# Repository guidance

Use the structure and tooling conventions of `harnessed` (locally `../cordify`).
Keep scheduling domain types, runtime coordination, and persistence separate.
The core depends only on the standard library and cronsim. Store extras must not
be imported when importing the core.

Use native Python 3.12 generics, complete public annotations, StrEnum for closed
choices, and explicit public exports. Prefer sibling imports within a package;
use absolute imports across packages. No parent-relative imports. Keep imports
at module scope except optional store dependencies. Use deferred annotations
only for concrete forward references or TYPE_CHECKING dependencies.

Tests should protect scheduling semantics, persistence, races, and public
contracts. Use FakeClock rather than real sleeps for temporal behavior. Never
claim exactly-once external effects: lease loss can only cancel cooperative
async handlers. Lane holders and concurrency limits are process-local.

Run Ruff, ty, pytest, a strict docs build, and the installed-wheel check before
finishing. Commit coherent increments when requested. Do not change adjacent
projects as part of this library's implementation.
