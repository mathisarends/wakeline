from typing import get_type_hints

import wakeline
from wakeline.stores import MemoryStore, PostgresStore, SqliteStore, Store, Stored


def test_store_list_annotation_resolves_on_python_314() -> None:
    # Python 3.14 defers annotations inside class scope; a method named list
    # must not shadow builtins.list in its own public return annotation.
    for store in (Store, MemoryStore, SqliteStore, PostgresStore):
        assert get_type_hints(store.list)["return"] == list[Stored]


def test_root_facade_exports_are_explicit_and_unique() -> None:
    assert len(wakeline.__all__) == len(set(wakeline.__all__))
    for name in wakeline.__all__:
        assert getattr(wakeline, name) is not None
