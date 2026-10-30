from collections.abc import Mapping
from typing import TypedDict

from ._time import Duration
from .models import Spec
from .policies import Alive, Catchup, CatchupWindow, Overlap, Poke
from .triggers import Trigger


class Options(TypedDict, total=False):
    labels: Mapping[str, str]
    params: object
    lane: str | None
    yield_to: tuple[str, ...] | list[str]
    max_defer: Duration
    requires: tuple[Alive, ...] | list[Alive]
    overlap: Overlap
    catchup: Catchup | CatchupWindow
    limits: tuple[str, ...] | list[str]
    poke: Poke
    pause_on_failure: int | None


class UpdateFields(Options, total=False):
    trigger: Trigger


class SpecFields(Options):
    kind: str
    key: str
    trigger: Trigger


class PartialSpecFields(UpdateFields, total=False):
    kind: str
    key: str


def build_spec(fields: PartialSpecFields) -> Spec:
    kind, key, trigger = fields.get("kind"), fields.get("key"), fields.get("trigger")
    if kind is None or key is None or trigger is None:
        raise TypeError("ensure requires kind, key, and trigger")
    return Spec(
        kind,
        key,
        trigger,
        labels=fields.get("labels", {}),
        params=fields.get("params"),
        lane=fields.get("lane"),
        yield_to=fields.get("yield_to", ()),
        max_defer=fields.get("max_defer", "10m"),
        requires=fields.get("requires", ()),
        overlap=fields.get("overlap"),
        catchup=fields.get("catchup"),
        limits=fields.get("limits"),
        poke=fields.get("poke", Poke.BUFFER_ONE),
        pause_on_failure=fields.get("pause_on_failure"),
    )
