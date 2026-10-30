import dataclasses
import hashlib
import json
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from wakeline.models import Cause, Outcome, Run, Spec, State
from wakeline.policies import Alive, Catchup, CatchupWindow, Overlap, Poke
from wakeline.triggers import Cron, Every, Once

from .state import Cell, Instance, Lease, Request


def _default(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return dict(value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {field.name: getattr(value, field.name) for field in dataclasses.fields(value)}
    raise TypeError(
        f"Parameters must be JSON serializable or dataclasses, got {type(value).__name__}"
    )


def spec_data(spec: Spec) -> dict[str, Any]:
    # Reflection and Any are confined to this serialization boundary. Never import
    # types named in persisted data; parameter reconstruction is registered in code.
    result = {field.name: getattr(spec, field.name) for field in dataclasses.fields(spec)}
    trigger = {
        field.name: getattr(spec.trigger, field.name) for field in dataclasses.fields(spec.trigger)
    }
    trigger["type"] = type(spec.trigger).__name__
    result["trigger"] = trigger
    return json.loads(json.dumps(result, default=_default, sort_keys=True, allow_nan=False))


def decode_spec(data: dict[str, Any]) -> Spec:
    data = dict(data)
    trigger = dict(data.pop("trigger"))
    name = trigger.pop("type")
    for key in ("at", "until"):
        if trigger.get(key) is not None:
            trigger[key] = datetime.fromisoformat(trigger[key])
    if trigger.get("quiet") is not None:
        trigger["quiet"] = tuple(trigger["quiet"])
    constructors = {"Every": Every, "Cron": Cron, "Once": Once}
    data["trigger"] = constructors[name](**trigger)
    data["requires"] = tuple(Alive(**item) for item in data["requires"])
    data["overlap"] = None if data["overlap"] is None else Overlap(data["overlap"])
    data["poke"] = Poke(data["poke"])
    catchup = data["catchup"]
    data["catchup"] = (
        CatchupWindow(**catchup)
        if isinstance(catchup, dict)
        else None
        if catchup is None
        else Catchup(catchup)
    )
    return Spec(**data)


def spec_hash(spec: Spec) -> str:
    return hashlib.sha256(json.dumps(spec_data(spec), sort_keys=True).encode()).hexdigest()


def encode(cell: Cell) -> str:
    data = {"format": 1, "instance": None, "lease": cell.lease, "runs": cell.runs}
    if cell.instance is not None:
        data["instance"] = {
            field.name: getattr(cell.instance, field.name) for field in dataclasses.fields(Instance)
        }
        data["instance"]["spec"] = spec_data(cell.instance.spec)
    return json.dumps(data, default=_default, sort_keys=True, allow_nan=False)


def decode(value: str) -> Cell:
    data = json.loads(value)
    if data["format"] != 1:
        raise ValueError(f"Unsupported wakeline store format: {data['format']}")
    raw = data["instance"]
    instance = None
    if raw is not None:
        raw["spec"] = decode_spec(raw["spec"])
        for name in (
            "nominal",
            "next_trigger",
            "rearm_at",
            "last_run",
            "last_success",
            "defer_since",
        ):
            if raw[name] is not None:
                raw[name] = datetime.fromisoformat(raw[name])
        raw["state"] = State(raw["state"])
        raw["pending"] = [
            Request(datetime.fromisoformat(r["scheduled_for"]), Cause(r["cause"]), r["reason"])
            for r in raw["pending"]
        ]
        instance = Instance(**raw)
    lease = data["lease"]
    lease = None if lease is None else Lease(lease["owner"], datetime.fromisoformat(lease["until"]))
    runs = []
    for run in data["runs"]:
        for name in ("scheduled_for", "fired_at", "finished_at"):
            if run[name] is not None:
                run[name] = datetime.fromisoformat(run[name])
        run["cause"] = Cause(run["cause"])
        run["outcome"] = Outcome(run["outcome"])
        runs.append(Run(**run))
    return Cell(instance, lease, runs)
