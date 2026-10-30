import dataclasses
import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, get_args, get_type_hints

from wakeline.models import Ctx, Result
from wakeline.policies import Catchup, CatchupWindow, Overlap

# Heterogeneous handlers are erased only at the registry boundary; decorators
# preserve the application callable's concrete type.
type Handler = Callable[[Ctx[Any]], Awaitable[Result]]
type Decoder = Callable[[object], object]


@dataclass(frozen=True)
class Kind:
    handler: Handler
    overlap: Overlap
    catchup: Catchup | CatchupWindow | None
    limits: tuple[str, ...]
    pause_on_failure: int | None
    decode: Decoder


def decoder_for(handler: Handler) -> Decoder:
    try:
        annotations = get_type_hints(handler)
    except (NameError, TypeError):
        return lambda value: value
    parameter = next(iter(inspect.signature(handler).parameters), None)
    arguments = get_args(annotations.get(parameter))
    if arguments and dataclasses.is_dataclass(arguments[0]):
        constructor = arguments[0]

        def decode(value: object) -> object:
            if not isinstance(value, dict):
                raise TypeError("Dataclass parameters must be a JSON object")
            return constructor(**value)

        return decode
    return lambda value: value
