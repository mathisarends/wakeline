import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Self

from wakeline.clock import Clock
from wakeline.models import Cause
from wakeline.policies import Catchup, CatchupWindow
from wakeline.triggers import Cron, Every, Once, Trigger, following, outside_quiet

from .state import Instance, Request


def deadline(trigger: Trigger, nominal: datetime | None, key: str) -> datetime | None:
    if nominal is None:
        return None
    jitter = 0.0 if isinstance(trigger, Once) else trigger.jitter
    # Stable jitter avoids resampling across restarts and competing coordinators.
    fraction = (
        int.from_bytes(hashlib.sha256(f"{key}:{nominal.isoformat()}".encode()).digest()[:8]) / 2**64
    )
    return outside_quiet(nominal + timedelta(seconds=jitter * fraction), trigger.quiet, trigger.tz)


@dataclass
class Timeline:
    generation: str
    trigger_at: datetime | None
    trigger_mono: float | None
    rearm_at: datetime | None
    rearm_mono: float | None

    @classmethod
    def build(cls, instance: Instance, clock: Clock) -> Self:
        result = cls(instance.generation, None, None, None, None)
        result.sync(instance, clock)
        return result

    def sync(self, instance: Instance, clock: Clock) -> None:
        mono, now = clock.monotonic(), clock.now()
        trigger = instance.spec.trigger
        relative = isinstance(trigger, Every) or (
            isinstance(trigger, Once) and trigger.after is not None
        )
        if instance.next_trigger is None:
            self.trigger_mono = None
        elif relative and self.trigger_mono is not None and self.trigger_at is not None:
            self.trigger_mono += (instance.next_trigger - self.trigger_at).total_seconds()
        else:
            self.trigger_mono = mono + (instance.next_trigger - now).total_seconds()
        self.trigger_at = instance.next_trigger
        if instance.rearm_at is None:
            self.rearm_mono = None
        elif (
            instance.rearm_relative
            and self.rearm_at == instance.rearm_at
            and self.rearm_mono is not None
        ):
            pass
        else:
            self.rearm_mono = mono + (instance.rearm_at - now).total_seconds()
        self.rearm_at = instance.rearm_at

    def next(self, instance: Instance, clock: Clock) -> float | None:
        if instance.pending:
            return clock.monotonic()
        deadlines = [d for d in (self.trigger_mono, self.rearm_mono) if d is not None]
        return min(deadlines) if deadlines else None

    def collect(self, instance: Instance, clock: Clock) -> None:
        """Consume due trigger slots and a one-shot rearm, coalescing their first run."""
        now_mono = clock.monotonic()
        requests: list[Request] = []
        trigger = instance.spec.trigger
        policy = instance.spec.catchup
        if self.trigger_mono is not None and self.trigger_mono <= now_mono:
            assert instance.next_trigger is not None
            first_due = instance.next_trigger
            # For monotonic intervals, elapsed time is independent of wall-clock jumps.
            effective_now = first_due + timedelta(seconds=now_mono - self.trigger_mono)
            if isinstance(trigger, Every) and trigger.quiet is None and trigger.jitter == 0:
                count = int((effective_now - first_due).total_seconds() // trigger.interval) + 1
                if trigger.until is not None:
                    count = min(
                        count,
                        int((trigger.until - first_due).total_seconds() // trigger.interval) + 1,
                    )
                due = [
                    first_due + timedelta(seconds=trigger.interval * i)
                    for i in range(count if policy == Catchup.ALL else 1)
                ]
                latest = first_due + timedelta(seconds=trigger.interval * (count - 1))
                instance.nominal = first_due + timedelta(seconds=trigger.interval * count)
                if trigger.until is not None and instance.nominal > trigger.until:
                    instance.nominal = None
                instance.next_trigger = deadline(trigger, instance.nominal, instance.spec.key)
            else:
                due = []
                latest = first_due
                if policy != Catchup.ALL and isinstance(trigger, Cron):
                    # Seek near now, then evaluate forward so cronsim retains its
                    # DST fixups. Reverse iteration has different gap semantics.
                    window = policy.seconds if isinstance(policy, CatchupWindow) else 0
                    dst = 3 * 3600 if len(trigger.expression.split()) == 5 else 1
                    lookback = max(window, dst) + trigger.jitter + 1
                    if trigger.quiet is not None:
                        lookback += 27 * 3600
                    anchor = effective_now - timedelta(seconds=lookback)
                    if instance.nominal is not None and instance.nominal < anchor:
                        instance.nominal = following(trigger, anchor)
                        instance.next_trigger = deadline(
                            trigger, instance.nominal, instance.spec.key
                        )
                while instance.next_trigger is not None and instance.next_trigger <= effective_now:
                    latest = instance.next_trigger
                    due.append(latest)
                    assert instance.nominal is not None
                    instance.nominal = following(trigger, instance.nominal)
                    instance.next_trigger = deadline(trigger, instance.nominal, instance.spec.key)
            if policy == Catchup.ALL:
                requests.extend(Request(at, Cause.TRIGGER) for at in due)
            elif policy == Catchup.NONE:
                if (effective_now - latest).total_seconds() <= 0.001:
                    requests.append(Request(latest, Cause.TRIGGER))
            elif isinstance(policy, CatchupWindow):
                if (effective_now - latest).total_seconds() <= policy.seconds:
                    requests.append(Request(latest, Cause.TRIGGER))
            else:
                requests.append(Request(first_due, Cause.TRIGGER))
        if self.rearm_mono is not None and self.rearm_mono <= now_mono:
            assert instance.rearm_at is not None
            if not requests:
                requests.append(Request(instance.rearm_at, Cause.REARM))
            instance.rearm_at = None
        elif requests:
            # The earlier trigger wins; a later handler rearm is consumed with it.
            instance.rearm_at = None
        instance.pending.extend(requests)
