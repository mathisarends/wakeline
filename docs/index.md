# wakeline

wakeline coordinates time-driven async work. A **kind** contains an async handler
and policy defaults. An **instance** contains a key, trigger, labels, parameters,
and coordination rules. Many instances can share one kind.

The coordinator owns one timing loop and a heap of deadlines. Tasks exist only
for handlers that are executing, brief poke operations, and the loop's wait.

```python
from wakeline import Coordinator, Ctx, FakeClock, every

clock = FakeClock()
coord = Coordinator(clock=clock)


@coord.kind("check")
async def check(ctx: Ctx[None]) -> None:
    print(ctx.key)


async def example() -> None:
    await coord.ensure(kind="check", key="check/one", trigger=every(minutes=10))
    await clock.advance("10m")
    assert coord.ledger.last("check/one").outcome == "ok"
    await coord.stop()
```

With real time, use `async with coord:` or pair `await coord.start()` and
`await coord.stop()`. Register kinds first. Management operations also work before
start; FakeClock can drive these instances without a background task.

Start with [coordination](coordination.md), then read
[persistence](persistence.md) and [testing](testing.md). The
[reference](reference.md) lists the public API. Application-specific schedulers
belong in your application; see [integration](integration.md).
