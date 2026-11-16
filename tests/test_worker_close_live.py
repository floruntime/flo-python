"""close() with work in flight, against a live server: the running
handler's complete or ack is sent before the connections close, and a
stream worker's unstarted records are handed back."""

import asyncio
import secrets

from flo import (
    ActionInvokeOptions,
    ActionRegisterOptions,
    ActionStatusOptions,
    ActionType,
    FloClient,
    RunStatus,
    StreamAppendOptions,
    StreamGroupReadOptions,
)


def fresh_namespace() -> str:
    return f"sdk-live-{secrets.token_hex(4)}"


async def test_live_action_worker_close_sends_the_running_actions_complete(
    client: FloClient,
) -> None:
    ns = fresh_namespace()
    await client.action.register(
        "slow", ActionType.USER, ActionRegisterOptions(namespace=ns, timeout_ms=30_000)
    )
    parent = FloClient(client._endpoint, namespace=ns)
    started = asyncio.Event()

    async def slow(ctx) -> bytes:  # type: ignore[no-untyped-def]
        started.set()
        await asyncio.sleep(0.5)
        return b"done"

    worker = parent.new_action_worker(block_ms=1000)
    worker.register_action("slow", slow)
    run = asyncio.create_task(worker.start())
    try:
        invoked = await client.action.invoke("slow", b"", ActionInvokeOptions(namespace=ns))
        await asyncio.wait_for(started.wait(), 10)
        await asyncio.wait_for(worker.close(), 10)
        assert run.done()
        await run
    finally:
        await parent.close()
    status = await client.action.status(invoked.run_id, ActionStatusOptions(namespace=ns))
    assert (status.status, status.output) == (RunStatus.COMPLETED, b"done")


async def test_live_stream_worker_close_acks_the_running_and_hands_back_the_rest(
    client: FloClient,
) -> None:
    ns = fresh_namespace()
    for i in range(3):
        await client.stream.append("events", f"e{i}".encode(), StreamAppendOptions(namespace=ns))
    parent = FloClient(client._endpoint, namespace=ns, timeout_ms=35000)
    started = asyncio.Event()
    seen: list[bytes] = []

    async def handler(ctx) -> None:  # type: ignore[no-untyped-def]
        seen.append(ctx.payload)
        started.set()
        await asyncio.sleep(0.5)

    # One slot, a 60 s lease and max_deliver 1: the two waiting for a slot
    # come back at once only if handed back, and undead only if no failure
    # counts.
    worker = parent.new_stream_worker(
        stream="events",
        group="w",
        handler=handler,
        concurrency=1,
        batch_size=3,
        block_ms=500,
        ack_timeout_ms=60_000,
        max_deliver=1,
    )
    run = asyncio.create_task(worker.start())
    try:
        await asyncio.wait_for(started.wait(), 10)
        await asyncio.wait_for(worker.close(), 10)
        assert run.done()
        await run
    finally:
        await parent.close()
    assert seen == [b"e0"]
    back = await client.stream.group_read(
        "events", "w", "c2", StreamGroupReadOptions(namespace=ns, count=10)
    )
    assert sorted((r.payload, r.deliveries) for r in back.records) == [(b"e1", 2), (b"e2", 2)]
    info = await client.stream.group_info("events", "w", ns)
    assert (info.dead, info.leased) == (0, 2)
    await client.stream.group_delete("events", "w", ns)
