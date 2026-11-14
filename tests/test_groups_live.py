"""Consumer groups lease each record, and KV transactions route by {tag},
against a live server: an acked record is gone, an expired lease is read
again with its deliveries counted, a nack past max_deliver is dead, and a key
outside a transaction's {tag} is refused."""

import asyncio
import secrets
import time

import pytest

from flo import (
    FloClient,
    GroupReadResult,
    QueueRefusalReason,
    StreamAppendOptions,
    StreamGroupAckOptions,
    StreamGroupCreateOptions,
    StreamGroupNackOptions,
    StreamGroupReadOptions,
)
from flo.exceptions import BadRequestError, ConflictError, NotFoundError, ValidationError
from flo.types import Ran, Reason


def fresh_namespace() -> str:
    return f"sdk-live-{secrets.token_hex(4)}"


async def _read_until(
    client: FloClient, ns: str, group: str, consumer: str, timeout_s: float
) -> GroupReadResult:
    """Read the group until it hands something out, or timeout_s passes."""
    deadline = time.monotonic() + timeout_s
    while True:
        got = await client.stream.group_read(
            "events", group, consumer, StreamGroupReadOptions(namespace=ns, block_ms=1000)
        )
        if got.records or time.monotonic() > deadline:
            return got


async def test_live_an_acked_record_is_gone_and_an_expired_one_comes_back(
    client: FloClient,
) -> None:
    ns = fresh_namespace()
    created = await client.stream.group_create(
        "events", "g", StreamGroupCreateOptions(namespace=ns, ack_timeout_ms=5000, max_deliver=3)
    )
    assert created is True
    # The same settings again: it exists. Others: refused.
    assert not await client.stream.group_create(
        "events", "g", StreamGroupCreateOptions(namespace=ns, ack_timeout_ms=5000, max_deliver=3)
    )
    with pytest.raises(ConflictError):
        await client.stream.group_create(
            "events", "g", StreamGroupCreateOptions(namespace=ns, ack_timeout_ms=6000)
        )
    for p in (b"one", b"two"):
        await client.stream.append("events", p, StreamAppendOptions(namespace=ns))

    # Read one and ack it within the lease.
    read = StreamGroupReadOptions(namespace=ns, count=1)
    got = await client.stream.group_read("events", "g", "c1", read)
    assert [(r.payload, r.deliveries) for r in got.records] == [(b"one", 1)]
    assert got.term > 0 and got.index > 0 and got.expires_at_ms > 0
    acked = await client.stream.group_ack(
        "events", "g", got, options=StreamGroupAckOptions(namespace=ns)
    )
    assert (acked.applied, acked.refused) == (1, [])
    # Acked again: gone, which is done.
    again = await client.stream.group_ack(
        "events", "g", got, options=StreamGroupAckOptions(namespace=ns)
    )
    assert [r.why for r in again.refused] == [QueueRefusalReason.GONE] and again.lost == []

    # The next read gets the second record, not the acked one; leave it.
    left = await client.stream.group_read("events", "g", "c1", read)
    assert [(r.payload, r.deliveries) for r in left.records] == [(b"two", 1)]
    pending = await client.stream.group_pending("events", "g", namespace=ns)
    assert [(p.consumer, p.delivery_count) for p in pending] == [("c1", 1)]
    info = await client.stream.group_info("events", "g", ns)
    assert (info.leased, info.ready, info.holders) == (1, 0, 1)
    # Nothing new while it is leased.
    nothing = await client.stream.group_read("events", "g", "c2", read)
    assert nothing.records == [] and nothing.expires_at_ms == 0

    # Its 5 s lease runs out: read again, by anyone, with deliveries 2.
    back = await _read_until(client, ns, "g", "c2", timeout_s=20)
    assert [(r.payload, r.deliveries) for r in back.records] == [(b"two", 2)]
    # c1's ack names the old delivery: stale, it applies nothing.
    stale = await client.stream.group_ack(
        "events", "g", left, options=StreamGroupAckOptions(namespace=ns)
    )
    assert stale.applied == 0
    assert [r.why for r in stale.lost] == [QueueRefusalReason.DELIVERED_AGAIN]
    touched = await client.stream.group_touch(
        "events", "g", back, options=StreamGroupAckOptions(namespace=ns)
    )
    assert touched.applied == 1 and touched.expires_at_ms >= back.expires_at_ms
    done = await client.stream.group_ack(
        "events", "g", back, options=StreamGroupAckOptions(namespace=ns)
    )
    assert done.applied == 1
    await client.stream.group_delete("events", "g", ns)


async def test_live_a_record_nacked_past_max_deliver_is_dead(client: FloClient) -> None:
    ns = fresh_namespace()
    await client.stream.group_create(
        "events", "g", StreamGroupCreateOptions(namespace=ns, max_deliver=2)
    )
    await client.stream.append("events", b"bad", StreamAppendOptions(namespace=ns))
    read = StreamGroupReadOptions(namespace=ns, count=10)
    nack = StreamGroupNackOptions(namespace=ns, error="handler failed")
    for deliveries in (1, 2):
        got = await client.stream.group_read("events", "g", "c1", read)
        assert [(r.payload, r.deliveries) for r in got.records] == [(b"bad", deliveries)]
        assert (await client.stream.group_nack("events", "g", got, options=nack)).applied == 1
    # Nacked twice of two attempts: dead, never read again.
    assert (await client.stream.group_read("events", "g", "c1", read)).records == []
    info = await client.stream.group_info("events", "g", ns)
    assert (info.dead, info.ready, info.leased) == (1, 0, 0)
    assert await client.stream.group_delete("events", "g", ns) == 1


async def test_live_a_stream_worker_handles_and_acks_each_record(client: FloClient) -> None:
    ns = fresh_namespace()
    parent = FloClient(client._endpoint, namespace=ns, timeout_ms=35000)
    for i in range(3):
        await client.stream.append("events", f"e{i}".encode(), StreamAppendOptions(namespace=ns))
    seen: list[bytes] = []

    async def handler(ctx) -> None:  # type: ignore[no-untyped-def]
        seen.append(ctx.payload)

    worker = parent.new_stream_worker(
        stream="events",
        group="w",
        handler=handler,
        block_ms=500,
        ack_timeout_ms=10_000,
        auto_touch=True,
    )
    run = asyncio.create_task(worker.start())
    try:
        deadline = time.monotonic() + 10
        while len(seen) < 3 and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        # Acks land on their own connection; wait for them to apply.
        while time.monotonic() < deadline:
            info = await client.stream.group_info("events", "w", ns)
            if info.pending == 0:
                break
            await asyncio.sleep(0.05)
    finally:
        worker.stop()
        await asyncio.wait_for(run, timeout=10)
    assert sorted(seen) == [b"e0", b"e1", b"e2"]
    assert (info.pending, info.dead) == (0, 0)


async def test_live_a_nack_with_a_1024_byte_error_applies(client: FloClient) -> None:
    # The server keeps a nack's error with the record but serves no read of
    # it, so the round trip is the nack applied and the record back.
    ns = fresh_namespace()
    await client.stream.group_create("events", "g", StreamGroupCreateOptions(namespace=ns))
    await client.stream.append("events", b"x", StreamAppendOptions(namespace=ns))
    read = StreamGroupReadOptions(namespace=ns, count=1)
    got = await client.stream.group_read("events", "g", "c1", read)
    error = "é" * 512
    assert len(error.encode("utf-8")) == 1024
    nacked = await client.stream.group_nack(
        "events", "g", got, options=StreamGroupNackOptions(namespace=ns, error=error)
    )
    assert (nacked.applied, nacked.refused) == (1, [])
    back = await client.stream.group_read("events", "g", "c1", read)
    assert [(r.payload, r.deliveries) for r in back.records] == [(b"x", 2)]
    # One byte more is refused before sending.
    with pytest.raises(ValidationError):
        await client.stream.group_nack(
            "events", "g", back, options=StreamGroupNackOptions(namespace=ns, error=error + "x")
        )
    await client.stream.group_delete("events", "g", ns)


async def test_live_abandoned_records_are_ready_at_once_and_never_dead(
    client: FloClient,
) -> None:
    # max_deliver 1: one failure would make the record dead.
    ns = fresh_namespace()
    await client.stream.group_create(
        "events", "g", StreamGroupCreateOptions(namespace=ns, ack_timeout_ms=60_000, max_deliver=1)
    )
    await client.stream.append("events", b"x", StreamAppendOptions(namespace=ns))
    read = StreamGroupReadOptions(namespace=ns, count=1)
    abandon = StreamGroupNackOptions(namespace=ns, abandon=True)
    for deliveries in (1, 2, 3):
        got = await client.stream.group_read("events", "g", "c1", read)
        assert [(r.payload, r.deliveries) for r in got.records] == [(b"x", deliveries)]
        assert (await client.stream.group_nack("events", "g", got, options=abandon)).applied == 1
    info = await client.stream.group_info("events", "g", ns)
    assert (info.ready, info.leased, info.dead) == (1, 0, 0)
    # A nack that fails it is a failure: dead at once at max_deliver 1.
    got = await client.stream.group_read("events", "g", "c1", read)
    assert [r.deliveries for r in got.records] == [4]
    failed = StreamGroupNackOptions(namespace=ns, error="boom")
    assert (await client.stream.group_nack("events", "g", got, options=failed)).applied == 1
    info = await client.stream.group_info("events", "g", ns)
    assert (info.ready, info.dead) == (0, 1)
    await client.stream.group_delete("events", "g", ns)


async def test_live_a_stopped_worker_abandons_its_cut_short_and_unstarted_records(
    client: FloClient,
) -> None:
    ns = fresh_namespace()
    parent = FloClient(client._endpoint, namespace=ns, timeout_ms=35000)
    for i in range(3):
        await client.stream.append("events", f"e{i}".encode(), StreamAppendOptions(namespace=ns))
    started = asyncio.Event()
    gate: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    seen: list[bytes] = []

    async def handler(ctx) -> None:  # type: ignore[no-untyped-def]
        seen.append(ctx.payload)
        started.set()
        await gate  # cancelled by the shutdown below

    # One slot, a 60 s lease and max_deliver 1: records come back at once
    # only if handed back, and undead only if no failure counts. The started
    # one's handler is cancelled while the worker stops, so it is handed back
    # too; the two waiting for a slot never started.
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
        worker.stop()
        gate.cancel()
        await asyncio.wait_for(run, timeout=10)
    finally:
        await parent.close()
    assert seen == [b"e0"]
    read = StreamGroupReadOptions(namespace=ns, count=10)
    back = await client.stream.group_read("events", "w", "c2", read)
    assert sorted((r.payload, r.deliveries) for r in back.records) == [
        (b"e0", 2),
        (b"e1", 2),
        (b"e2", 2),
    ]
    info = await client.stream.group_info("events", "w", ns)
    assert (info.dead, info.leased) == (0, 3)
    await client.stream.group_delete("events", "w", ns)


async def test_live_a_report_to_a_deleted_group_is_not_found(client: FloClient) -> None:
    ns = fresh_namespace()
    await client.stream.group_create("events", "g", StreamGroupCreateOptions(namespace=ns))
    await client.stream.append("events", b"x", StreamAppendOptions(namespace=ns))
    got = await client.stream.group_read(
        "events", "g", "c1", StreamGroupReadOptions(namespace=ns, count=1)
    )
    assert got.records
    await client.stream.group_delete("events", "g", ns)
    with pytest.raises(NotFoundError):
        await client.stream.group_ack(
            "events", "g", got, options=StreamGroupAckOptions(namespace=ns)
        )
    with pytest.raises(NotFoundError):
        await client.stream.group_nack(
            "events", "g", got, options=StreamGroupNackOptions(namespace=ns, abandon=True)
        )
    with pytest.raises(NotFoundError):
        await client.stream.group_touch(
            "events", "g", got, options=StreamGroupAckOptions(namespace=ns)
        )


async def test_live_a_transaction_over_one_tag_and_a_key_outside_it(client: FloClient) -> None:
    tag = secrets.token_hex(4)
    a, b = f"{{{tag}}}a", f"{{{tag}}}b"
    async with await client.kv.begin(a) as txn:
        await txn.put(a, b"1")
        await txn.put(b, b"2")
        committed = await txn.commit()
    assert committed.op_count == 2
    got_a, got_b = await client.kv.get(a), await client.kv.get(b)
    assert got_a is not None and got_a.value == b"1"
    assert got_b is not None and got_b.value == b"2"

    # A key with no {tag} routes by its whole key: not with the transaction's.
    async with await client.kv.begin(a) as txn:
        with pytest.raises(BadRequestError) as raised:
            await txn.put(f"untagged-{tag}", b"x")
        await txn.rollback()
    assert (raised.value.reason, raised.value.ran) == (Reason.KEYS_NOT_COLOCATED, Ran.NO)
    assert await client.kv.get(f"untagged-{tag}") is None
