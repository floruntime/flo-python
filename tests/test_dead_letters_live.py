"""Dead letters of queues and consumer groups, the group steal, and stream
settings, against a live server: a dead letter lists with its error and
failures, pages by cursor, is requeued or deleted by id or all, and one not
dead is refused NOT_DEAD; a steal takes an idle holder's records and refuses
NOT_IDLE ones; an alter works on a stream an append made, and clears a
retention bound."""

import asyncio
import secrets

import pytest

from flo import (
    DequeueOptions,
    DlqCursor,
    DlqDeleteOptions,
    DlqListOptions,
    DlqRequeueOptions,
    EnqueueOptions,
    FloClient,
    GroupDlqDeleteOptions,
    GroupDlqListOptions,
    GroupDlqRequeueOptions,
    PeekOptions,
    QueueFailOptions,
    QueueRefusalReason,
    StreamAlterOptions,
    StreamAppendOptions,
    StreamCreateOptions,
    StreamGroupAckOptions,
    StreamGroupCreateOptions,
    StreamGroupNackOptions,
    StreamGroupReadOptions,
    StreamGroupStealOptions,
    StreamID,
    StreamInfoOptions,
)
from flo.exceptions import BadRequestError, ConflictError, NotFoundError, ValidationError
from flo.types import OpCode, OptionTag
from flo.wire import OptionsBuilder


def fresh_namespace() -> str:
    return f"sdk-live-{secrets.token_hex(4)}"


async def _dead_queue(client: FloClient, ns: str, bodies: list[bytes]) -> list[int]:
    """Enqueue bodies to "jobs" and fail each dead, in order; their seqs."""
    seqs = []
    for body in bodies:
        seqs.append(await client.queue.enqueue("jobs", body, EnqueueOptions(namespace=ns)))
        got = await client.queue.dequeue("jobs", 1, DequeueOptions(namespace=ns, consumer="w"))
        assert [m.payload for m in got.messages] == [body]
        failed = await client.queue.fail(
            "jobs", got, options=QueueFailOptions(namespace=ns, dead=True, error=f"bad {body!r}")
        )
        assert (failed.applied, failed.refused) == (1, [])
    return seqs


async def test_live_queue_dead_letters_list_page_and_requeue(client: FloClient) -> None:
    ns = fresh_namespace()
    seqs = await _dead_queue(client, ns, [b"a", b"b", b"c"])

    first = await client.queue.dlq_list("jobs", DlqListOptions(namespace=ns, limit=2))
    assert [(d.seq, d.payload, d.size, d.error) for d in first.messages] == [
        (seqs[0], b"a", 1, "bad b'a'"),
        (seqs[1], b"b", 1, "bad b'b'"),
    ]
    assert all(d.deliveries == 1 and d.failures == 1 for d in first.messages)
    assert all(d.dead_at_ms >= d.enqueued_at_ms > 0 for d in first.messages)
    assert isinstance(first.cursor, DlqCursor)
    rest = await client.queue.dlq_list(
        "jobs", DlqListOptions(namespace=ns, limit=2, after=first.cursor)
    )
    assert ([d.seq for d in rest.messages], rest.cursor) == ([seqs[2]], None)

    # Requeue one by seq: ready again at once, failures reset, error kept.
    requeued = await client.queue.dlq_requeue("jobs", [seqs[0]], DlqRequeueOptions(namespace=ns))
    assert (requeued.applied, requeued.refused, requeued.more) == (1, [], False)
    ready = await client.queue.peek("jobs", 10, PeekOptions(namespace=ns))
    assert [m.seq for m in ready.messages] == [seqs[0]]

    # It isn't dead now: requeue and delete refuse it NOT_DEAD, applying
    # nothing; a seq the queue never had is GONE.
    again = await client.queue.dlq_requeue(
        "jobs", [seqs[0], 999_999], DlqRequeueOptions(namespace=ns)
    )
    assert again.applied == 0
    assert [(r.seq, r.why) for r in again.refused] == [
        (seqs[0], QueueRefusalReason.NOT_DEAD),
        (999_999, QueueRefusalReason.GONE),
    ]
    assert again.refused[0].reason.startswith("not dead")
    not_dead = await client.queue.dlq_delete("jobs", [seqs[0]], DlqDeleteOptions(namespace=ns))
    assert [(r.seq, r.why) for r in not_dead.refused] == [(seqs[0], QueueRefusalReason.NOT_DEAD)]

    # All, a batch of one at a time: more says another remains.
    batch = await client.queue.dlq_requeue(
        "jobs", options=DlqRequeueOptions(namespace=ns, all=True, limit=1)
    )
    assert (batch.applied, batch.more) == (1, True)
    last = await client.queue.dlq_requeue(
        "jobs", options=DlqRequeueOptions(namespace=ns, all=True, limit=1)
    )
    assert (last.applied, last.more) == (1, False)
    empty = await client.queue.dlq_list("jobs", DlqListOptions(namespace=ns))
    assert (empty.messages, empty.cursor) == ([], None)


async def test_live_queue_dead_letters_delete_and_dry_run(client: FloClient) -> None:
    ns = fresh_namespace()
    seqs = await _dead_queue(client, ns, [b"x", b"y"])

    dry = await client.queue.dlq_delete(
        "jobs", options=DlqDeleteOptions(namespace=ns, all=True, dry_run=True)
    )
    assert (dry.applied, dry.refused, dry.more) == (2, [], False)
    assert len((await client.queue.dlq_list("jobs", DlqListOptions(namespace=ns))).messages) == 2

    one = await client.queue.dlq_delete("jobs", [seqs[0]], DlqDeleteOptions(namespace=ns))
    assert (one.applied, one.refused) == (1, [])
    # Deleted for good: gone, not dead.
    gone = await client.queue.dlq_delete("jobs", [seqs[0]], DlqDeleteOptions(namespace=ns))
    assert [(r.seq, r.why) for r in gone.refused] == [(seqs[0], QueueRefusalReason.GONE)]
    rest = await client.queue.dlq_delete("jobs", options=DlqDeleteOptions(namespace=ns, all=True))
    assert (rest.applied, rest.more) == (1, False)
    assert (await client.queue.dlq_list("jobs", DlqListOptions(namespace=ns))).messages == []
    # Nothing ready either: deleted, not requeued.
    assert (await client.queue.peek("jobs", 10, PeekOptions(namespace=ns))).messages == []


async def test_live_queue_dead_letter_refusals(client: FloClient) -> None:
    ns = fresh_namespace()
    await client.queue.enqueue("jobs", b"x", EnqueueOptions(namespace=ns))
    with pytest.raises(NotFoundError):
        await client.queue.dlq_list("nope", DlqListOptions(namespace=ns))
    with pytest.raises(NotFoundError):
        await client.queue.dlq_requeue("nope", [1], DlqRequeueOptions(namespace=ns))
    # The server refuses neither seqs nor all, as the client does first.
    with pytest.raises(BadRequestError):
        await client._send_and_check(
            0x186, ns, b"jobs", b"\x00" + b"\x01\x00" + b"\x01" + (100).to_bytes(4, "little")
        )


async def _group_with_dead(client: FloClient, ns: str, count: int) -> list[StreamID]:
    """A group "g" on "events" whose first `count` records are dead."""
    await client.stream.group_create(
        "events", "g", StreamGroupCreateOptions(namespace=ns, ack_timeout_ms=60_000, max_deliver=1)
    )
    for i in range(count + 1):
        await client.stream.append("events", f"e{i}".encode(), StreamAppendOptions(namespace=ns))
    got = await client.stream.group_read(
        "events", "g", "c1", StreamGroupReadOptions(namespace=ns, count=count)
    )
    assert len(got.records) == count
    nacked = await client.stream.group_nack(
        "events", "g", got, options=StreamGroupNackOptions(namespace=ns, error="handler failed")
    )
    assert nacked.applied == count
    return [r.id for r in got.records]


async def test_live_group_dead_letters_list_requeue_and_delete(client: FloClient) -> None:
    ns = fresh_namespace()
    ids = await _group_with_dead(client, ns, 3)

    first = await client.stream.group_dlq_list(
        "events", "g", GroupDlqListOptions(namespace=ns, limit=2)
    )
    assert [(d.id, d.deliveries, d.failures, d.error) for d in first.dead] == [
        (ids[0], 1, 1, "handler failed"),
        (ids[1], 1, 1, "handler failed"),
    ]
    assert first.cursor is not None
    rest = await client.stream.group_dlq_list(
        "events", "g", GroupDlqListOptions(namespace=ns, after=first.cursor)
    )
    assert ([d.id for d in rest.dead], rest.cursor) == ([ids[2]], None)

    requeued = await client.stream.group_dlq_requeue(
        "events", "g", [ids[0]], GroupDlqRequeueOptions(namespace=ns)
    )
    assert (requeued.applied, requeued.refused) == (1, [])
    # Ready again: the next read takes it, a new delivery.
    again = await client.stream.group_read(
        "events", "g", "c2", StreamGroupReadOptions(namespace=ns, count=1)
    )
    assert [(r.id, r.deliveries) for r in again.records] == [(ids[0], 2)]

    # Leased now, not dead: refused NOT_DEAD by requeue and delete alike.
    for refused in (
        await client.stream.group_dlq_requeue(
            "events", "g", [ids[0]], GroupDlqRequeueOptions(namespace=ns)
        ),
        await client.stream.group_dlq_delete(
            "events", "g", [ids[0]], GroupDlqDeleteOptions(namespace=ns)
        ),
    ):
        assert refused.applied == 0
        assert [(r.id, r.why) for r in refused.refused] == [(ids[0], QueueRefusalReason.NOT_DEAD)]
        assert refused.refused[0].reason.startswith("not dead")

    dry = await client.stream.group_dlq_delete(
        "events", "g", options=GroupDlqDeleteOptions(namespace=ns, all=True, dry_run=True)
    )
    assert (dry.applied, dry.more) == (2, False)
    batch = await client.stream.group_dlq_delete(
        "events", "g", options=GroupDlqDeleteOptions(namespace=ns, all=True, limit=1)
    )
    assert (batch.applied, batch.more) == (1, True)
    named = await client.stream.group_dlq_delete(
        "events", "g", [ids[2]], GroupDlqDeleteOptions(namespace=ns)
    )
    assert (named.applied, named.refused) == (1, [])
    left = await client.stream.group_dlq_list("events", "g", GroupDlqListOptions(namespace=ns))
    assert (left.dead, left.cursor) == ([], None)


async def test_live_group_dead_letters_of_no_group_are_not_found(client: FloClient) -> None:
    ns = fresh_namespace()
    await client.stream.append("events", b"e", StreamAppendOptions(namespace=ns))
    with pytest.raises(NotFoundError):
        await client.stream.group_dlq_list("events", "nope", GroupDlqListOptions(namespace=ns))


async def test_live_a_steal_takes_an_idle_holders_records(client: FloClient) -> None:
    ns = fresh_namespace()
    await client.stream.group_create(
        "events", "g", StreamGroupCreateOptions(namespace=ns, ack_timeout_ms=60_000)
    )
    for p in (b"one", b"two"):
        await client.stream.append("events", p, StreamAppendOptions(namespace=ns))
    held = await client.stream.group_read(
        "events", "g", "stuck", StreamGroupReadOptions(namespace=ns, count=2)
    )
    assert len(held.records) == 2

    # Not idle long enough: refused NOT_IDLE, nothing taken.
    early = await client.stream.group_steal(
        "events", "g", "rescuer", held.records, 60_000, StreamGroupStealOptions(namespace=ns)
    )
    assert (early.records, early.expires_at_ms) == ([], 0)
    assert [(r.id, r.why) for r in early.refused] == [
        (rec.id, QueueRefusalReason.NOT_IDLE) for rec in held.records
    ]
    assert early.refused[0].reason.startswith("not idle")

    await asyncio.sleep(0.3)
    stolen = await client.stream.group_steal(
        "events",
        "g",
        "rescuer",
        held.records[:1],
        100,
        StreamGroupStealOptions(namespace=ns, lease_ms=10_000),
    )
    assert [(r.id, r.deliveries) for r in stolen.records] == [(held.records[0].id, 2)]
    assert stolen.refused == [] and stolen.expires_at_ms > 0 and stolen.consumer == "rescuer"
    members = await client.stream.group_members("events", "g", namespace=ns)
    assert {m.id: m.pending for m in members.members} == {"stuck": 1, "rescuer": 1}

    # The old holder's ack is stale; the stealer's applies.
    stale = await client.stream.group_ack(
        "events", "g", held, held.records[:1], StreamGroupAckOptions(namespace=ns)
    )
    assert [r.why for r in stale.refused] == [QueueRefusalReason.DELIVERED_AGAIN]
    acked = await client.stream.group_ack(
        "events", "g", stolen, options=StreamGroupAckOptions(namespace=ns)
    )
    assert (acked.applied, acked.refused) == (1, [])
    # Stealing what the holder no longer has: delivered again.
    late = await client.stream.group_steal(
        "events", "g", "rescuer", held.records[:1], 1, StreamGroupStealOptions(namespace=ns)
    )
    assert [r.why for r in late.refused] == [QueueRefusalReason.GONE]


async def test_live_an_alter_works_on_a_stream_an_append_made(client: FloClient) -> None:
    ns = fresh_namespace()
    await client.stream.append("made", b"r", StreamAppendOptions(namespace=ns))
    await client.stream.alter("made", StreamAlterOptions(namespace=ns, retention_count=500))
    info = await client.stream.info("made", StreamInfoOptions(namespace=ns))
    assert (info.count, info.retention_count, info.retention_age_s) == (1, 500, 0)
    await client.stream.alter("made", StreamAlterOptions(namespace=ns, retention_age_s=3600))
    info = await client.stream.info("made", StreamInfoOptions(namespace=ns))
    assert (info.retention_count, info.retention_age_s) == (500, 3600)

    # Now it has settings: a create must name the same ones.
    with pytest.raises(ConflictError, match="other settings"):
        await client.stream.create("made", StreamCreateOptions(namespace=ns))
    await client.stream.create(
        "made", StreamCreateOptions(namespace=ns, retention_count=500, retention_age_s=3600)
    )
    # An alter of a stream that doesn't exist is not found.
    with pytest.raises(NotFoundError):
        await client.stream.alter("never", StreamAlterOptions(namespace=ns, retention_count=1))


async def test_live_a_create_gives_an_append_made_stream_its_settings(client: FloClient) -> None:
    ns = fresh_namespace()
    await client.stream.append("s", b"r", StreamAppendOptions(namespace=ns))
    await client.stream.create("s", StreamCreateOptions(namespace=ns, retention_count=10))
    info = await client.stream.info("s", StreamInfoOptions(namespace=ns))
    assert (info.count, info.retention_count) == (1, 10)
    # The same again is a retry; others are refused.
    await client.stream.create("s", StreamCreateOptions(namespace=ns, retention_count=10))
    with pytest.raises(ConflictError):
        await client.stream.create("s", StreamCreateOptions(namespace=ns, retention_count=11))


async def test_live_an_alter_clears_a_retention_bound(client: FloClient) -> None:
    ns = fresh_namespace()
    await client.stream.create(
        "s", StreamCreateOptions(namespace=ns, retention_age_s=3600, retention_count=500)
    )
    # Neither bound given, or both 0: unchanged.
    await client.stream.alter("s", StreamAlterOptions(namespace=ns))
    info = await client.stream.info("s", StreamInfoOptions(namespace=ns))
    assert (info.retention_age_s, info.retention_count) == (3600, 500)

    await client.stream.alter("s", StreamAlterOptions(namespace=ns, clear_retention_age=True))
    info = await client.stream.info("s", StreamInfoOptions(namespace=ns))
    assert (info.retention_age_s, info.retention_count) == (0, 500)
    await client.stream.alter("s", StreamAlterOptions(namespace=ns, clear_retention_count=True))
    info = await client.stream.info("s", StreamInfoOptions(namespace=ns))
    assert (info.retention_age_s, info.retention_count) == (0, 0)


async def test_live_setting_and_clearing_one_bound_is_refused(client: FloClient) -> None:
    ns = fresh_namespace()
    await client.stream.create("s", StreamCreateOptions(namespace=ns, retention_count=500))
    # The SDK refuses it before sending; the server refuses it too.
    with pytest.raises(ValidationError):
        await client.stream.alter(
            "s", StreamAlterOptions(namespace=ns, retention_count=5, clear_retention_count=True)
        )
    for tag in (OptionTag.RETENTION_COUNT, OptionTag.RETENTION_AGE):
        clear = (
            OptionTag.RETENTION_COUNT_CLEAR
            if tag == OptionTag.RETENTION_COUNT
            else OptionTag.RETENTION_AGE_CLEAR
        )
        options = OptionsBuilder().add_u64(tag, 5).add_flag(clear).build()
        with pytest.raises(BadRequestError, match="both sets and clears"):
            await client._send_and_check(
                OpCode.STREAM_ALTER, client.get_namespace(ns), b"s", b"", options
            )
    info = await client.stream.info("s", StreamInfoOptions(namespace=ns))
    assert (info.retention_age_s, info.retention_count) == (0, 500)


async def test_live_a_create_with_a_clear_is_refused(client: FloClient) -> None:
    # StreamCreateOptions has no clear; sent by hand, the server refuses it.
    ns = fresh_namespace()
    for clear in (OptionTag.RETENTION_AGE_CLEAR, OptionTag.RETENTION_COUNT_CLEAR):
        options = OptionsBuilder().add_flag(clear).build()
        with pytest.raises(BadRequestError):
            await client._send_and_check(
                OpCode.STREAM_CREATE, client.get_namespace(ns), b"s", b"", options
            )


async def test_live_a_retention_count_of_u64_max_is_refused(client: FloClient) -> None:
    ns = fresh_namespace()
    top = 0xFFFFFFFFFFFFFFFF
    with pytest.raises(BadRequestError, match="retention bound too large"):
        await client.stream.create("s", StreamCreateOptions(namespace=ns, retention_count=top))
    await client.stream.create("s", StreamCreateOptions(namespace=ns, retention_count=500))
    with pytest.raises(BadRequestError, match="retention bound too large"):
        await client.stream.alter("s", StreamAlterOptions(namespace=ns, retention_count=top))
    # An age that large is past the 100-year cap, refused for that.
    with pytest.raises(BadRequestError, match="over 100 years"):
        await client.stream.alter("s", StreamAlterOptions(namespace=ns, retention_age_s=top))
    info = await client.stream.info("s", StreamInfoOptions(namespace=ns))
    assert (info.retention_age_s, info.retention_count) == (0, 500)
