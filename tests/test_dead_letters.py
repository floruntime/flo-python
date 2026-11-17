"""The dead letter ops of queues and consumer groups, and the group steal:
bodies checked byte for byte against the server's schemas, answers decoded
whole, and the requests sent as table ops."""

import asyncio
import struct

import pytest

from flo import (
    DlqCursor,
    DlqDeleteOptions,
    DlqListOptions,
    DlqRequeueOptions,
    FloClient,
    GroupDlqCursor,
    GroupDlqDeleteOptions,
    GroupDlqListOptions,
    StreamGroupStealOptions,
)
from flo.exceptions import IncompleteResponseError, ValidationError
from flo.kv_body import (
    decode_group_dlq_answer,
    decode_group_dlq_list_answer,
    decode_group_steal_answer,
    decode_queue_dlq_answer,
    decode_queue_dlq_list_answer,
    decode_queue_report_answer,
    encode_group_dlq_body,
    encode_group_dlq_list_body,
    encode_group_steal_body,
    encode_queue_dlq_body,
    encode_queue_dlq_list_body,
    is_table_op,
)
from flo.types import (
    HEADER_SIZE,
    GroupRecord,
    OpCode,
    PendingEntry,
    QueueRefusalReason,
    StatusCode,
    StreamID,
)
from flo.wire import REQUEST_HEADER_FORMAT

from .test_result_resend import _str16
from .test_retry import _frame, _Scripted


def _str8(s: str) -> bytes:
    return bytes([len(s)]) + s.encode()


def test_the_ops_have_the_servers_codes_and_carry_bodies() -> None:
    codes = {
        OpCode.STREAM_GROUP_STEAL: 0x15F,
        OpCode.STREAM_GROUP_DLQ_LIST: 0x160,
        OpCode.STREAM_GROUP_DLQ_REQUEUE: 0x161,
        OpCode.STREAM_GROUP_DLQ_DELETE: 0x162,
        OpCode.QUEUE_DLQ_LIST: 0x185,
        OpCode.QUEUE_DLQ_REQUEUE: 0x186,
        OpCode.QUEUE_DLQ_DELETE: 0x187,
    }
    for op, code in codes.items():
        assert int(op) == code
        assert is_table_op(op)
    # The hand-coded dead letter ops are gone.
    for code in (0x176, 0x177, 0x178):
        with pytest.raises(ValueError):
            OpCode(code)


def test_the_refusal_reasons_include_not_dead_and_not_idle() -> None:
    assert QueueRefusalReason(6) is QueueRefusalReason.NOT_DEAD
    assert QueueRefusalReason(7) is QueueRefusalReason.NOT_IDLE
    # A queue report answer carrying them decodes, labelled as the server said.
    data = (
        struct.pack("<I", 0)
        + b"\x02"
        + struct.pack("<QB", 4, 6)
        + _str8("not dead")
        + struct.pack("<QB", 5, 7)
        + _str8("not idle")
    )
    r = decode_queue_report_answer("queue.complete", data)
    assert [(x.seq, x.why, x.reason) for x in r.refused] == [
        (4, QueueRefusalReason.NOT_DEAD, "not dead"),
        (5, QueueRefusalReason.NOT_IDLE, "not idle"),
    ]
    with pytest.raises(IncompleteResponseError):
        decode_queue_report_answer("queue.complete", data[:-10] + b"\x08" + data[-9:])


# ── Queue dead letters ──────────────────────────────────────────────────────


def test_a_queue_dlq_list_body_is_its_limit_then_its_cursor() -> None:
    assert encode_queue_dlq_list_body(20, None) == b"\x01" + struct.pack("<I", 20) + b"\x00"
    assert encode_queue_dlq_list_body(5, DlqCursor(dead_ns=7, seq=9)) == (
        b"\x01" + struct.pack("<I", 5) + b"\x01" + struct.pack("<QQ", 7, 9)
    )
    for bad in (0, 101):
        with pytest.raises(ValidationError):
            encode_queue_dlq_list_body(bad, None)


def _dead_message(seq: int, payload: bytes, error: str) -> bytes:
    return (
        struct.pack("<QIIIqqI", seq, 2, 5, 4, 100, 200, len(payload) + 1000)
        + struct.pack("<H", len(payload))
        + payload
        + _str16(error)
    )


def test_a_queue_dlq_page_decodes_its_letters_and_cursor() -> None:
    letters = _dead_message(4, b"a", "boom") + _dead_message(6, b"bc", "")
    data = b"\x02" + letters + b"\x01" + struct.pack("<QQ", 123, 6)
    page = decode_queue_dlq_list_answer(data)
    assert [
        (
            d.seq,
            d.priority,
            d.deliveries,
            d.failures,
            d.enqueued_at_ms,
            d.dead_at_ms,
            d.size,
            d.payload,
            d.error,
        )
        for d in page.messages
    ] == [
        (4, 2, 5, 4, 100, 200, 1001, b"a", "boom"),
        (6, 2, 5, 4, 100, 200, 1002, b"bc", ""),
    ]
    assert page.cursor == DlqCursor(dead_ns=123, seq=6)
    last = decode_queue_dlq_list_answer(b"\x00\x00")
    assert (last.messages, last.cursor) == ([], None)
    for bad in (data[:-1], data + b"\x00", b"\x00\x02"):
        with pytest.raises(IncompleteResponseError):
            decode_queue_dlq_list_answer(bad)


def test_a_queue_dlq_requeue_or_delete_names_its_messages_or_all() -> None:
    named = encode_queue_dlq_body([3, 8], False, 100)
    assert named == (
        b"\x01\x02" + struct.pack("<QQ", 3, 8) + b"\x01\x00" + b"\x01" + struct.pack("<I", 100)
    )
    every = encode_queue_dlq_body(None, True, 10, dry_run=True)
    assert every == b"\x00" + b"\x01\x01" + b"\x01" + struct.pack("<I", 10) + b"\x01\x01"
    # Neither, both, too many or a bad limit are refused before the trip.
    for seqs, all_, limit in (
        (None, False, 100),
        ([1], True, 100),
        (list(range(101)), False, 100),
        (None, True, 0),
        (None, True, 101),
    ):
        with pytest.raises(ValidationError):
            encode_queue_dlq_body(seqs, all_, limit)


def test_a_queue_dlq_answer_decodes_applied_refused_and_more() -> None:
    data = struct.pack("<I", 2) + b"\x01" + struct.pack("<QB", 9, 6) + _str8("not dead") + b"\x01"
    r = decode_queue_dlq_answer("queue.dlq_requeue", data)
    assert r.applied == 2 and r.more is True
    assert [(x.seq, x.why, x.reason) for x in r.refused] == [
        (9, QueueRefusalReason.NOT_DEAD, "not dead")
    ]
    for bad in (data[:-1], data + b"\x00", data[:-1] + b"\x02"):
        with pytest.raises(IncompleteResponseError):
            decode_queue_dlq_answer("queue.dlq_requeue", bad)


# ── Group dead letters and steal ────────────────────────────────────────────


def test_a_steal_body_names_the_records_and_their_deliveries() -> None:
    rid = StreamID(timestamp_ms=1000, sequence=2)
    body = encode_group_steal_body("g", "rescuer", 50, None, [(rid, 3)])
    assert body == (
        _str16("g")
        + _str16("rescuer")
        + struct.pack("<Q", 50)
        + b"\x00"
        + b"\x01"
        + struct.pack("<QQI", 1000, 2, 3)
    )
    leased = encode_group_steal_body("g", "c", 1, 5000, [])
    assert (
        leased
        == _str16("g")
        + _str16("c")
        + struct.pack("<Q", 1)
        + b"\x01"
        + struct.pack("<Q", 5000)
        + b"\x00"
    )
    for consumer, idle, lease in (("", 1, None), ("c", 0, None), ("c", 43_200_001, None)):
        with pytest.raises(ValidationError):
            encode_group_steal_body("g", consumer, idle, lease, [])
    with pytest.raises(ValidationError):
        encode_group_steal_body("g", "c", 1, 4999, [])
    with pytest.raises(ValidationError):
        encode_group_steal_body("g", "c", 1, None, [(rid, 1)] * 101)


def test_a_steal_answer_is_a_delivery_with_its_refusals() -> None:
    data = (
        struct.pack("<QQq", 3, 77, 9000)
        + b"\x01"
        + struct.pack("<QQI", 1000, 2, 4)
        + b"\x01"
        + struct.pack("<QQB", 1000, 3, 7)
        + _str8("not idle")
    )
    r = decode_group_steal_answer(data, "rescuer")
    assert (r.term, r.index, r.expires_at_ms, r.consumer) == (3, 77, 9000, "rescuer")
    [rec] = r.records
    assert (rec.id, rec.deliveries) == (StreamID(1000, 2), 4)
    [ref] = r.refused
    assert (ref.id, ref.why, ref.reason) == (
        StreamID(1000, 3),
        QueueRefusalReason.NOT_IDLE,
        "not idle",
    )
    for bad in (data[:-1], data + b"\x00", data[:-10] + b"\x09" + data[-9:]):
        with pytest.raises(IncompleteResponseError):
            decode_group_steal_answer(bad, "rescuer")


def test_a_group_dlq_list_body_and_answer() -> None:
    assert encode_group_dlq_list_body("g", 20, None) == (
        _str16("g") + b"\x01" + struct.pack("<I", 20) + b"\x00"
    )
    cursor = GroupDlqCursor(dead_ns=5, ms=6, seq=7)
    assert encode_group_dlq_list_body("g", 1, cursor) == (
        _str16("g") + b"\x01" + struct.pack("<I", 1) + b"\x01" + struct.pack("<QQQ", 5, 6, 7)
    )
    data = (
        b"\x01"
        + struct.pack("<QQIIq", 1000, 2, 5, 5, 4242)
        + _str16("handler blew up")
        + b"\x01"
        + struct.pack("<QQQ", 9, 1000, 2)
    )
    page = decode_group_dlq_list_answer(data)
    [d] = page.dead
    assert (d.id, d.deliveries, d.failures, d.dead_at_ms, d.error) == (
        StreamID(1000, 2),
        5,
        5,
        4242,
        "handler blew up",
    )
    assert page.cursor == GroupDlqCursor(dead_ns=9, ms=1000, seq=2)
    assert decode_group_dlq_list_answer(b"\x00\x00").cursor is None
    for bad in (data[:-1], data + b"\x00"):
        with pytest.raises(IncompleteResponseError):
            decode_group_dlq_list_answer(bad)


def test_a_group_dlq_requeue_or_delete_body_and_answer() -> None:
    rid = StreamID(timestamp_ms=1000, sequence=2)
    assert encode_group_dlq_body("g", [rid], False, 100) == (
        _str16("g")
        + b"\x01\x01"
        + struct.pack("<QQ", 1000, 2)
        + b"\x01\x00\x01"
        + struct.pack("<I", 100)
    )
    assert encode_group_dlq_body("g", None, True, 5, dry_run=False) == (
        _str16("g") + b"\x00\x01\x01\x01" + struct.pack("<I", 5) + b"\x01\x00"
    )
    for records, all_ in ((None, False), ([rid], True)):
        with pytest.raises(ValidationError):
            encode_group_dlq_body("g", records, all_, 100)
    data = struct.pack("<I", 0) + b"\x01" + struct.pack("<QQB", 1000, 2, 6) + _str8("x") + b"\x00"
    r = decode_group_dlq_answer("stream.group.dlq_delete", data)
    assert (r.applied, r.more) == (0, False)
    assert [(x.id, x.why, x.reason) for x in r.refused] == [(rid, QueueRefusalReason.NOT_DEAD, "x")]


# ── Round trips ─────────────────────────────────────────────────────────────


class _Recording(_Scripted):
    """Records each request's op and payload, answering reply(op)."""

    def __init__(self, answers: dict[int, bytes]) -> None:
        super().__init__(lambda *_: b"")
        self.answers = answers
        self.seen: list[tuple[int, bytes]] = []

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.writers.append(writer)
        try:
            while True:
                header = await reader.readexactly(HEADER_SIZE)
                _, n, rid, _, op, *_ = struct.unpack(REQUEST_HEADER_FORMAT, header)
                self.seen.append((op, await reader.readexactly(n)))
                writer.write(_frame(rid, StatusCode.OK, self.answers[op]))
        except (asyncio.IncompleteReadError, ConnectionError):
            writer.close()


async def test_the_client_sends_each_op_as_a_table_op() -> None:
    fake = _Recording(
        {
            OpCode.QUEUE_DLQ_LIST: b"\x00\x00",
            OpCode.QUEUE_DLQ_REQUEUE: struct.pack("<I", 1) + b"\x00\x00",
            OpCode.QUEUE_DLQ_DELETE: struct.pack("<I", 2) + b"\x00\x01",
            OpCode.STREAM_GROUP_STEAL: struct.pack("<QQq", 1, 2, 3) + b"\x00\x00",
            OpCode.STREAM_GROUP_DLQ_LIST: b"\x00\x00",
            OpCode.STREAM_GROUP_DLQ_REQUEUE: struct.pack("<I", 0) + b"\x00\x00",
            OpCode.STREAM_GROUP_DLQ_DELETE: struct.pack("<I", 0) + b"\x00\x00",
        }
    )
    server, addr = await fake.serve()
    client = await FloClient(addr, timeout_ms=1000).connect()
    rid = StreamID(1000, 2)
    try:
        q = client.queue
        await q.dlq_list("q", DlqListOptions(namespace="ns", limit=10, after=DlqCursor(1, 2)))
        assert (await q.dlq_requeue("q", [5], DlqRequeueOptions(namespace="ns"))).applied == 1
        deleted = await q.dlq_delete("q", None, DlqDeleteOptions(namespace="ns", all=True))
        assert (deleted.applied, deleted.more) == (2, True)
        s = client.stream
        stolen = await s.group_steal(
            "s",
            "g",
            "c",
            [GroupRecord(id=rid, deliveries=2), PendingEntry(id=rid, delivery_count=3)],
            10,
            StreamGroupStealOptions(namespace="ns"),
        )
        assert (stolen.term, stolen.index, stolen.consumer) == (1, 2, "c")
        await s.group_dlq_list("s", "g", GroupDlqListOptions(namespace="ns"))
        await s.group_dlq_requeue("s", "g", [rid])
        await s.group_dlq_delete("s", "g", None, GroupDlqDeleteOptions(all=True, dry_run=True))
    finally:
        await client.close()
        await fake.shut(server)
    ns_q = _str16("ns") + _str16("q")
    ns_s = _str16("ns") + _str16("s")
    assert fake.seen[0] == (
        OpCode.QUEUE_DLQ_LIST,
        ns_q + encode_queue_dlq_list_body(10, DlqCursor(1, 2)),
    )
    assert fake.seen[1] == (OpCode.QUEUE_DLQ_REQUEUE, ns_q + encode_queue_dlq_body([5], False, 100))
    assert fake.seen[2] == (
        OpCode.QUEUE_DLQ_DELETE,
        ns_q + encode_queue_dlq_body(None, True, 100, dry_run=False),
    )
    assert fake.seen[3] == (
        OpCode.STREAM_GROUP_STEAL,
        ns_s + encode_group_steal_body("g", "c", 10, None, [(rid, 2), (rid, 3)]),
    )
    assert fake.seen[4] == (
        OpCode.STREAM_GROUP_DLQ_LIST,
        ns_s + encode_group_dlq_list_body("g", 20, None),
    )
    assert fake.seen[5][0] == OpCode.STREAM_GROUP_DLQ_REQUEUE
    assert fake.seen[6][1].endswith(encode_group_dlq_body("g", None, True, 100, dry_run=True))
