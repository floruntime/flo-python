"""Consumer groups on the delivery model: bodies and answers byte for byte
against the server's group handler, stale reports answered per record, and
KV's keys_not_colocated refusal. Client paths run against an in-process fake
server."""

import asyncio
import logging
import struct

import pytest

from flo import (
    FloClient,
    GroupReadResult,
    GroupRecord,
    GroupRefusal,
    GroupReportResult,
    RetryPolicy,
    StreamGroupCreateOptions,
    StreamGroupNackOptions,
    StreamGroupReadOptions,
)
from flo.exceptions import BadRequestError, IncompleteResponseError, ValidationError
from flo.types import OpCode, OptionTag, QueueRefusalReason, Ran, Reason, StatusCode, StreamID
from flo.wire import (
    OptionsIterator,
    parse_group_info_response,
    parse_group_read_response,
    parse_group_report_response,
    parse_group_touch_response,
    serialize_group_report_value,
)

from .test_queue_ops import _Recording
from .test_result_resend import _str16, _until
from .test_retry import _frame
from .test_stream_worker_ack import REPORT_OK, _records

_FAST = RetryPolicy(max_attempts=1)


def _rec(ms: int, seq: int, deliveries: int) -> GroupRecord:
    return GroupRecord(id=StreamID(timestamp_ms=ms, sequence=seq), deliveries=deliveries)


def _refused(*records: tuple[int, int, QueueRefusalReason]) -> bytes:
    return struct.pack("<I", len(records)) + b"".join(
        struct.pack("<QQB", ms, seq, why) for ms, seq, why in records
    )


def _value_and_options(payload: bytes) -> tuple[bytes, bytes]:
    """A request's [ns][key][value_len:u32][value][options_len:u16][options]."""
    at = 2 + int.from_bytes(payload[0:2], "little")
    at += 2 + int.from_bytes(payload[at : at + 2], "little")
    n = int.from_bytes(payload[at : at + 4], "little")
    value = payload[at + 4 : at + 4 + n]
    return value, payload[at + 4 + n + 2 :]


# ── Bodies and answers ──────────────────────────────────────────────────────


def test_a_report_names_the_consumer_delivery_and_each_records_deliveries() -> None:
    delivery = GroupReadResult(
        records=[_rec(1000, 1, 1), _rec(1000, 2, 3)], term=3, index=40, consumer="c1"
    )
    value = serialize_group_report_value("g", delivery, None)
    assert value == (
        _str16("g")
        + _str16("c1")
        + struct.pack("<QQI", 3, 40, 2)
        + struct.pack("<QQI", 1000, 1, 1)
        + struct.pack("<QQI", 1000, 2, 3)
    )
    # Some of the read's records only.
    one = serialize_group_report_value("g", delivery, delivery.records[1:])
    assert one.endswith(struct.pack("<QQI", 3, 40, 1) + struct.pack("<QQI", 1000, 2, 3))
    with pytest.raises(ValidationError):
        serialize_group_report_value("g", delivery, [])
    with pytest.raises(ValidationError):
        serialize_group_report_value("g", delivery, [_rec(1, i, 1) for i in range(101)])


def test_a_group_read_answer_decodes_its_token_and_each_records_deliveries() -> None:
    got = parse_group_read_response(_records(2, term=7, index=99, deliveries=2), "c1")
    assert (got.term, got.index, got.expires_at_ms, got.consumer) == (7, 99, 9_000_000, "c1")
    assert [(r.id, r.payload, r.deliveries) for r in got.records] == [
        (StreamID(1000, 1), b"hello", 2),
        (StreamID(1000, 2), b"hello", 2),
    ]
    # Nothing read: the fixed part, all zero.
    empty = parse_group_read_response(bytes(28), "c1")
    assert (empty.records, empty.expires_at_ms) == ([], 0)
    for cut in (_records(1)[:-1], _records(1) + b"\x00", bytes(27)):
        with pytest.raises(IncompleteResponseError):
            parse_group_read_response(cut, "c1")


def test_report_and_touch_answers_carry_each_refused_record_and_why() -> None:
    answer = struct.pack("<I", 1) + _refused(
        (1000, 2, QueueRefusalReason.DELIVERED_AGAIN), (1000, 3, QueueRefusalReason.GONE)
    )
    got = parse_group_report_response("stream group ack", answer)
    assert got == GroupReportResult(
        applied=1,
        refused=[
            GroupRefusal(id=StreamID(1000, 2), why=QueueRefusalReason.DELIVERED_AGAIN),
            GroupRefusal(id=StreamID(1000, 3), why=QueueRefusalReason.GONE),
        ],
    )
    # GONE is done; the other went to another delivery.
    assert [r.id for r in got.lost] == [StreamID(1000, 2)]
    touch = parse_group_touch_response(
        struct.pack("<qI", 123_456, 0) + _refused((1, 1, QueueRefusalReason.EXPIRED))
    )
    assert (touch.expires_at_ms, touch.applied) == (123_456, 0)
    assert touch.refused[0].why == QueueRefusalReason.EXPIRED
    with pytest.raises(IncompleteResponseError):
        parse_group_report_response("stream group ack", struct.pack("<II", 0, 1) + bytes(16))
    with pytest.raises(IncompleteResponseError):
        # why 9 is no refusal the server sends
        parse_group_report_response(
            "stream group ack", struct.pack("<II", 0, 1) + struct.pack("<QQB", 1, 1, 9)
        )


def test_a_group_info_answer_decodes_its_counts() -> None:
    info = parse_group_info_response(struct.pack("<QIQQQQQ", 5, 2, 77, 3, 2, 1, 4))
    assert (info.pending, info.holders, info.created_at_ns) == (5, 2, 77)
    assert (info.ready, info.leased, info.dead, info.trimmed_pending) == (3, 2, 1, 4)


async def test_group_settings_and_names_out_of_bounds_are_refused_before_sending() -> None:
    client = FloClient("127.0.0.1:1")
    refused = [
        client.stream.group_create("s", "g", StreamGroupCreateOptions(ack_timeout_ms=4999)),
        client.stream.group_create("s", "g", StreamGroupCreateOptions(ack_timeout_ms=43_200_001)),
        client.stream.group_create("s", "g", StreamGroupCreateOptions(max_deliver=0)),
        client.stream.group_create("s", "g" * 257),
        client.stream.group_read("s", "g", "c", StreamGroupReadOptions(count=101)),
        client.stream.group_read("s", "g", "c", StreamGroupReadOptions(count=0)),
    ]
    delivery = GroupReadResult(records=[_rec(1, 1, 1)], term=1, index=1, consumer="c")
    refused += [
        client.stream.group_nack("s", "g", delivery, None, StreamGroupNackOptions(error="x" * 256)),
        client.stream.group_nack(
            "s", "g", delivery, None, StreamGroupNackOptions(delay_ms=86_400_001)
        ),
    ]
    for call in refused:
        with pytest.raises(ValidationError):
            await call


# ── Through the client ─────────────────────────────────────────────────────


async def test_a_consumer_reads_acks_nacks_and_touches_by_its_delivery() -> None:
    def reply(_n: int, op: int, rid: int) -> bytes:
        if op == OpCode.STREAM_GROUP_CREATE:
            return _frame(rid, StatusCode.OK, b"\x01")
        if op == OpCode.STREAM_GROUP_READ:
            return _frame(rid, StatusCode.OK, _records(2, term=3, index=40))
        if op == OpCode.STREAM_GROUP_TOUCH:
            return _frame(rid, StatusCode.OK, struct.pack("<qI", 5_000, 2) + _refused())
        return _frame(rid, StatusCode.OK, REPORT_OK)

    fake = _Recording(reply)
    server, addr = await fake.serve()
    client = await FloClient(addr, namespace="ns", timeout_ms=1000, retry=_FAST).connect()
    try:
        created = await client.stream.group_create(
            "s", "g", StreamGroupCreateOptions(ack_timeout_ms=60_000, max_deliver=3)
        )
        got = await client.stream.group_read("s", "g", "c1", StreamGroupReadOptions(count=2))
        touched = await client.stream.group_touch("s", "g", got)
        acked = await client.stream.group_ack("s", "g", got, got.records[:1])
        await client.stream.group_nack(
            "s", "g", got, got.records[1:], StreamGroupNackOptions(delay_ms=500, error="boom")
        )
    finally:
        await client.close()
        await fake.shut(server)
    assert created is True and touched.expires_at_ms == 5_000 and acked.applied == 1
    ops = [op for op, _ in fake.payloads]
    assert ops == [
        OpCode.STREAM_GROUP_CREATE,
        OpCode.STREAM_GROUP_READ,
        OpCode.STREAM_GROUP_TOUCH,
        OpCode.STREAM_GROUP_ACK,
        OpCode.STREAM_GROUP_NACK,
    ]
    (_, create), (_, read), (_, touch), (_, ack), (_, nack) = fake.payloads
    value, options = _value_and_options(create)
    assert value == _str16("g")
    opts = {o.tag: o.data for o in OptionsIterator(options)}
    assert opts == {
        OptionTag.ACK_TIMEOUT_MS: struct.pack("<I", 60_000),
        OptionTag.MAX_DELIVER: b"\x03",
    }
    value, options = _value_and_options(read)
    assert value == _str16("g") + _str16("c1")
    assert {o.tag: o.data for o in OptionsIterator(options)} == {
        OptionTag.COUNT: struct.pack("<I", 2)
    }
    head = _str16("g") + _str16("c1")
    assert _value_and_options(touch)[0] == head + struct.pack("<QQI", 3, 40, 2) + struct.pack(
        "<QQIQQI", 1000, 1, 1, 1000, 2, 1
    )
    assert _value_and_options(ack)[0] == head + struct.pack("<QQIQQI", 3, 40, 1, 1000, 1, 1)
    value, options = _value_and_options(nack)
    assert value == head + struct.pack("<QQIQQI", 3, 40, 1, 1000, 2, 1)
    assert {o.tag: o.data for o in OptionsIterator(options)} == {
        OptionTag.DELAY_MS: struct.pack("<Q", 500),
        OptionTag.ERROR_TEXT: b"boom",
    }


async def test_a_stale_group_report_succeeds_and_names_each_record_it_skipped() -> None:
    # The server answers a stale ack ok, applying nothing to those records
    # and saying why: the call itself doesn't raise.
    def reply(_n: int, op: int, rid: int) -> bytes:
        if op == OpCode.STREAM_GROUP_READ:
            return _frame(rid, StatusCode.OK, _records(2))
        return _frame(
            rid,
            StatusCode.OK,
            struct.pack("<I", 0)
            + _refused(
                (1000, 1, QueueRefusalReason.EXPIRED), (1000, 2, QueueRefusalReason.OTHER_HOLDER)
            ),
        )

    fake = _Recording(reply)
    server, addr = await fake.serve()
    client = await FloClient(addr, timeout_ms=1000, retry=_FAST).connect()
    try:
        got = await client.stream.group_read("s", "g", "c1")
        result = await client.stream.group_ack("s", "g", got)
    finally:
        await client.close()
        await fake.shut(server)
    assert result.applied == 0
    assert [(r.id.sequence, r.why) for r in result.lost] == [
        (1, QueueRefusalReason.EXPIRED),
        (2, QueueRefusalReason.OTHER_HOLDER),
    ]


async def test_a_stream_worker_logs_a_stale_ack_and_keeps_going(
    caplog: pytest.LogCaptureFixture,
) -> None:
    reads = 0

    def reply(_n: int, op: int, rid: int) -> bytes:
        nonlocal reads
        if op == OpCode.STREAM_GROUP_READ:
            reads += 1
            return _frame(rid, StatusCode.OK, _records(1)) if reads == 1 else b""
        if op == OpCode.STREAM_GROUP_ACK:
            return _frame(
                rid,
                StatusCode.OK,
                struct.pack("<I", 0) + _refused((1000, 1, QueueRefusalReason.DELIVERED_AGAIN)),
            )
        if op == OpCode.WORKER_HEARTBEAT:
            return _frame(rid, StatusCode.OK, b"\x00")
        return _frame(rid, StatusCode.OK, b"")

    fake = _Recording(reply)
    server, addr = await fake.serve()

    async def handler(_ctx: object) -> None:
        return None

    worker = FloClient(addr, timeout_ms=1000, retry=_FAST).new_stream_worker(
        stream="s", group="g", handler=handler, block_ms=30000
    )
    caplog.set_level(logging.WARNING, logger="flo")
    run = asyncio.create_task(worker.start())
    try:
        await _until(lambda: OpCode.STREAM_GROUP_ACK in [op for op, _ in fake.payloads], 10)
        await asyncio.sleep(0.1)
        assert not run.done()
    finally:
        worker.stop()
        await asyncio.wait_for(run, timeout=10)
        await fake.shut(server)
    stale = [r for r in caplog.records if "DELIVERED_AGAIN" in r.getMessage()]
    assert len(stale) == 1 and stale[0].levelno == logging.WARNING
    assert OpCode.STREAM_GROUP_NACK not in [op for op, _ in fake.payloads]


async def test_a_stream_worker_touches_a_long_record_by_its_delivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from flo import worker as worker_mod

    # A 600 ms lease: a touch every 100 ms, the floor.
    monkeypatch.setattr(worker_mod, "_DEFAULT_GROUP_LEASE_MS", 600)
    reads = 0

    def reply(_n: int, op: int, rid: int) -> bytes:
        nonlocal reads
        if op == OpCode.STREAM_GROUP_READ:
            reads += 1
            return _frame(rid, StatusCode.OK, _records(1, term=5, index=6)) if reads == 1 else b""
        if op == OpCode.STREAM_GROUP_TOUCH:
            return _frame(rid, StatusCode.OK, struct.pack("<qI", 1, 1) + _refused())
        if op == OpCode.STREAM_GROUP_ACK:
            return _frame(rid, StatusCode.OK, REPORT_OK)
        if op == OpCode.WORKER_HEARTBEAT:
            return _frame(rid, StatusCode.OK, b"\x00")
        if op == OpCode.STREAM_GROUP_LIST:
            return _frame(rid, StatusCode.OK, b"\x00\x00\x00")
        return _frame(rid, StatusCode.OK, b"")

    fake = _Recording(reply)
    server, addr = await fake.serve()

    async def slow(_ctx: object) -> None:
        await asyncio.sleep(0.45)

    worker = FloClient(addr, timeout_ms=1000, retry=_FAST).new_stream_worker(
        stream="s", group="g", handler=slow, block_ms=30000, auto_touch=True
    )
    run = asyncio.create_task(worker.start())
    try:
        await _until(lambda: OpCode.STREAM_GROUP_ACK in [op for op, _ in fake.payloads], 10)
    finally:
        worker.stop()
        await asyncio.wait_for(run, timeout=10)
        await fake.shut(server)
    touches = [p for op, p in fake.payloads if op == OpCode.STREAM_GROUP_TOUCH]
    assert 3 <= len(touches) <= 5
    head = _str16("g") + _str16(worker.config.consumer)
    assert _value_and_options(touches[0])[0] == head + struct.pack("<QQIQQI", 5, 6, 1, 1000, 1, 1)
    ops = [op for op, _ in fake.payloads]
    assert ops.index(OpCode.STREAM_GROUP_ACK) > max(
        i for i, op in enumerate(ops) if op == OpCode.STREAM_GROUP_TOUCH
    )


# ── KV: keys in a transaction share a {tag} ─────────────────────────────────


async def test_a_key_that_routes_elsewhere_in_a_transaction_is_refused_unrun() -> None:
    message = (
        b"bad request: this key routes elsewhere than the key the transaction was begun with; "
        b"give every key in a transaction the same {tag}, e.g. {user:1}:balance"
    )

    def reply(_n: int, op: int, rid: int) -> bytes:
        if op == OpCode.KV_BEGIN_TXN:
            return _frame(rid, StatusCode.OK, struct.pack("<QQ", 9, 1234))
        body = int(Reason.KEYS_NOT_COLOCATED).to_bytes(2, "little") + bytes([Ran.NO]) + message
        return _frame(rid, StatusCode.BAD_REQUEST, body)

    fake = _Recording(reply)
    server, addr = await fake.serve()
    # A policy that would retry anything retryable: this isn't.
    client = await FloClient(
        addr, timeout_ms=1000, retry=RetryPolicy(max_attempts=4, initial_backoff_ms=1)
    ).connect()
    try:
        txn = await client.kv.begin("{t}a")
        with pytest.raises(BadRequestError) as raised:
            await txn.put("b", b"x")
    finally:
        await client.close()
        await fake.shut(server)
    assert (raised.value.reason, raised.value.ran) == (Reason.KEYS_NOT_COLOCATED, Ran.NO)
    assert "same {tag}" in str(raised.value)
    assert [op for op, _ in fake.payloads] == [OpCode.KV_BEGIN_TXN, OpCode.KV_PUT]
