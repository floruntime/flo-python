"""Queue dequeue, complete, fail and touch on the op table, a full queue's
refusal, and status 12 (unavailable) as routine. Bodies are checked byte for
byte against the server's schemas; refusal paths run against an in-process
fake server."""

import asyncio
import logging
import struct

import pytest

from flo import (
    DequeueOptions,
    EnqueueOptions,
    FloClient,
    QueueFailOptions,
    RetryPolicy,
)
from flo.exceptions import (
    BlockTooLongError,
    IncompleteResponseError,
    OverloadedError,
    UnavailableError,
    ValidationError,
    raise_for_status,
)
from flo.kv_body import (
    decode_dequeue_answer,
    decode_queue_report_answer,
    decode_queue_touch_answer,
    dequeue_block_ms,
    encode_dequeue_body,
    encode_queue_complete_body,
    encode_queue_fail_body,
    is_table_op,
)
from flo.types import (
    HEADER_SIZE,
    DequeueResult,
    Message,
    OpCode,
    OptionTag,
    QueueRefusal,
    QueueRefusalReason,
    QueueReportResult,
    Ran,
    Reason,
    StatusCode,
)
from flo.wire import REQUEST_HEADER_FORMAT, OptionsIterator

from .test_result_resend import _str16, _until
from .test_retry import _frame, _Scripted
from .test_stream_worker_ack import _one_record


def _refusal(reason: Reason, ran: Ran, message: bytes) -> bytes:
    return struct.pack("<HB", reason, ran) + message


def test_the_queue_ops_have_the_servers_codes_and_carry_bodies() -> None:
    codes = (OpCode.QUEUE_DEQUEUE, OpCode.QUEUE_COMPLETE, OpCode.QUEUE_FAIL, OpCode.QUEUE_TOUCH)
    assert [int(c) for c in codes] == [0x181, 0x182, 0x183, 0x184]
    for code in (0x171, 0x172, 0x174):
        with pytest.raises(ValueError):
            OpCode(code)
    assert all(is_table_op(c) for c in codes)


def test_status_12_is_unavailable_and_its_body_is_read() -> None:
    assert StatusCode(12) is StatusCode.UNAVAILABLE
    with pytest.raises(UnavailableError) as raised:
        raise_for_status(StatusCode(12), _refusal(Reason.UNCLASSIFIED, Ran.NO, b"retry"))
    assert (raised.value.ran, str(raised.value)) == (Ran.NO, "retry")


# ── Bodies ──────────────────────────────────────────────────────────────────


def test_a_dequeue_body_is_consumer_count_lease_wait_and_ack() -> None:
    body = encode_dequeue_body("c1", 10, 60000, 1500, True)
    assert body == (
        b"\x01"
        + _str16("c1")
        + b"\x01"
        + struct.pack("<I", 10)
        + b"\x01"
        + struct.pack("<Q", 60000)
        + b"\x01"
        + struct.pack("<Q", 1500)
        + b"\x01\x01"
    )
    assert dequeue_block_ms(body) == 1500
    # No lease is the server's 30 s.
    bare = encode_dequeue_body("", 1, None, None, False)
    assert (
        bare
        == b"\x01"
        + _str16("")
        + b"\x01"
        + struct.pack("<I", 1)
        + b"\x00\x01"
        + bytes(8)
        + b"\x01\x00"
    )
    assert dequeue_block_ms(bare) == 0


@pytest.mark.parametrize(
    ("count", "lease_ms", "block_ms", "error"),
    [
        (0, None, 0, ValidationError),
        (101, None, 0, ValidationError),
        (1, 4999, 0, ValidationError),
        (1, 43_200_001, 0, ValidationError),
        (1, None, 300_001, BlockTooLongError),
    ],
    ids=["no-count", "too-many", "short-lease", "long-lease", "long-wait"],
)
def test_a_dequeue_the_server_would_refuse_is_refused_here(
    count: int, lease_ms: int | None, block_ms: int, error: type[Exception]
) -> None:
    with pytest.raises(error):
        encode_dequeue_body("", count, lease_ms, block_ms, False)


def _dequeue_answer(msgs: list[tuple[int, int, int, bytes]], term: int, index: int) -> bytes:
    out = bytes([len(msgs)])
    for seq, prio, deliveries, payload in msgs:
        out += struct.pack("<QIIqI", seq, prio, deliveries, 500, len(payload)) + payload
    return out + struct.pack("<QQq", term, index, 9000)


def test_a_dequeue_answer_decodes_its_messages_and_token() -> None:
    got = decode_dequeue_answer(_dequeue_answer([(4, 0, 1, b"a"), (5, 2, 2, b"bc")], 3, 40), "c1")
    assert (got.term, got.index, got.expires_at_ms, got.consumer) == (3, 40, 9000, "c1")
    assert [(m.seq, m.priority, m.deliveries, m.payload) for m in got.messages] == [
        (4, 0, 1, b"a"),
        (5, 2, 2, b"bc"),
    ]
    with pytest.raises(IncompleteResponseError):
        decode_dequeue_answer(_dequeue_answer([], 3, 40)[:-1], "")


_DELIVERY = DequeueResult(
    messages=[
        Message(seq=4, payload=b"a", deliveries=1),
        Message(seq=5, payload=b"", deliveries=2),
    ],
    term=3,
    index=40,
    consumer="c1",
)
_REPORT = (
    b"\x01" + _str16("c1") + struct.pack("<QQ", 3, 40) + b"\x02" + struct.pack("<QIQI", 4, 1, 5, 2)
)


def test_reports_name_the_consumer_token_and_each_messages_deliveries() -> None:
    assert encode_queue_complete_body(_DELIVERY) == _REPORT
    one = encode_queue_complete_body(_DELIVERY, _DELIVERY.messages[1:])
    assert one == b"\x01" + _str16("c1") + struct.pack("<QQ", 3, 40) + b"\x01" + struct.pack(
        "<QI", 5, 2
    )
    assert encode_queue_fail_body(_DELIVERY, None, 250, True, b"boom") == (
        _REPORT + b"\x01" + struct.pack("<Q", 250) + b"\x01\x01\x01" + _str16("boom")
    )
    with pytest.raises(ValidationError):
        encode_queue_complete_body(DequeueResult(messages=[]))
    with pytest.raises(ValidationError):
        encode_queue_fail_body(_DELIVERY, None, 0, False, b"x" * 1025)


def _refused(entries: list[tuple[int, int, bytes]]) -> bytes:
    return bytes([len(entries)]) + b"".join(
        struct.pack("<QB", seq, why) + bytes([len(text)]) + text for seq, why, text in entries
    )


def test_report_answers_carry_each_refused_message_and_why() -> None:
    data = struct.pack("<I", 1) + _refused([(5, 0, b"gone"), (6, 2, b"another consumer")])
    got = decode_queue_report_answer("queue.complete", data)
    assert got.applied == 1
    assert got.refused == [
        QueueRefusal(seq=5, why=QueueRefusalReason.GONE, reason="gone"),
        QueueRefusal(seq=6, why=QueueRefusalReason.OTHER_HOLDER, reason="another consumer"),
    ]
    # For a complete, gone means done: only the other is lost.
    assert [r.seq for r in got.lost] == [6]
    touched = decode_queue_touch_answer(struct.pack("<qI", 7000, 2) + _refused([]))
    assert (touched.expires_at_ms, touched.applied, touched.refused) == (7000, 2, [])
    with pytest.raises(IncompleteResponseError):
        decode_queue_report_answer("queue.fail", struct.pack("<I", 0) + _refused([(5, 9, b"")]))


# ── Against a fake server ───────────────────────────────────────────────────


class _Recording(_Scripted):
    """Records each request's op and payload, and answers with reply(n, op, rid)."""

    def __init__(self, reply) -> None:  # type: ignore[no-untyped-def]
        super().__init__(reply)
        self.payloads: list[tuple[int, bytes]] = []

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.writers.append(writer)
        try:
            while True:
                header = await reader.readexactly(HEADER_SIZE)
                _, n, rid, _, op, *_ = struct.unpack(REQUEST_HEADER_FORMAT, header)
                payload = await reader.readexactly(n)
                self.payloads.append((op, payload))
                self.ids.append(rid)
                self.ops.append(op)
                writer.write(self.reply(len(self.ids) - 1, op, rid))
        except (asyncio.IncompleteReadError, ConnectionError):
            writer.close()


_FAST = RetryPolicy(max_attempts=4, initial_backoff_ms=1, max_backoff_ms=2)


async def test_a_consumer_dequeues_completes_and_sees_per_message_refusals() -> None:
    def reply(_n: int, op: int, rid: int) -> bytes:
        if op == OpCode.QUEUE_DEQUEUE:
            return _frame(
                rid, StatusCode.OK, _dequeue_answer([(4, 0, 1, b"a"), (5, 0, 2, b"")], 3, 40)
            )
        # One applied, one already done: an ok answer either way.
        return _frame(rid, StatusCode.OK, struct.pack("<I", 1) + _refused([(5, 0, b"gone")]))

    fake = _Recording(reply)
    server, addr = await fake.serve()
    client = await FloClient(addr, namespace="ns", timeout_ms=1000, retry=_FAST).connect()
    try:
        got = await client.queue.dequeue("q", 2, DequeueOptions(consumer="c1", block_ms=0))
        report = await client.queue.complete("q", got)
        failed = await client.queue.fail("q", got, got.messages[:1], QueueFailOptions(dead=True))
    finally:
        await client.close()
        await fake.shut(server)
    assert report == QueueReportResult(
        applied=1, refused=[QueueRefusal(seq=5, why=QueueRefusalReason.GONE, reason="gone")]
    )
    assert report.lost == [] and failed.applied == 1
    prefix = _str16("ns") + _str16("q")
    (op0, dq), (op1, cp), (op2, _) = fake.payloads
    assert (op0, op1, op2) == (OpCode.QUEUE_DEQUEUE, OpCode.QUEUE_COMPLETE, OpCode.QUEUE_FAIL)
    assert dq == prefix + encode_dequeue_body("c1", 2, None, 0, False)
    assert cp == prefix + b"\x01" + _str16("c1") + struct.pack(
        "<QQ", 3, 40
    ) + b"\x02" + struct.pack("<QIQI", 4, 1, 5, 2)


async def test_an_enqueue_to_a_full_queue_is_retried_then_raised_overloaded() -> None:
    full = _refusal(
        Reason.UNCLASSIFIED,
        Ran.NO,
        b"overloaded: queue 'q' is full: it holds 2 messages (at most 2) and 10 held bytes "
        b"(at most 1024), ready, leased and dead; consume or purge it",
    )
    fake = _Recording(lambda _n, _op, rid: _frame(rid, StatusCode.OVERLOADED, full))
    server, addr = await fake.serve()
    client = await FloClient(addr, timeout_ms=1000, retry=_FAST).connect()
    try:
        with pytest.raises(OverloadedError) as raised:
            await client.queue.enqueue("q", b"x", EnqueueOptions(delay_ms=5000))
    finally:
        await client.close()
        await fake.shut(server)
    assert raised.value.ran == Ran.NO
    assert "is full" in str(raised.value)
    assert len(fake.ids) == _FAST.max_attempts
    # delay_ms travels as its option, a u64:
    # [ns_len:u16][ns][key_len:u16][key][value_len:u32][value][options_len:u16][options]
    _, payload = fake.payloads[0]
    at = 2 + int.from_bytes(payload[0:2], "little")
    at += 2 + int.from_bytes(payload[at : at + 2], "little")
    at += 4 + int.from_bytes(payload[at : at + 4], "little")
    options = payload[at + 2 :]
    found = [o for o in OptionsIterator(options) if o.tag == OptionTag.DELAY_MS]
    assert found and found[0].as_u64() == 5000


async def test_a_stream_worker_retries_an_unavailable_group_read_quietly(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The first reads are refused unavailable, ran no (records still being
    # read back from disk), past the client's own retries; then a record.
    unavailable = _refusal(
        Reason.UNCLASSIFIED, Ran.NO, b"unavailable: reading records back from disk; retry"
    )
    handled: list[bytes] = []

    def reply(n: int, op: int, rid: int) -> bytes:
        if op == OpCode.STREAM_GROUP_READ:
            reads = sum(1 for o in fake.ops if o == OpCode.STREAM_GROUP_READ)
            if reads <= 3:
                return _frame(rid, StatusCode.UNAVAILABLE, unavailable)
            if reads == 4:
                return _frame(rid, StatusCode.OK, _one_record())
            return b""  # park
        if op == OpCode.WORKER_HEARTBEAT:
            return _frame(rid, StatusCode.OK, b"\x00")
        return _frame(rid, StatusCode.OK, b"")

    fake = _Recording(reply)
    server, addr = await fake.serve()
    client = FloClient(addr, timeout_ms=1000, retry=RetryPolicy(max_attempts=1))

    async def handler(ctx: object) -> None:
        handled.append(b"x")

    worker = client.new_stream_worker(stream="s", group="g", handler=handler, block_ms=100)
    with caplog.at_level(logging.INFO, logger="flo.worker"):
        run = asyncio.create_task(worker.start())
        try:
            await _until(lambda: bool(handled), timeout=10)
        finally:
            worker.stop()
            await asyncio.wait_for(run, timeout=10)
            await fake.shut(server)
    assert handled
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    reads = [p for op, p in fake.payloads if op == OpCode.STREAM_GROUP_READ]
    # The same group and consumer each time.
    assert len({p[: p.index(b"g") + 1] for p in reads[:4]}) == 1


async def test_a_pending_drain_asks_again_after_an_unavailable_claim() -> None:
    unavailable = _refusal(Reason.UNCLASSIFIED, Ran.NO, b"unavailable: retry")
    claims = 0

    class Claims:
        async def group_claim(self, *_args: object, **_kw: object) -> object:
            nonlocal claims
            claims += 1
            if claims <= 2:
                raise_for_status(StatusCode.UNAVAILABLE, unavailable)
            from flo.types import StreamClaimResult, StreamID

            return StreamClaimResult(records=[], next_cursor=StreamID(), done=True)

    worker = FloClient("127.0.0.1:1").new_stream_worker(stream="s", group="g", handler=print)

    class Client:
        stream = Claims()

    worker._client = Client()  # type: ignore[assignment]
    worker._semaphore = asyncio.Semaphore(1)
    await asyncio.wait_for(worker._drain_pending(), 5)
    assert claims == 3


@pytest.mark.parametrize("delay_ms", [-1, 86_400_001, True])
async def test_an_enqueue_delay_past_a_day_is_refused_before_sending(delay_ms: int) -> None:
    sent: list[object] = []
    client = FloClient("127.0.0.1:1")

    async def send(*args: object, **kw: object) -> None:
        sent.append(args)

    client._send_and_check = send  # type: ignore[method-assign,assignment]
    with pytest.raises(ValidationError):
        await client.queue.enqueue("q", b"x", EnqueueOptions(delay_ms=delay_ms))
    assert sent == []


def test_a_halted_shards_refusal_is_not_a_routine_retry() -> None:
    from flo.retry import is_refused_unrun, is_shard_halted

    def refusal(reason: Reason, ran: Ran = Ran.NO) -> Exception:
        try:
            raise_for_status(StatusCode.UNAVAILABLE, _refusal(reason, ran, b"x"))
        except UnavailableError as e:
            return e
        raise AssertionError

    halted = refusal(Reason.SHARD_HALTED)
    assert is_shard_halted(halted) and not is_refused_unrun(halted)
    assert is_refused_unrun(refusal(Reason.LEADER_UNKNOWN))
    assert not is_refused_unrun(refusal(Reason.LEADER_UNKNOWN, Ran.UNKNOWN))
