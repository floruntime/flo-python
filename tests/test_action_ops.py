"""The action ops on the op table (await, complete, fail, touch, info), the
stream group and queue reads, and what the client does with their refusals.
Bodies are checked byte for byte against the server's schemas; the round
trips run against an in-process fake server."""

import asyncio
import dataclasses
import struct

import pytest

from flo import FloClient, RetryPolicy, WorkerAwaitOptions, WorkerFailOptions
from flo.exceptions import (
    BlockTooLongError,
    ConflictError,
    IncompleteResponseError,
    StaleReportError,
    UnavailableError,
    ValidationError,
)
from flo.kv_body import (
    decode_action_info_answer,
    decode_await_answer,
    decode_group_list_answer,
    decode_group_members_answer,
    decode_peek_answer,
    decode_touch_answer,
    encode_await_body,
    encode_complete_body,
    encode_fail_body,
    encode_group_members_body,
    encode_peek_body,
    encode_touch_body,
    is_table_op,
)
from flo.queue import parse_dlq_page
from flo.types import HEADER_SIZE, GroupMemberState, OpCode, Ran, Reason, StatusCode
from flo.wire import REQUEST_HEADER_FORMAT

from .test_result_resend import NO_TASK, TASK, _str16, task_answer
from .test_retry import _frame, _Scripted


def test_the_action_ops_have_the_servers_codes_and_carry_bodies() -> None:
    codes = (OpCode.ACTION_AWAIT, OpCode.ACTION_COMPLETE, OpCode.ACTION_FAIL, OpCode.ACTION_TOUCH)
    assert [int(c) for c in codes] == [0x30A, 0x30B, 0x30C, 0x30D]
    assert int(OpCode.ACTION_INFO) == 0x30F
    assert int(OpCode.QUEUE_PEEK) == 0x180
    assert (int(OpCode.STREAM_GROUP_LIST), int(OpCode.STREAM_GROUP_MEMBERS)) == (0x15D, 0x15E)
    for code in (0x17C, 0x306, 0x307, 0x308, 0x309):
        with pytest.raises(ValueError):
            OpCode(code)
    for op in (*codes, OpCode.ACTION_INFO, OpCode.QUEUE_PEEK, OpCode.STREAM_GROUP_MEMBERS):
        assert is_table_op(op)


# ── Await ───────────────────────────────────────────────────────────────────


def test_an_await_body_lists_its_actions_then_block_ms_then_its_key() -> None:
    body = encode_await_body(["echo", "ab"], 5000, b"rk-1")
    assert body == (
        b"\x02"
        + _str16("echo")
        + _str16("ab")
        + b"\x01"
        + struct.pack("<Q", 5000)
        + b"\x01\x04rk-1"
    )
    # No block_ms is the server's default; no key is absent.
    assert encode_await_body(["a"], None, None) == b"\x01" + _str16("a") + b"\x00\x00"


@pytest.mark.parametrize(
    ("actions", "block_ms", "key", "error"),
    [
        ([], 0, None, ValidationError),
        (["a"] * 65, 0, None, ValidationError),
        (["a" * 257], 0, None, ValidationError),
        (["a"], 300_001, None, BlockTooLongError),
        (["a"], -1, None, ValidationError),
        (["a"], 0, b"k" * 65, ValidationError),
    ],
    ids=["no-actions", "too-many", "long-name", "long-wait", "negative-wait", "long-key"],
)
def test_an_await_the_server_would_refuse_is_refused_here(
    actions: list[str], block_ms: int, key: bytes | None, error: type[Exception]
) -> None:
    with pytest.raises(error):
        encode_await_body(actions, block_ms, key)


def test_an_await_answer_decodes_every_field_of_the_task() -> None:
    task = dataclasses.replace(TASK, caller_run="wf-1", caller_workflow="orders")
    assert decode_await_answer(task_answer(task)) == task
    assert decode_await_answer(NO_TASK) is None
    assert task.task_id == "act-42:1:3:40:charge"


@pytest.mark.parametrize("cut", range(1, len(task_answer(TASK))))
def test_an_await_answer_cut_short_is_refused(cut: int) -> None:
    with pytest.raises(IncompleteResponseError):
        decode_await_answer(task_answer(TASK)[:cut])


def test_an_await_answer_with_bytes_left_over_is_refused() -> None:
    with pytest.raises(IncompleteResponseError):
        decode_await_answer(task_answer(TASK) + b"\x00")
    with pytest.raises(IncompleteResponseError):
        decode_await_answer(NO_TASK + b"\x00")


# ── Reports ─────────────────────────────────────────────────────────────────

_DELIVERY = _str16("charge") + _str16("w1") + struct.pack("<IQQ", 1, 3, 40)


def test_a_complete_names_the_delivery_then_its_outcome_and_result() -> None:
    assert encode_complete_body("w1", TASK, "", b"pong") == (
        _DELIVERY + b"\x01" + _str16("") + b"\x01" + _str16("pong")
    )


def test_a_fail_names_the_delivery_then_retry_abandoned_delay_and_error() -> None:
    assert encode_fail_body("w1", TASK, b"boom", True, False, 250) == (
        _DELIVERY + b"\x01\x01\x01\x00\x01" + struct.pack("<Q", 250) + b"\x01" + _str16("boom")
    )
    with pytest.raises(ValidationError):
        encode_fail_body("w1", TASK, b"", True, False, 86_400_001)
    with pytest.raises(ValidationError):
        encode_fail_body("w1", TASK, b"x" * 65_536, True, False, 0)


def test_a_touch_names_only_the_delivery() -> None:
    assert encode_touch_body("w1", TASK) == _DELIVERY
    assert decode_touch_answer(struct.pack("<q", 1234)) == 1234
    with pytest.raises(IncompleteResponseError):
        decode_touch_answer(b"")


# ── Reads ───────────────────────────────────────────────────────────────────


def test_action_info_decodes_its_counts_and_latency() -> None:
    head = _str16("payments") + struct.pack("<IQIqQ", 2, 5000, 3, 77, 4) + b"\x00\x00"
    some = b"".join(b"\x01" + struct.pack("<Q", v) for v in (1, 9, 2, 0, 9, 40, 30, 90, 120))
    info = decode_action_info_answer(head + some)
    assert (info.owner, info.version, info.timeout_ms, info.max_attempts) == (
        "payments",
        2,
        5000,
        3,
    )
    assert (info.created_at_ms, info.ready, info.ready_capped, info.too_many_runs) == (
        77,
        4,
        False,
        False,
    )
    assert (info.running, info.completed, info.failed, info.timed_out) == (1, 9, 2, 0)
    assert (info.latency_count, info.latency_p95_ms, info.latency_max_ms) == (9, 90, 120)
    # Too many runs: no counts.
    capped = head[:-2] + b"\x01\x01" + b"\x00" * 9
    info = decode_action_info_answer(capped)
    assert (info.ready_capped, info.too_many_runs, info.running, info.latency_avg_ms) == (
        True,
        True,
        None,
        None,
    )
    with pytest.raises(IncompleteResponseError):
        decode_action_info_answer(capped[:-1])


def test_a_group_list_decodes_each_group() -> None:
    group = _str16("g1") + struct.pack("<IQqIBQQ", 2, 5, 100, 30000, 10, 200, 7)
    answer = struct.pack("<H", 1) + group + b"\x01"
    page = decode_group_list_answer(answer)
    assert page.more is True
    [g] = page.groups
    assert (g.name, g.members, g.pending, g.created_at_ms) == ("g1", 2, 5, 100)
    assert (g.ack_timeout_ms, g.max_deliver, g.last_delivered_ms, g.last_delivered_seq) == (
        30000,
        10,
        200,
        7,
    )
    with pytest.raises(IncompleteResponseError):
        decode_group_list_answer(answer[:-1])


def test_group_members_decodes_each_member() -> None:
    assert encode_group_members_body("g1") == _str16("g1")
    active = _str16("c1") + b"\x00" + struct.pack("<Iq", 3, 50) + b"\x01" + struct.pack("<q", 60)
    leaving = _str16("c2") + b"\x01" + struct.pack("<Iq", 0, 51) + b"\x00"
    page = decode_group_members_answer(struct.pack("<H", 2) + active + leaving + b"\x00")
    assert page.more is False
    assert [(m.id, m.state, m.pending, m.joined_at_ms, m.last_active_ms) for m in page.members] == [
        ("c1", GroupMemberState.ACTIVE, 3, 50, 60),
        ("c2", GroupMemberState.LEAVING, 0, 51, None),
    ]
    with pytest.raises(IncompleteResponseError):
        decode_group_members_answer(struct.pack("<H", 1) + leaving.replace(b"\x01", b"\x02", 1))


def test_a_peek_sends_its_limit_and_decodes_the_messages() -> None:
    assert encode_peek_body(20) == b"\x01" + struct.pack("<I", 20)
    for bad in (0, 101):
        with pytest.raises(ValidationError):
            encode_peek_body(bad)
    msg = struct.pack("<QIIqI", 9, 0, 1, 500, 5000) + _str16("hello")
    page = decode_peek_answer(b"\x01" + msg + b"\x01")
    assert page.more is True
    [m] = page.messages
    assert (m.seq, m.priority, m.attempts, m.enqueued_at_ms, m.size, m.payload) == (
        9,
        0,
        1,
        500,
        5000,
        b"hello",
    )


def _dead(seq: int, payload: bytes) -> bytes:
    return (
        struct.pack("<QII", seq, len(payload) + 10, len(payload))
        + payload
        + struct.pack("<qIBq", 100, 3, 2, 200)
    )


def test_a_dlq_page_decodes_its_letters_and_trailer() -> None:
    data = struct.pack("<I", 2) + _dead(4, b"a") + _dead(6, b"bc") + struct.pack("<QBQ", 7, 1, 6)
    page = parse_dlq_page(data)
    assert [(d.seq, d.size, d.payload, d.attempts, d.priority) for d in page.messages] == [
        (4, 11, b"a", 3, 2),
        (6, 12, b"bc", 3, 2),
    ]
    assert (page.total, page.has_more, page.last_seq) == (7, True, 6)
    for bad in (data[:-1], data + b"\x00", data[:-9] + b"\x02" + data[-8:]):
        with pytest.raises(IncompleteResponseError):
            parse_dlq_page(bad)


async def test_dlq_list_sends_its_page_in_the_value() -> None:
    seen: list[bytes] = []

    def reply(_n: int, _op: int, rid: int) -> bytes:
        return _frame(rid, StatusCode.OK, struct.pack("<I", 0) + struct.pack("<QBQ", 0, 0, 0))

    class Recording(_Scripted):
        async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            self.writers.append(writer)
            try:
                while True:
                    header = await reader.readexactly(HEADER_SIZE)
                    _, n, rid, _, _op, *_ = struct.unpack(REQUEST_HEADER_FORMAT, header)
                    seen.append(await reader.readexactly(n))
                    writer.write(reply(0, 0, rid))
            except (asyncio.IncompleteReadError, ConnectionError):
                writer.close()

    from flo import DlqListOptions

    fake = Recording(reply)
    server, addr = await fake.serve()
    client = await FloClient(addr, timeout_ms=1000).connect()
    try:
        await client.queue.dlq_list("q", DlqListOptions(namespace="ns", limit=10, after=6))
        await client.queue.dlq_list("q", DlqListOptions(namespace="ns"))
    finally:
        await client.close()
        await fake.shut(server)
    # [ns][key][value_len:u32][value][options_len:u16], no options.
    prefix = _str16("ns") + _str16("q")
    assert seen[0] == prefix + struct.pack("<IIQH", 12, 10, 6, 0)
    assert seen[1] == prefix + struct.pack("<IIH", 4, 100, 0)


# ── Refusals ────────────────────────────────────────────────────────────────


async def _complete_against(*replies: bytes) -> tuple[_Scripted, BaseException | None]:
    """Complete TASK against a server answering with replies in turn (each
    filled in with the request id); returns the fake and what was raised."""
    fake = _Scripted(lambda n, _op, rid: _frame(rid, *_unpack(replies[min(n, len(replies) - 1)])))
    server, addr = await fake.serve()
    policy = RetryPolicy(max_attempts=4, initial_backoff_ms=1, max_backoff_ms=2)
    client = await FloClient(addr, timeout_ms=1000, retry=policy).connect()
    raised: BaseException | None = None
    try:
        await client.worker.complete("w1", TASK, b"pong")
    except Exception as e:
        raised = e
    finally:
        await client.close()
        await fake.shut(server)
    return fake, raised


def _answer(status: StatusCode, data: bytes = b"") -> bytes:
    return bytes([status]) + data


def _unpack(reply: bytes) -> tuple[StatusCode, bytes]:
    return StatusCode(reply[0]), reply[1:]


def _refusal(reason: Reason, ran: Ran, message: bytes) -> bytes:
    return struct.pack("<HB", reason, ran) + message


async def test_a_leader_not_ready_is_retried_and_the_report_then_applies() -> None:
    not_ready = _answer(
        StatusCode.UNAVAILABLE,
        _refusal(Reason.LEADER_UNKNOWN, Ran.NO, b"unavailable: leadership isn't settled"),
    )
    fake, raised = await _complete_against(not_ready, not_ready, _answer(StatusCode.OK))
    assert raised is None
    assert fake.ops == [OpCode.ACTION_COMPLETE] * 3
    assert len(set(fake.ids)) == 3  # each try a new request


async def test_a_leader_not_ready_past_the_retries_is_raised_unavailable() -> None:
    not_ready = _answer(StatusCode.UNAVAILABLE, _refusal(Reason.LEADER_UNKNOWN, Ran.NO, b"retry"))
    fake, raised = await _complete_against(not_ready)
    assert isinstance(raised, UnavailableError)
    assert (raised.reason, raised.ran) == (Reason.LEADER_UNKNOWN, Ran.NO)
    assert len(fake.ids) == 4


async def test_a_stale_report_is_its_own_conflict_and_is_not_retried() -> None:
    message = b"stale report: the run was completed with another result"
    stale = _answer(StatusCode.CONFLICT, _refusal(Reason.STALE_REPORT, Ran.NO, message))
    fake, raised = await _complete_against(stale)
    assert isinstance(raised, StaleReportError)
    assert isinstance(raised, ConflictError)
    assert str(raised) == message.decode()
    assert len(fake.ids) == 1


async def test_another_conflict_is_not_a_stale_report() -> None:
    other = _answer(StatusCode.CONFLICT, _refusal(Reason.CAS_MISMATCH, Ran.NO, b"cas"))
    _, raised = await _complete_against(other)
    assert isinstance(raised, ConflictError)
    assert not isinstance(raised, StaleReportError)


async def test_reports_are_keyed_by_the_run_in_the_tasks_namespace() -> None:
    payloads: list[tuple[int, bytes]] = []

    class Recording(_Scripted):
        async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            self.writers.append(writer)
            try:
                while True:
                    header = await reader.readexactly(HEADER_SIZE)
                    _, n, rid, _, op, *_ = struct.unpack(REQUEST_HEADER_FORMAT, header)
                    payloads.append((op, await reader.readexactly(n)))
                    answer = struct.pack("<q", 9) if op == OpCode.ACTION_TOUCH else b""
                    writer.write(_frame(rid, StatusCode.OK, answer))
            except (asyncio.IncompleteReadError, ConnectionError):
                writer.close()

    task = dataclasses.replace(TASK, namespace="billing")
    fake = Recording(lambda *_: b"")
    server, addr = await fake.serve()
    client = await FloClient(addr, namespace="other", timeout_ms=1000).connect()
    try:
        assert await client.worker.touch("w1", task) == 9
        await client.worker.complete("w1", task, b"pong")
        await client.worker.fail("w1", task, "boom", WorkerFailOptions(abandoned=True))
    finally:
        await client.close()
        await fake.shut(server)
    for _op, payload in payloads:
        assert payload.startswith(_str16("billing") + _str16(task.run))
    assert [op for op, _ in payloads] == [
        OpCode.ACTION_TOUCH,
        OpCode.ACTION_COMPLETE,
        OpCode.ACTION_FAIL,
    ]


async def test_an_await_sends_its_body_on_the_workers_key() -> None:
    payloads: list[bytes] = []

    class Recording(_Scripted):
        async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            self.writers.append(writer)
            try:
                while True:
                    header = await reader.readexactly(HEADER_SIZE)
                    _, n, rid, _, _op, *_ = struct.unpack(REQUEST_HEADER_FORMAT, header)
                    payloads.append(await reader.readexactly(n))
                    writer.write(_frame(rid, StatusCode.OK, task_answer(TASK)))
            except (asyncio.IncompleteReadError, ConnectionError):
                writer.close()

    fake = Recording(lambda *_: b"")
    server, addr = await fake.serve()
    client = await FloClient(addr, namespace="ns", timeout_ms=1000).connect()
    try:
        got = await client.worker.await_task(
            "w1", ["charge"], WorkerAwaitOptions(block_ms=0, request_key=b"rk")
        )
    finally:
        await client.close()
        await fake.shut(server)
    assert got.task == TASK
    assert payloads == [_str16("ns") + _str16("w1") + encode_await_body(["charge"], 0, b"rk")]


async def test_a_worker_resends_an_await_whose_answer_was_lost_with_the_same_key() -> None:
    # The first await's connection drops unanswered; the next is answered
    # empty; later ones park.
    awaits: list[bytes] = []
    writers: list[asyncio.StreamWriter] = []

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writers.append(writer)
        try:
            while True:
                header = await reader.readexactly(HEADER_SIZE)
                _, n, rid, _, op, *_ = struct.unpack(REQUEST_HEADER_FORMAT, header)
                payload = await reader.readexactly(n)
                if op == OpCode.ACTION_AWAIT:
                    awaits.append(payload)
                    if len(awaits) == 1:
                        writer.close()
                        return
                    if len(awaits) == 2:
                        writer.write(_frame(rid, StatusCode.OK, NO_TASK))
                    continue
                answer = b"\x00" if op == OpCode.WORKER_HEARTBEAT else b""
                writer.write(_frame(rid, StatusCode.OK, answer))
        except (asyncio.IncompleteReadError, ConnectionError):
            pass

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    worker = FloClient(f"127.0.0.1:{port}", timeout_ms=1000).new_action_worker(block_ms=100)

    async def noop(_ctx: object) -> bytes:
        return b""

    worker.register_action("charge", noop)
    run = asyncio.create_task(worker.start())
    try:
        for _ in range(500):
            if len(awaits) >= 3:
                break
            await asyncio.sleep(0.01)
    finally:
        worker.stop()
        await asyncio.wait_for(run, timeout=10)
        server.close()
        for w in writers:
            w.close()
        await server.wait_closed()

    def key(payload: bytes) -> bytes:
        # The body's last field, [present:u8][len:u8][key], with a 32-byte key.
        assert payload[-34:-32] == b"\x01\x20"
        return payload[-32:]

    assert len(awaits) >= 3
    first, again, after = (key(p) for p in awaits[:3])
    assert first and first == again
    assert after and after != first
