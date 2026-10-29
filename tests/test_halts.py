"""A halted shard: a write it committed but hasn't applied comes back as
outcome_pending, a write it refuses as unavailable with reason shard_halted,
and neither is sent again; its reads succeed, marked stale. Workers keep
asking a halted shard, backing off, and resume once it is repaired. Runs
against an in-process fake server."""

import asyncio
import logging
import struct
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from flo import FloClient, RetryPolicy
from flo import retry as retry_mod
from flo.exceptions import (
    IncompleteResponseError,
    NotFoundError,
    OutcomePendingError,
    ServerError,
    StaleNotFoundError,
    UnavailableError,
)
from flo.types import (
    MAGIC,
    RESPONSE_STALE,
    TABLE_HASH,
    VERSION,
    OpCode,
    Ran,
    Reason,
    StatusCode,
)
from flo.wire import RESPONSE_HEADER_FORMAT, compute_crc32c
from flo.worker import ActionContext

from .test_result_resend import ACTION, _task, _until
from .test_retry import _frame, _Scripted
from .test_stream_worker_ack import _one_record

_FAST = RetryPolicy(max_attempts=4, initial_backoff_ms=1, max_backoff_ms=2)


def _halted(request_id: int, status: StatusCode, ran: Ran, message: bytes) -> bytes:
    body = int(Reason.SHARD_HALTED).to_bytes(2, "little") + bytes([int(ran)]) + message
    return _frame(request_id, status, body)


def _flagged(request_id: int, flags: int, data: bytes, status: StatusCode = StatusCode.OK) -> bytes:
    def header(crc: int) -> bytes:
        return struct.pack(
            RESPONSE_HEADER_FORMAT,
            MAGIC,
            len(data),
            request_id,
            crc,
            VERSION,
            status,
            flags,
            0,
            TABLE_HASH,
        )

    return header(compute_crc32c(header(0), data)) + data


async def _against(fake: _Scripted, call: Callable[[FloClient], Awaitable[Any]]) -> Any:
    server, addr = await fake.serve()
    client = await FloClient(addr, timeout_ms=1000, retry=_FAST).connect()
    try:
        return await call(client)
    finally:
        await client.close()
        await fake.shut(server)


# ── outcome_pending and shard_halted are never retried ─────────────────────


async def test_outcome_pending_surfaces_after_exactly_one_attempt() -> None:
    message = b"committed at 7, not applied: applies when the shard resumes"
    fake = _Scripted(
        lambda _n, _op, rid: _halted(rid, StatusCode.OUTCOME_PENDING, Ran.UNKNOWN, message)
    )
    with pytest.raises(OutcomePendingError) as e:
        await _against(fake, lambda c: c.kv.put("k", b"v"))
    assert len(fake.ids) == 1
    assert (e.value.status_code, e.value.reason, e.value.ran) == (
        StatusCode.OUTCOME_PENDING,
        Reason.SHARD_HALTED,
        Ran.UNKNOWN,
    )
    assert str(e.value) == message.decode()


async def test_a_write_to_a_halted_shard_surfaces_after_exactly_one_attempt() -> None:
    fake = _Scripted(
        lambda _n, _op, rid: _halted(rid, StatusCode.UNAVAILABLE, Ran.NO, b"shard 2 halted")
    )
    with pytest.raises(UnavailableError) as e:
        await _against(fake, lambda c: c.kv.put("k", b"v"))
    assert len(fake.ids) == 1
    assert (e.value.reason, e.value.ran, str(e.value)) == (
        Reason.SHARD_HALTED,
        Ran.NO,
        "shard 2 halted",
    )


# ── stale reads ────────────────────────────────────────────────────────────

_READS: list[tuple[str, bytes, Callable[[FloClient], Awaitable[Any]], Callable[[Any], bool]]] = [
    ("get", struct.pack("<QI", 1, 1) + b"v\x00", lambda c: c.kv.get("k"), lambda r: r.stale),
    (
        "json_get",
        struct.pack("<QI", 1, 2) + b"{}",
        lambda c: c.kv.json_get("k"),
        lambda r: r.stale,
    ),
    (
        "mget",
        struct.pack("<H", 1) + b"\x01" + struct.pack("<QI", 1, 1) + b"v" + b"\x00",
        lambda c: c.kv.mget(["k"]),
        lambda r: r[0].stale,
    ),
    ("scan", struct.pack("<H", 0) + b"\x00", lambda c: c.kv.scan(), lambda r: r.stale),
    ("history", b"\x00\x00", lambda c: c.kv.history("k"), lambda r: r.stale),
    ("exists", b"\x01" + struct.pack("<Q", 1), lambda c: c.kv.exists("k"), lambda r: r.stale),
]


@pytest.mark.parametrize("stale", [True, False])
@pytest.mark.parametrize(
    ("_name", "answer", "read", "is_stale"), _READS, ids=[r[0] for r in _READS]
)
async def test_a_read_says_whether_a_halted_shard_answered_it(
    stale: bool,
    _name: str,
    answer: bytes,
    read: Callable[[FloClient], Awaitable[Any]],
    is_stale: Callable[[Any], bool],
) -> None:
    flags = RESPONSE_STALE if stale else 0
    fake = _Scripted(lambda _n, _op, rid: _flagged(rid, flags, answer))
    assert is_stale(await _against(fake, read)) is stale


@pytest.mark.parametrize("bit", [0x02, 0x80])
async def test_an_unknown_flag_bit_is_refused(bit: int) -> None:
    answer = struct.pack("<QI", 1, 1) + b"v\x00"
    fake = _Scripted(lambda _n, _op, rid: _flagged(rid, RESPONSE_STALE | bit, answer))
    with pytest.raises(IncompleteResponseError, match="flags"):
        await _against(fake, lambda c: c.kv.get("k"))


# ── an absent key on a stale answer ───────────────────────────────────────


def _not_found(request_id: int, flags: int) -> bytes:
    body = int(Reason.NOT_FOUND).to_bytes(2, "little") + bytes([int(Ran.NO)]) + b"key not found"
    return _flagged(request_id, flags, body, StatusCode.NOT_FOUND)


_ABSENT_READS: list[tuple[str, Callable[[FloClient], Awaitable[Any]]]] = [
    ("get", lambda c: c.kv.get("k")),
    ("json_get", lambda c: c.kv.json_get("k")),
]


@pytest.mark.parametrize(("_name", "read"), _ABSENT_READS, ids=[r[0] for r in _ABSENT_READS])
async def test_a_key_absent_on_a_stale_answer_is_not_reported_absent(
    _name: str, read: Callable[[FloClient], Awaitable[Any]]
) -> None:
    fake = _Scripted(lambda _n, _op, rid: _not_found(rid, RESPONSE_STALE))
    with pytest.raises(StaleNotFoundError) as e:
        await _against(fake, read)
    assert isinstance(e.value, NotFoundError)
    assert (e.value.stale, e.value.reason, str(e.value)) == (
        True,
        Reason.NOT_FOUND,
        "key not found",
    )


@pytest.mark.parametrize(("_name", "read"), _ABSENT_READS, ids=[r[0] for r in _ABSENT_READS])
async def test_a_key_absent_on_a_fresh_answer_is_absent(
    _name: str, read: Callable[[FloClient], Awaitable[Any]]
) -> None:
    fake = _Scripted(lambda _n, _op, rid: _not_found(rid, 0))
    assert await _against(fake, read) is None


async def test_history_exists_and_mget_keep_the_stale_mark_on_an_absent_key() -> None:
    fake = _Scripted(lambda _n, _op, rid: _not_found(rid, RESPONSE_STALE))
    history = await _against(fake, lambda c: c.kv.history("k"))
    assert (history.versions, history.more, history.stale) == ([], False, True)

    absent = b"\x00" + struct.pack("<Q", 0)
    fake = _Scripted(lambda _n, _op, rid: _flagged(rid, RESPONSE_STALE, absent))
    exists = await _against(fake, lambda c: c.kv.exists("k"))
    assert (exists.exists, exists.version, exists.stale) == (False, 0, True)

    missing = struct.pack("<H", 1) + b"\x00" + struct.pack("<QI", 0, 0) + b"\x00"
    fake = _Scripted(lambda _n, _op, rid: _flagged(rid, RESPONSE_STALE, missing))
    (entry,) = await _against(fake, lambda c: c.kv.mget(["k"]))
    assert (entry.found, entry.stale) == (False, True)


async def test_a_refusal_on_a_stale_answer_says_so() -> None:
    body = int(Reason.UNCLASSIFIED).to_bytes(2, "little") + bytes([int(Ran.NO)]) + b"bad cursor"
    fake = _Scripted(
        lambda _n, _op, rid: _flagged(rid, RESPONSE_STALE, body, StatusCode.BAD_REQUEST)
    )
    with pytest.raises(ServerError) as e:
        await _against(fake, lambda c: c.kv.scan())
    assert e.value.stale is True


# ── workers keep asking a halted shard ─────────────────────────────────────

_HALTED_FOR = 3


async def test_an_action_worker_waits_out_a_halted_shard_and_resumes(
    caplog: pytest.LogCaptureFixture,
) -> None:
    awaits = 0
    completes: list[int] = []

    def reply(_n: int, op: int, rid: int) -> bytes:
        nonlocal awaits
        if op == OpCode.ACTION_AWAIT:
            awaits += 1
            if awaits <= _HALTED_FOR:
                return _halted(rid, StatusCode.UNAVAILABLE, Ran.NO, b"shard 0 halted")
            if awaits == _HALTED_FOR + 1:
                return _frame(rid, StatusCode.OK, _task())
            return b""  # park, as a long poll holds its connection
        if op == OpCode.ACTION_COMPLETE:
            completes.append(rid)
        if op == OpCode.WORKER_HEARTBEAT:
            return _frame(rid, StatusCode.OK, b"\x00")
        return _frame(rid, StatusCode.OK, b"")

    fake = _Scripted(reply)
    server, addr = await fake.serve()
    client = FloClient(addr, timeout_ms=1000, retry=_FAST)
    worker = client.new_action_worker(block_ms=30000)

    async def done(_ctx: ActionContext) -> bytes:
        return b"done"

    worker.register_action(ACTION, done)
    caplog.set_level(logging.WARNING, logger="flo")
    run = asyncio.create_task(worker.start())
    try:
        await _until(lambda: bool(completes), timeout=10)
        assert awaits >= _HALTED_FOR + 1
        halted = [r for r in caplog.records if "shard halted" in r.getMessage()]
        assert len(halted) == _HALTED_FOR
        assert all(r.levelno == logging.WARNING for r in halted)
        assert all("shard 0 halted" in r.getMessage() for r in halted)
    finally:
        worker.stop()
        await asyncio.wait_for(run, timeout=10)
        await fake.shut(server)


async def test_a_stream_worker_waits_out_a_halted_shard_to_join_its_group() -> None:
    joins = 0

    def reply(_n: int, op: int, rid: int) -> bytes:
        nonlocal joins
        if op == OpCode.STREAM_GROUP_JOIN:
            joins += 1
            if joins <= _HALTED_FOR:
                return _halted(rid, StatusCode.UNAVAILABLE, Ran.NO, b"shard 0 halted")
        if op == OpCode.STREAM_GROUP_READ:
            worker.stop()
            return _frame(rid, StatusCode.OK, struct.pack("<I", 0))
        return _frame(rid, StatusCode.OK, b"")

    fake = _Scripted(reply)
    server, addr = await fake.serve()
    parent = FloClient(addr, timeout_ms=1000, retry=_FAST)

    async def handler(_ctx: object) -> None:
        return None

    worker = parent.new_stream_worker(stream="s", group="g", handler=handler, block_ms=100)
    try:
        await asyncio.wait_for(worker.start(), timeout=10)
        assert joins == _HALTED_FOR + 1
    finally:
        await fake.shut(server)


@pytest.mark.parametrize("op", [OpCode.ACTION_REGISTER, OpCode.WORKER_REGISTER])
async def test_an_action_worker_started_during_a_halt_comes_up_once_repaired(
    op: OpCode, caplog: pytest.LogCaptureFixture
) -> None:
    refused = 0
    completes: list[int] = []

    def reply(_n: int, got: int, rid: int) -> bytes:
        nonlocal refused
        if got == op and refused < _HALTED_FOR:
            refused += 1
            return _halted(rid, StatusCode.UNAVAILABLE, Ran.NO, b"shard 0 halted")
        if got == OpCode.ACTION_AWAIT:
            return _frame(rid, StatusCode.OK, _task()) if not completes else b""
        if got == OpCode.ACTION_COMPLETE:
            completes.append(rid)
        if got == OpCode.WORKER_HEARTBEAT:
            return _frame(rid, StatusCode.OK, b"\x00")
        return _frame(rid, StatusCode.OK, b"")

    fake = _Scripted(reply)
    server, addr = await fake.serve()
    client = FloClient(addr, timeout_ms=1000, retry=_FAST)
    worker = client.new_action_worker(block_ms=30000)

    async def done(_ctx: ActionContext) -> bytes:
        return b"done"

    worker.register_action(ACTION, done)
    caplog.set_level(logging.WARNING, logger="flo")
    run = asyncio.create_task(worker.start())
    try:
        await _until(lambda: bool(completes), timeout=10)
        assert fake.ops.count(op) == _HALTED_FOR + 1
        halted = [r for r in caplog.records if "shard halted" in r.getMessage()]
        assert len(halted) == _HALTED_FOR
        assert all(r.levelno == logging.WARNING for r in halted)
    finally:
        worker.stop()
        await asyncio.wait_for(run, timeout=10)
        await fake.shut(server)


async def test_a_stream_worker_started_during_a_halt_comes_up_once_repaired(
    caplog: pytest.LogCaptureFixture,
) -> None:
    registers = 0
    reads = 0

    def reply(_n: int, op: int, rid: int) -> bytes:
        nonlocal registers, reads
        if op == OpCode.WORKER_REGISTER:
            registers += 1
            if registers <= _HALTED_FOR:
                return _halted(rid, StatusCode.UNAVAILABLE, Ran.NO, b"shard 0 halted")
        if op == OpCode.STREAM_GROUP_READ:
            reads += 1
            worker.stop()
            return _frame(rid, StatusCode.OK, struct.pack("<I", 0))
        if op == OpCode.WORKER_HEARTBEAT:
            return _frame(rid, StatusCode.OK, b"\x00")
        return _frame(rid, StatusCode.OK, b"")

    fake = _Scripted(reply)
    server, addr = await fake.serve()
    parent = FloClient(addr, timeout_ms=1000, retry=_FAST)

    async def handler(_ctx: object) -> None:
        return None

    worker = parent.new_stream_worker(stream="s", group="g", handler=handler, block_ms=100)
    caplog.set_level(logging.WARNING, logger="flo")
    try:
        await asyncio.wait_for(worker.start(), timeout=10)
        assert (registers, reads) == (_HALTED_FOR + 1, 1)
        halted = [r for r in caplog.records if "shard halted" in r.getMessage()]
        assert len(halted) == _HALTED_FOR
        assert all(r.levelno == logging.WARNING for r in halted)
    finally:
        await fake.shut(server)


# ── outcome_pending ends a worker's start; it is never resent ─────────────


def _pending(request_id: int) -> bytes:
    return _halted(
        request_id,
        StatusCode.OUTCOME_PENDING,
        Ran.UNKNOWN,
        b"halted at 9: a repair skips it; don't resend it unchanged",
    )


async def test_an_action_registration_answered_outcome_pending_ends_start_after_one_try() -> None:
    def reply(_n: int, op: int, rid: int) -> bytes:
        if op == OpCode.ACTION_REGISTER:
            return _pending(rid)
        return _frame(rid, StatusCode.OK, b"")

    fake = _Scripted(reply)
    server, addr = await fake.serve()
    worker = FloClient(addr, timeout_ms=1000, retry=_FAST).new_action_worker(block_ms=100)

    async def done(_ctx: ActionContext) -> bytes:
        return b"done"

    worker.register_action(ACTION, done)
    try:
        with pytest.raises(OutcomePendingError, match="a repair skips it"):
            await asyncio.wait_for(worker.start(), timeout=5)
        assert fake.ops.count(OpCode.ACTION_REGISTER) == 1
        assert OpCode.WORKER_REGISTER not in fake.ops
    finally:
        await fake.shut(server)


async def test_a_group_join_answered_outcome_pending_ends_start_after_one_try() -> None:
    def reply(_n: int, op: int, rid: int) -> bytes:
        if op == OpCode.STREAM_GROUP_JOIN:
            return _pending(rid)
        if op == OpCode.WORKER_HEARTBEAT:
            return _frame(rid, StatusCode.OK, b"\x00")
        return _frame(rid, StatusCode.OK, b"")

    fake = _Scripted(reply)
    server, addr = await fake.serve()
    parent = FloClient(addr, timeout_ms=1000, retry=_FAST)

    async def handler(_ctx: object) -> None:
        return None

    worker = parent.new_stream_worker(stream="s", group="g", handler=handler, block_ms=100)
    try:
        with pytest.raises(OutcomePendingError):
            await asyncio.wait_for(worker.start(), timeout=5)
        assert fake.ops.count(OpCode.STREAM_GROUP_JOIN) == 1
    finally:
        await fake.shut(server)


# ── a stop or drain ends a halted wait at once ─────────────────────────────


@pytest.mark.parametrize("how", ["stop", "drain"])
@pytest.mark.parametrize("kind", ["action-registration", "group-join"])
async def test_a_stop_or_drain_ends_a_halted_wait_promptly(kind: str, how: str) -> None:
    halted_op = (
        OpCode.ACTION_REGISTER if kind == "action-registration" else OpCode.STREAM_GROUP_JOIN
    )

    def reply(_n: int, op: int, rid: int) -> bytes:
        if op == halted_op:
            return _halted(rid, StatusCode.UNAVAILABLE, Ran.NO, b"shard 0 halted")
        if op == OpCode.WORKER_HEARTBEAT:
            return _frame(rid, StatusCode.OK, b"\x00")
        return _frame(rid, StatusCode.OK, b"")

    fake = _Scripted(reply)
    server, addr = await fake.serve()
    parent = FloClient(addr, timeout_ms=1000, retry=_FAST)
    worker: Any
    if kind == "action-registration":
        worker = parent.new_action_worker(block_ms=100)

        async def done(_ctx: ActionContext) -> bytes:
            return b"done"

        worker.register_action(ACTION, done)
    else:

        async def handler(_ctx: object) -> None:
            return None

        worker = parent.new_stream_worker(stream="s", group="g", handler=handler, block_ms=100)
    run = asyncio.create_task(worker.start())
    try:
        await _until(lambda: fake.ops.count(halted_op) >= 2, timeout=10)
        getattr(worker, how)()
        await asyncio.wait_for(run, timeout=2)
        asked = fake.ops.count(halted_op)
        await asyncio.sleep(0.3)
        assert fake.ops.count(halted_op) == asked
    finally:
        if not run.done():
            run.cancel()
        await fake.shut(server)


# ── results and acks are never resent on a halt ────────────────────────────


@pytest.mark.parametrize("answer", ["halted", "pending"])
@pytest.mark.parametrize("outcome", ["complete", "fail"])
async def test_an_action_result_refused_by_a_halt_is_sent_once(outcome: str, answer: str) -> None:
    result_op = OpCode.ACTION_COMPLETE if outcome == "complete" else OpCode.ACTION_FAIL
    awaits = 0

    def reply(_n: int, op: int, rid: int) -> bytes:
        nonlocal awaits
        if op == OpCode.ACTION_AWAIT:
            awaits += 1
            return _frame(rid, StatusCode.OK, _task()) if awaits == 1 else b""
        if op == result_op:
            if answer == "halted":
                return _halted(rid, StatusCode.UNAVAILABLE, Ran.NO, b"shard 0 halted")
            return _pending(rid)
        if op == OpCode.WORKER_HEARTBEAT:
            return _frame(rid, StatusCode.OK, b"\x00")
        return _frame(rid, StatusCode.OK, b"")

    fake = _Scripted(reply)
    server, addr = await fake.serve()
    worker = FloClient(addr, timeout_ms=1000, retry=_FAST).new_action_worker(block_ms=30000)

    async def handle(_ctx: ActionContext) -> bytes:
        if outcome == "fail":
            raise RuntimeError("no")
        return b"done"

    worker.register_action(ACTION, handle)
    run = asyncio.create_task(worker.start())
    try:
        await _until(lambda: result_op in fake.ops, timeout=10)
        await asyncio.sleep(0.5)
        assert fake.ops.count(result_op) == 1
    finally:
        worker.stop()
        await asyncio.wait_for(run, timeout=10)
        await fake.shut(server)


@pytest.mark.parametrize("answer", ["halted", "pending"])
async def test_a_stream_ack_refused_by_a_halt_is_sent_once(answer: str) -> None:
    reads = 0

    def reply(_n: int, op: int, rid: int) -> bytes:
        nonlocal reads
        if op == OpCode.STREAM_GROUP_READ:
            reads += 1
            return _frame(rid, StatusCode.OK, _one_record()) if reads == 1 else b""
        if op == OpCode.STREAM_GROUP_ACK:
            if answer == "halted":
                return _halted(rid, StatusCode.UNAVAILABLE, Ran.NO, b"shard 0 halted")
            return _pending(rid)
        if op == OpCode.WORKER_HEARTBEAT:
            return _frame(rid, StatusCode.OK, b"\x00")
        return _frame(rid, StatusCode.OK, b"")

    fake = _Scripted(reply)
    server, addr = await fake.serve()

    async def handler(_ctx: object) -> None:
        return None

    worker = FloClient(addr, timeout_ms=1000, retry=_FAST).new_stream_worker(
        stream="s", group="g", handler=handler, block_ms=30000
    )
    run = asyncio.create_task(worker.start())
    try:
        await _until(lambda: OpCode.STREAM_GROUP_ACK in fake.ops, timeout=10)
        await asyncio.sleep(0.5)
        assert fake.ops.count(OpCode.STREAM_GROUP_ACK) == 1
        assert OpCode.STREAM_GROUP_NACK not in fake.ops
    finally:
        worker.stop()
        await asyncio.wait_for(run, timeout=10)
        await fake.shut(server)


# ── the halt backoff's cap ─────────────────────────────────────────────────


def test_the_halt_backoff_reaches_30_seconds_and_never_passes_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(retry_mod.random, "uniform", lambda _lo, hi: hi)
    b = retry_mod._halted_backoff()
    waits = [b.next() for _ in range(20)]
    assert max(waits) == 30.0
    assert waits[-1] == 30.0
