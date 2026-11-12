"""A request the server refused without running it is sent again, as a new
request; anything else comes straight back. Runs against an in-process fake
server."""

import asyncio
import logging
import struct
from collections.abc import Callable

import pytest

from flo import FloClient, RetryPolicy
from flo import retry as retry_mod
from flo.exceptions import (
    ConflictError,
    IncompleteResponseError,
    LimitError,
    OverloadedError,
    ServerError,
    UnavailableError,
)
from flo.retry import _Backoff, _policy_backoff
from flo.types import HEADER_SIZE, MAGIC, TABLE_HASH, VERSION, OpCode, Ran, Reason, StatusCode
from flo.wire import REQUEST_HEADER_FORMAT, RESPONSE_HEADER_FORMAT, compute_crc32c

from .test_stream_worker_ack import _records


def _frame(request_id: int, status: int, data: bytes) -> bytes:
    def header(crc: int) -> bytes:
        return struct.pack(
            RESPONSE_HEADER_FORMAT,
            MAGIC,
            len(data),
            request_id,
            crc,
            VERSION,
            status,
            0,
            0,
            TABLE_HASH,
        )

    return header(compute_crc32c(header(0), data)) + data


def _refusal(request_id: int, status: StatusCode, ran: Ran) -> bytes:
    body = int(Reason.UNCLASSIFIED).to_bytes(2, "little") + bytes([int(ran)]) + b"busy"
    return _frame(request_id, status, body)


def _kv_value(request_id: int) -> bytes:
    return _frame(request_id, StatusCode.OK, struct.pack("<QI", 1, 1) + b"v\x00")


Reply = Callable[[int, int, int], bytes]


class _Scripted:
    """Answers the nth request (from 0) with reply(n, op_code, request_id) and
    records every request id it saw."""

    def __init__(self, reply: Reply) -> None:
        self.reply = reply
        self.ids: list[int] = []
        self.ops: list[int] = []
        self.writers: list[asyncio.StreamWriter] = []

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.writers.append(writer)
        try:
            while True:
                header = await reader.readexactly(HEADER_SIZE)
                _, payload_len, request_id, _, op_code, *_ = struct.unpack(
                    REQUEST_HEADER_FORMAT, header
                )
                await reader.readexactly(payload_len)
                n = len(self.ids)
                self.ids.append(request_id)
                self.ops.append(op_code)
                writer.write(self.reply(n, op_code, request_id))
        except (asyncio.IncompleteReadError, ConnectionError):
            writer.close()

    async def serve(self) -> tuple[asyncio.Server, str]:
        server = await asyncio.start_server(self.handle, "127.0.0.1", 0)
        return server, f"127.0.0.1:{server.sockets[0].getsockname()[1]}"

    async def shut(self, server: asyncio.Server) -> None:
        server.close()
        for writer in self.writers:
            writer.close()
        await server.wait_closed()


_FAST = RetryPolicy(max_attempts=4, initial_backoff_ms=1, max_backoff_ms=2)


@pytest.mark.parametrize("status", [StatusCode.OVERLOADED, StatusCode.UNAVAILABLE])
async def test_a_refusal_that_did_not_run_is_retried_as_a_new_request(status: StatusCode) -> None:
    fake = _Scripted(
        lambda n, _op, rid: _refusal(rid, status, Ran.NO) if n == 0 else _kv_value(rid)
    )
    server, addr = await fake.serve()
    client = await FloClient(addr, timeout_ms=1000, retry=_FAST).connect()
    try:
        result = await client.kv.get("k")
        assert result is not None
        assert result.value == b"v"
        assert len(fake.ids) == 2
        assert fake.ids[0] != fake.ids[1]
    finally:
        await client.close()
        await fake.shut(server)


@pytest.mark.parametrize(
    ("reply", "error"),
    [
        (lambda rid: _refusal(rid, StatusCode.OVERLOADED, Ran.UNKNOWN), OverloadedError),
        (lambda rid: _refusal(rid, StatusCode.UNAVAILABLE, Ran.YES), UnavailableError),
        (lambda rid: _refusal(rid, StatusCode.CONFLICT, Ran.NO), ConflictError),
        (lambda rid: _frame(rid, StatusCode.OVERLOADED, b"\x00\x00"), IncompleteResponseError),
    ],
    ids=["overloaded-may-have-run", "unavailable-ran", "conflict-not-run", "no-ran-byte"],
)
async def test_only_refusals_that_did_not_run_are_retried(
    reply: Callable[[int], bytes], error: type[Exception]
) -> None:
    fake = _Scripted(lambda _n, _op, rid: reply(rid))
    server, addr = await fake.serve()
    client = await FloClient(addr, timeout_ms=1000, retry=_FAST).connect()
    try:
        with pytest.raises(error):
            await client.kv.get("k")
        assert len(fake.ids) == 1
    finally:
        await client.close()
        await fake.shut(server)


@pytest.mark.parametrize("attempts", [4, 1])
async def test_retries_stop_at_max_attempts(attempts: int) -> None:
    fake = _Scripted(lambda _n, _op, rid: _refusal(rid, StatusCode.OVERLOADED, Ran.NO))
    server, addr = await fake.serve()
    policy = RetryPolicy(max_attempts=attempts, initial_backoff_ms=1, max_backoff_ms=2)
    client = await FloClient(addr, timeout_ms=1000, retry=policy).connect()
    try:
        with pytest.raises(OverloadedError) as raised:
            await client.kv.get("k")
        assert raised.value.ran == Ran.NO
        assert len(fake.ids) == attempts
    finally:
        await client.close()
        await fake.shut(server)


def test_backoff_stays_under_its_doubling_ceiling() -> None:
    b = _policy_backoff(RetryPolicy())
    ceilings = [0.05, 0.1, 0.2, 0.4, 0.8, 1.0, 1.0]
    for _ in range(200):
        b.reset()
        for c in ceilings:
            assert 0 <= b.next() <= c


@pytest.mark.parametrize(
    ("policy", "ceiling_ms"),
    [
        (RetryPolicy(initial_backoff_ms=-50, max_backoff_ms=-1), 0),
        (RetryPolicy(initial_backoff_ms=-50, max_backoff_ms=20), 20),
        (RetryPolicy(initial_backoff_ms=100, max_backoff_ms=10), 100),
        (RetryPolicy(initial_backoff_ms=float("nan"), max_backoff_ms=float("nan")), 0),
        (RetryPolicy(initial_backoff_ms=float("inf"), max_backoff_ms=20), 20),
        (RetryPolicy(initial_backoff_ms=40, max_backoff_ms=float("inf")), 40),
    ],
    ids=[
        "both-negative",
        "initial-negative",
        "max-below-initial",
        "both-nan",
        "initial-inf",
        "max-inf",
    ],
)
def test_a_policy_is_normalised_so_waits_stay_in_bounds(
    policy: RetryPolicy, ceiling_ms: float
) -> None:
    assert policy.initial_backoff_ms >= 0
    assert policy.max_backoff_ms >= policy.initial_backoff_ms
    assert policy.max_backoff_ms == ceiling_ms
    b = _policy_backoff(policy)
    for _ in range(50):
        assert 0 <= b.next() <= ceiling_ms / 1000.0


@pytest.mark.parametrize(
    ("given", "attempts"),
    [(float("nan"), 1), (float("inf"), 1), (3.7, 3), (0, 0)],
    ids=["nan", "inf", "fraction", "zero"],
)
def test_max_attempts_is_a_whole_finite_count(given: float, attempts: int) -> None:
    assert RetryPolicy(max_attempts=given).max_attempts == attempts  # type: ignore[arg-type]


def test_a_server_error_not_from_a_refusal_is_not_taken_as_unrun() -> None:
    from flo.exceptions import OverloadedError
    from flo.retry import is_refused_unrun

    assert not is_refused_unrun(OverloadedError("built here"))


def test_the_backoff_helper_normalises_its_own_bounds() -> None:
    b = _Backoff(-1.0, -2.0)
    assert all(b.next() == 0 for _ in range(5))


async def test_stream_worker_retries_a_refused_group_create() -> None:
    creates = 0
    policies: list[RetryPolicy] = []

    def reply(_n: int, op: int, rid: int) -> bytes:
        nonlocal creates
        if op == OpCode.STREAM_GROUP_CREATE:
            creates += 1
            if creates <= 3:
                return _refusal(rid, StatusCode.OVERLOADED, Ran.NO)
            return _frame(rid, StatusCode.OK, b"\x01")
        if op == OpCode.STREAM_GROUP_READ:
            policies.extend(c._retry for c in (worker._client, worker._ack_client) if c)
            worker.stop()
            return _frame(rid, StatusCode.OK, _records(0))
        return _frame(rid, StatusCode.OK, b"")

    fake = _Scripted(reply)
    server, addr = await fake.serve()
    # Retries off in the client, so the worker's own backoff absorbs the refusals.
    parent = FloClient(addr, timeout_ms=1000, retry=RetryPolicy(max_attempts=1))

    async def handler(_ctx: object) -> None:
        return None

    worker = parent.new_stream_worker(
        stream="s", group="g", handler=handler, block_ms=100, ack_timeout_ms=60_000
    )
    try:
        await asyncio.wait_for(worker.start(), timeout=10)
        assert creates == 4
        # The worker's connections use the client's policy.
        assert policies == [parent._retry, parent._retry]
    finally:
        await fake.shut(server)


async def test_stream_worker_still_fails_on_other_create_refusals() -> None:
    def reply(_n: int, op: int, rid: int) -> bytes:
        if op == OpCode.STREAM_GROUP_CREATE:
            return _refusal(rid, StatusCode.OVERLOADED, Ran.UNKNOWN)
        return _frame(rid, StatusCode.OK, b"")

    fake = _Scripted(reply)
    server, addr = await fake.serve()
    parent = FloClient(addr, timeout_ms=1000, retry=RetryPolicy(max_attempts=1))

    async def handler(_ctx: object) -> None:
        return None

    worker = parent.new_stream_worker(
        stream="s", group="g", handler=handler, block_ms=100, max_deliver=3
    )
    try:
        with pytest.raises(ServerError):
            await asyncio.wait_for(worker.start(), timeout=10)
        assert fake.ops.count(OpCode.STREAM_GROUP_CREATE) == 1
    finally:
        await fake.shut(server)


async def test_a_stopped_worker_does_not_sit_out_its_backoff() -> None:
    def reply(_n: int, op: int, rid: int) -> bytes:
        if op == OpCode.STREAM_GROUP_CREATE:
            return _refusal(rid, StatusCode.OVERLOADED, Ran.NO)
        return _frame(rid, StatusCode.OK, b"")

    fake = _Scripted(reply)
    server, addr = await fake.serve()
    parent = FloClient(addr, timeout_ms=1000, retry=RetryPolicy(max_attempts=1))

    async def handler(_ctx: object) -> None:
        return None

    worker = parent.new_stream_worker(
        stream="s", group="g", handler=handler, block_ms=100, ack_timeout_ms=60_000
    )
    try:
        task = asyncio.create_task(worker.start())
        # Long enough for the backoff ceiling to grow well past the deadline below.
        await asyncio.sleep(1.5)
        worker.stop()
        await asyncio.wait_for(task, timeout=0.5)
    finally:
        await fake.shut(server)


# ── limit: terminal, never retried; loops log it once and back off ──────────


def _limit(request_id: int, reason: Reason = Reason.TOO_MANY_ACTIONS) -> bytes:
    body = int(reason).to_bytes(2, "little") + bytes([int(Ran.NO)]) + b"limit: node is full"
    return _frame(request_id, StatusCode.LIMIT, body)


async def test_a_limit_is_never_retried() -> None:
    fake = _Scripted(lambda _n, _op, rid: _limit(rid))
    server, addr = await fake.serve()
    # A policy that would retry an overloaded refusal four times.
    client = await FloClient(addr, timeout_ms=1000, retry=_FAST).connect()
    try:
        with pytest.raises(LimitError) as raised:
            await client.kv.get("k")
        assert len(fake.ids) == 1
        assert (raised.value.reason, raised.value.ran) == (Reason.TOO_MANY_ACTIONS, Ran.NO)
        assert str(raised.value) == "limit: node is full"
    finally:
        await client.close()
        await fake.shut(server)


async def test_an_action_worker_at_the_action_limit_waits_and_says_so_once(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # Registering past the node's actions is refused until one is deleted;
    # the worker backs off and comes up once it isn't, logging the limit once.
    monkeypatch.setattr(retry_mod, "_WORKER_INITIAL_BACKOFF_S", 0.01)
    refused = 4
    registers = 0

    def reply(_n: int, op: int, rid: int) -> bytes:
        nonlocal registers
        if op == OpCode.ACTION_REGISTER:
            registers += 1
            if registers <= refused:
                return _limit(rid)
        if op == OpCode.ACTION_AWAIT:
            worker.stop()
            return b""
        if op == OpCode.WORKER_HEARTBEAT:
            return _frame(rid, StatusCode.OK, b"\x00")
        return _frame(rid, StatusCode.OK, b"")

    fake = _Scripted(reply)
    server, addr = await fake.serve()
    worker = FloClient(addr, timeout_ms=1000, retry=_FAST).new_action_worker(block_ms=100)

    async def done(_ctx: object) -> bytes:
        return b"done"

    worker.register_action("echo", done)
    caplog.set_level(logging.WARNING, logger="flo")
    try:
        await asyncio.wait_for(worker.start(), timeout=10)
    finally:
        await fake.shut(server)
    assert registers == refused + 1
    said = [r for r in caplog.records if "TOO_MANY_ACTIONS" in r.getMessage()]
    assert len(said) == 1 and said[0].levelno == logging.ERROR


async def test_a_stream_worker_at_a_limit_backs_off_without_spinning(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(retry_mod, "_WORKER_INITIAL_BACKOFF_S", 0.01)
    monkeypatch.setattr(retry_mod, "_HALTED_MAX_BACKOFF_S", 0.2)
    reads = 0

    def reply(_n: int, op: int, rid: int) -> bytes:
        nonlocal reads
        if op == OpCode.STREAM_GROUP_READ:
            reads += 1
            # Two reasons in turn: each is said once, however often it comes.
            return _limit(rid, Reason.MEMORY_BUDGET if reads <= 5 else Reason.TOO_MANY_ACTIONS)
        if op == OpCode.WORKER_HEARTBEAT:
            return _frame(rid, StatusCode.OK, b"\x00")
        return _frame(rid, StatusCode.OK, b"")

    fake = _Scripted(reply)
    server, addr = await fake.serve()
    parent = FloClient(addr, timeout_ms=1000, retry=_FAST)

    async def handler(_ctx: object) -> None:
        return None

    worker = parent.new_stream_worker(stream="s", group="g", handler=handler, block_ms=100)
    caplog.set_level(logging.INFO, logger="flo")
    run = asyncio.create_task(worker.start())
    try:
        await _until_true(lambda: reads >= 8, timeout=10)
        assert not run.done()  # it didn't crash
    finally:
        worker.stop()
        await asyncio.wait_for(run, timeout=5)
        await fake.shut(server)
    # Backed off: with a 0.2 s cap it reads a handful of times a second.
    assert reads < 40
    said = [r.getMessage() for r in caplog.records if "at a limit" in r.getMessage()]
    assert len(said) == 2
    assert "MEMORY_BUDGET" in said[0] and "TOO_MANY_ACTIONS" in said[1]


async def _until_true(cond: Callable[[], bool], timeout: float) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not cond():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.01)
