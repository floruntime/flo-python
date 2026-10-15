"""A worker's result (complete, fail, ack, nack) is decided by the handler
alone, and a lost one is resent rather than turned into another result.
Runs against an in-process fake server."""

import asyncio
import logging
import struct
from collections.abc import Callable
from typing import Any

import pytest

from flo import FloClient
from flo.types import HEADER_SIZE, OpCode, Ran, StatusCode
from flo.wire import (
    REQUEST_HEADER_FORMAT,
    parse_task_assignment,
    serialize_worker_complete_value,
    serialize_worker_fail_value,
)
from flo.worker import ActionContext, StreamContext

from .test_retry import _frame, _refusal
from .test_stream_worker_ack import _one_record, _response

TASK_ID = "task-42"
ACTION = "charge"


def _task() -> bytes:
    tid, ttype = TASK_ID.encode(), ACTION.encode()
    return (
        struct.pack("<H", len(tid))
        + tid
        + struct.pack("<H", len(ttype))
        + ttype
        + struct.pack("<qI", 1000, 1)  # created_at, attempt
        + bytes([0])  # no caller
        + b"{}"
    )


class FakeActionServer:
    """Hands out one task, then answers every await empty. The nth result
    (complete or fail, from 0) gets reply(n): "ok", "drop" (close the
    connection unanswered), "hang" (never answer) or "refuse"."""

    def __init__(self, reply: Callable[[int], str] = lambda n: "ok") -> None:
        self.reply = reply
        self.delivered = False
        self.results: list[tuple[int, bytes]] = []
        self.writers: set[asyncio.StreamWriter] = set()

    def ops(self) -> list[int]:
        return [op for op, _ in self.results]

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.writers.add(writer)
        try:
            while True:
                header = await reader.readexactly(HEADER_SIZE)
                _, payload_len, request_id, _, op_code, *_ = struct.unpack(
                    REQUEST_HEADER_FORMAT, header
                )
                payload = await reader.readexactly(payload_len)
                if op_code == OpCode.ACTION_AWAIT:
                    if not self.delivered:
                        self.delivered = True
                        writer.write(_frame(request_id, StatusCode.OK, _task()))
                    else:
                        await asyncio.sleep(0.05)
                        writer.write(_frame(request_id, StatusCode.OK, b""))
                    continue
                if op_code in (OpCode.ACTION_COMPLETE, OpCode.ACTION_FAIL):
                    action = self.reply(len(self.results))
                    self.results.append((op_code, header + payload))
                    if action == "drop":
                        writer.close()
                        return
                    if action == "hang":
                        continue
                    if action == "refuse":
                        writer.write(_refusal(request_id, StatusCode.BAD_REQUEST, Ran.YES))
                        continue
                writer.write(_frame(request_id, StatusCode.OK, b""))
        except (asyncio.IncompleteReadError, ConnectionError):
            pass


async def _until(cond: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not cond():
        assert asyncio.get_running_loop().time() < deadline, "condition not reached"
        await asyncio.sleep(0.01)


async def _run_action(
    fake: FakeActionServer,
    handler: Callable[[ActionContext], Any],
    done: Callable[[], bool],
    *,
    timeout_ms: int = 5000,
    settle: float = 0.3,
) -> None:
    """Run an action worker until done(), let it settle, then stop it."""
    server = await asyncio.start_server(fake.handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    worker = FloClient(f"127.0.0.1:{port}", timeout_ms=timeout_ms).new_action_worker(block_ms=100)
    worker.register_action(ACTION, handler)
    run = asyncio.create_task(worker.start())
    try:
        await _until(done)
        await asyncio.sleep(settle)
    finally:
        worker.stop()
        await asyncio.wait_for(run, timeout=10)
        server.close()
        for writer in fake.writers:
            writer.close()
        await server.wait_closed()


async def _done(ctx: ActionContext) -> bytes:
    return b"done"


def _masked(frame: bytes) -> bytes:
    # Bytes 8..16 are the request id, 16..20 the CRC that covers it.
    return frame[:8] + bytes(12) + frame[20:]


async def test_a_complete_whose_connection_drops_is_resent_not_failed() -> None:
    fake = FakeActionServer(lambda n: "drop" if n == 0 else "ok")
    await _run_action(fake, _done, lambda: len(fake.results) >= 2)
    assert fake.ops() == [OpCode.ACTION_COMPLETE, OpCode.ACTION_COMPLETE]


async def test_the_resent_result_is_the_same_request() -> None:
    fake = FakeActionServer(lambda n: "drop" if n == 0 else "ok")
    await _run_action(fake, _done, lambda: len(fake.results) >= 2)
    first, second = (frame for _, frame in fake.results)
    assert first[8:16] != second[8:16]
    assert _masked(first) == _masked(second)


async def test_a_complete_that_times_out_is_resent_not_failed() -> None:
    fake = FakeActionServer(lambda n: "hang" if n == 0 else "ok")
    await _run_action(fake, _done, lambda: len(fake.results) >= 2, timeout_ms=300)
    assert fake.ops() == [OpCode.ACTION_COMPLETE, OpCode.ACTION_COMPLETE]


async def test_a_refused_complete_is_neither_resent_nor_failed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake = FakeActionServer(lambda n: "refuse")
    with caplog.at_level(logging.INFO, logger="flo.worker"):
        await _run_action(fake, _done, lambda: len(fake.results) >= 1)
    assert fake.ops() == [OpCode.ACTION_COMPLETE]
    assert not any("Action completed" in r.getMessage() for r in caplog.records)


async def test_a_result_is_sent_at_most_three_times(caplog: pytest.LogCaptureFixture) -> None:
    fake = FakeActionServer(lambda n: "drop")
    with caplog.at_level(logging.ERROR, logger="flo.worker"):
        await _run_action(fake, _done, lambda: len(fake.results) >= 3, settle=0.5)
    assert fake.ops() == [OpCode.ACTION_COMPLETE] * 3
    assert any(TASK_ID in r.getMessage() and "Dropped" in r.getMessage() for r in caplog.records)


async def test_stop_lets_a_running_handler_finish_and_completes_it() -> None:
    fake = FakeActionServer()
    started = asyncio.Event()

    async def slow(ctx: ActionContext) -> bytes:
        started.set()
        await asyncio.sleep(0.3)
        return b"done"

    await _run_action(fake, slow, started.is_set, settle=0.0)
    assert fake.ops() == [OpCode.ACTION_COMPLETE]
    frame = fake.results[0][1]
    assert serialize_worker_complete_value(ACTION, TASK_ID, b"done", "success") in frame


async def _run_with_timeout(fake: FakeActionServer, handler: Any) -> None:
    server = await asyncio.start_server(fake.handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    worker = FloClient(f"127.0.0.1:{port}").new_action_worker(block_ms=100, action_timeout=0.1)
    worker.register_action(ACTION, handler)
    run = asyncio.create_task(worker.start())
    try:
        await _until(lambda: len(fake.results) >= 1)
    finally:
        worker.stop()
        await asyncio.wait_for(run, timeout=10)
        server.close()
        for writer in fake.writers:
            writer.close()
        await server.wait_closed()


async def test_a_handler_timeout_fails_for_good() -> None:
    fake = FakeActionServer()

    async def slow(ctx: ActionContext) -> bytes:
        await asyncio.sleep(10)
        return b""

    await _run_with_timeout(fake, slow)
    assert fake.ops() == [OpCode.ACTION_FAIL]
    frame = fake.results[0][1]
    assert serialize_worker_fail_value(ACTION, TASK_ID, "Action timed out", False) in frame


async def test_a_timeout_error_raised_by_the_handler_is_an_ordinary_failure() -> None:
    fake = FakeActionServer()

    async def raises(ctx: ActionContext) -> bytes:
        raise asyncio.TimeoutError("upstream call timed out")

    await _run_with_timeout(fake, raises)
    assert fake.ops() == [OpCode.ACTION_FAIL]
    frame = fake.results[0][1]
    assert serialize_worker_fail_value(ACTION, TASK_ID, "upstream call timed out", True) in frame


async def test_a_cancelled_task_is_failed_and_the_cancellation_kept() -> None:
    fails: list[tuple[str, bool]] = []

    class Results:
        async def fail(self, wid: str, action: str, tid: str, message: str, opts: Any) -> None:
            fails.append((message, opts.retry))

    class ResultClient:
        worker = Results()

    worker = FloClient("127.0.0.1:1").new_action_worker()
    started = asyncio.Event()

    async def forever(ctx: ActionContext) -> bytes:
        started.set()
        await asyncio.Event().wait()
        return b""

    worker.register_action(ACTION, forever)
    worker._result_client = ResultClient()  # type: ignore[assignment]
    worker._semaphore = asyncio.Semaphore(0)
    task = parse_task_assignment(_task())
    run = asyncio.create_task(worker._execute_task(task))
    await asyncio.wait_for(started.wait(), timeout=1)
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run
    assert fails == [("Action cancelled", True)]
    assert worker._semaphore.locked() is False


async def test_a_poll_reconnect_leaves_a_healthy_result_connection_alone() -> None:
    reconnects = 0

    class Healthy:
        is_connected = True

        async def reconnect(self) -> None:
            nonlocal reconnects
            reconnects += 1

    worker = FloClient("127.0.0.1:1").new_action_worker()
    worker._result_client = Healthy()  # type: ignore[assignment]
    await worker._reconnect_result_client(only_if_down=True)
    assert reconnects == 0
    Healthy.is_connected = False
    await worker._reconnect_result_client(only_if_down=True)
    assert reconnects == 1


async def test_a_result_lost_after_stop_is_resent() -> None:
    fake = FakeActionServer(lambda n: "drop" if n == 0 else "ok")
    started = asyncio.Event()

    async def slow(ctx: ActionContext) -> bytes:
        started.set()
        await asyncio.sleep(0.2)
        return b"done"

    await _run_action(fake, slow, started.is_set, settle=0.0)
    assert fake.ops() == [OpCode.ACTION_COMPLETE, OpCode.ACTION_COMPLETE]


async def test_stop_gives_up_on_a_gone_server_after_a_bounded_wait(
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake = FakeActionServer(lambda n: "drop")
    started = asyncio.Event()

    async def slow(ctx: ActionContext) -> bytes:
        started.set()
        await asyncio.sleep(0.2)
        return b"done"

    server = await asyncio.start_server(fake.handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    worker = FloClient(f"127.0.0.1:{port}").new_action_worker(block_ms=100)
    worker.register_action(ACTION, slow)
    run = asyncio.create_task(worker.start())
    try:
        await _until(started.is_set)
        server.close()  # no new connections: every reconnect is refused
        loop = asyncio.get_running_loop()
        stopped = loop.time()
        with caplog.at_level(logging.ERROR, logger="flo.worker"):
            worker.stop()
            await asyncio.wait_for(run, timeout=10)
        elapsed = loop.time() - stopped
    finally:
        for writer in fake.writers:
            writer.close()
        await server.wait_closed()
    assert fake.ops() == [OpCode.ACTION_COMPLETE]
    assert 1.5 < elapsed < 4.0
    assert any(TASK_ID in r.getMessage() and "Dropped" in r.getMessage() for r in caplog.records)


class AckDropServer:
    """Hands out one record, then answers reads empty; closes the connection
    that sent each of the first `drops` acks without answering it."""

    def __init__(self, drops: int, not_found: bool = False) -> None:
        self.drops = drops
        self.not_found = not_found
        self.delivered = False
        self.acks = 0
        self.nacks = 0
        self.writers: set[asyncio.StreamWriter] = set()

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.writers.add(writer)
        try:
            while True:
                header = await reader.readexactly(HEADER_SIZE)
                _, payload_len, request_id, _, op_code, *_ = struct.unpack(
                    REQUEST_HEADER_FORMAT, header
                )
                await reader.readexactly(payload_len)
                if op_code == OpCode.STREAM_GROUP_READ:
                    if not self.delivered:
                        self.delivered = True
                        writer.write(_response(request_id, _one_record()))
                    else:
                        await asyncio.sleep(0.05)
                        writer.write(_response(request_id, struct.pack("<I", 0)))
                    continue
                if op_code == OpCode.STREAM_GROUP_ACK:
                    self.acks += 1
                    if self.not_found:
                        writer.write(_refusal(request_id, StatusCode.NOT_FOUND, Ran.UNKNOWN))
                        continue
                    if self.acks <= self.drops:
                        writer.close()
                        return
                if op_code == OpCode.STREAM_GROUP_NACK:
                    self.nacks += 1
                writer.write(_response(request_id, b""))
        except (asyncio.IncompleteReadError, ConnectionError):
            pass


@pytest.mark.parametrize(
    ("drops", "not_found", "acks"), [(1, False, 2), (3, False, 3), (0, True, 1)]
)
async def test_a_lost_or_refused_ack_is_never_nacked(
    drops: int, not_found: bool, acks: int
) -> None:
    fake = AckDropServer(drops, not_found)
    server = await asyncio.start_server(fake.handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]

    async def ok(ctx: StreamContext) -> None:
        pass

    worker = FloClient(f"127.0.0.1:{port}").new_stream_worker(
        stream="s", group="g", handler=ok, block_ms=100
    )
    run = asyncio.create_task(worker.start())
    try:
        await _until(lambda: fake.acks >= acks)
        await asyncio.sleep(0.3)
    finally:
        worker.stop()
        await asyncio.wait_for(run, timeout=10)
        server.close()
        for writer in fake.writers:
            writer.close()
        await server.wait_closed()
    assert (fake.acks, fake.nacks) == (acks, 0)


async def test_stop_during_a_poll_neither_reconnects_nor_registers_again() -> None:
    connections = 0
    registers: list[int] = []
    polling = asyncio.Event()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal connections
        connections += 1
        try:
            while True:
                header = await reader.readexactly(HEADER_SIZE)
                _, payload_len, request_id, _, op_code, *_ = struct.unpack(
                    REQUEST_HEADER_FORMAT, header
                )
                await reader.readexactly(payload_len)
                if op_code in (OpCode.ACTION_REGISTER, OpCode.WORKER_REGISTER):
                    registers.append(op_code)
                if op_code == OpCode.ACTION_AWAIT:
                    polling.set()
                    continue  # park: the poll is in flight when stop() comes
                writer.write(_frame(request_id, StatusCode.OK, b""))
        except (asyncio.IncompleteReadError, ConnectionError):
            pass

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    worker = FloClient(f"127.0.0.1:{port}").new_action_worker(block_ms=30000)
    worker.register_action(ACTION, _done)
    run = asyncio.create_task(worker.start())
    await _until(polling.is_set)
    before = (connections, len(registers))
    worker.stop()
    await asyncio.wait_for(run, timeout=10)
    await asyncio.sleep(0.3)
    server.close()
    await server.wait_closed()
    assert (connections, len(registers)) == before
