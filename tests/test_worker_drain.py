"""Drain and stop, for both workers: a drain lets running handlers finish,
heartbeating, and keeps the server told; a stop or drain ends a reconnect at
once and holds even when it lands before start(); a stream worker registers,
heartbeats and hands back the records it hasn't started. Runs against an
in-process fake server that keeps the worker registry as flo does."""

import asyncio
import contextlib
import struct
from collections.abc import Callable
from typing import Any

import pytest

from flo import FloClient, RetryPolicy
from flo import worker as worker_mod
from flo.types import HEADER_SIZE, OpCode, Ran, Reason, StatusCode, WorkerStatus
from flo.wire import REQUEST_HEADER_FORMAT
from flo.worker import ActionContext, ActionWorker, StreamContext, StreamWorker

from .test_result_resend import ACTION, _task, _until
from .test_retry import _frame
from .test_stream_worker_ack import _records

# How the fake answers the nth poll (await or group read): bytes to send,
# None for an empty answer after a short wait, or "park" to never answer.
Poll = Callable[[int], "bytes | None | str"]


def _value(payload: bytes) -> bytes:
    """A request's value: [ns_len:u16][ns][key_len:u16][key][value_len:u32][value]."""
    ns_len = struct.unpack_from("<H", payload)[0]
    at = 2 + ns_len
    key_len = struct.unpack_from("<H", payload, at)[0]
    at += 2 + key_len
    value_len = struct.unpack_from("<I", payload, at)[0]
    return payload[at + 4 : at + 4 + value_len]


class Registry:
    """A fake server that keeps one worker's registration as flo does:
    registering makes it active, a drain notice makes it draining, a heartbeat
    answers its status (not found once it is gone), deregistering removes it.
    `hang_up(op, n)` closes the connection instead of answering the nth
    request with op; `refuse()` stops listening, so reconnects fail."""

    def __init__(
        self,
        poll: Poll,
        hang_up: Callable[[int, int], bool] = lambda op, n: False,
        delay: dict[int, float] | None = None,
        claim: bytes = b"",
    ):
        self.poll = poll
        self.hang_up = hang_up
        # The first group claim's records; later claims find none.
        self.claim = claim
        # Seconds each op's answer is held back.
        self.delay = delay or {}
        self.status: WorkerStatus | None = None
        self.requests: list[tuple[int, bytes]] = []
        self.writers: list[asyncio.StreamWriter] = []
        self.server: asyncio.Server | None = None

    def of(self, op: int) -> list[bytes]:
        return [p for o, p in self.requests if o == op]

    def ops(self) -> list[int]:
        return [o for o, _ in self.requests]

    async def listen(self) -> str:
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", 0)
        return f"127.0.0.1:{self.server.sockets[0].getsockname()[1]}"

    def refuse(self) -> None:
        assert self.server is not None
        self.server.close()
        for w in self.writers:
            w.close()

    async def close(self) -> None:
        self.refuse()
        assert self.server is not None
        await self.server.wait_closed()

    @staticmethod
    def _not_found(request_id: int) -> bytes:
        body = (
            int(Reason.NOT_FOUND).to_bytes(2, "little") + bytes([int(Ran.NO)]) + b"worker not found"
        )
        return _frame(request_id, StatusCode.NOT_FOUND, body)

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.writers.append(writer)
        try:
            while True:
                header = await reader.readexactly(HEADER_SIZE)
                _, payload_len, request_id, _, op, *_ = struct.unpack(REQUEST_HEADER_FORMAT, header)
                payload = await reader.readexactly(payload_len)
                n = len(self.of(op))
                self.requests.append((op, payload))
                if self.hang_up(op, n):
                    writer.close()
                    return
                if op in self.delay:
                    await asyncio.sleep(self.delay[op])
                if op == OpCode.WORKER_REGISTER:
                    self.status = WorkerStatus.ACTIVE
                elif op == OpCode.WORKER_DEREGISTER:
                    self.status = None
                elif op in (OpCode.WORKER_DRAIN, OpCode.WORKER_HEARTBEAT):
                    if self.status is None:
                        writer.write(self._not_found(request_id))
                        continue
                    if op == OpCode.WORKER_DRAIN:
                        self.status = WorkerStatus.DRAINING
                    else:
                        writer.write(_frame(request_id, StatusCode.OK, bytes([self.status])))
                        continue
                elif op == OpCode.STREAM_GROUP_CLAIM:
                    # Records, then the cursor: all ones once the scan is done.
                    body = (self.claim if n == 0 else _records(0)) + b"\xff" * 16
                    writer.write(_frame(request_id, StatusCode.OK, body))
                    continue
                elif op in (OpCode.ACTION_AWAIT, OpCode.STREAM_GROUP_READ):
                    answer = self.poll(n)
                    if answer == "park":
                        continue
                    if answer is None:
                        await asyncio.sleep(0.02)
                        answer = b"" if op == OpCode.ACTION_AWAIT else _records(0)
                    assert isinstance(answer, bytes)
                    writer.write(_frame(request_id, StatusCode.OK, answer))
                    continue
                writer.write(_frame(request_id, StatusCode.OK, b""))
        except (asyncio.IncompleteReadError, ConnectionError):
            pass


def _task_then_park(n: int) -> "bytes | None | str":
    return _task() if n == 0 else "park"


def _action_worker(
    endpoint: str, handler: Callable[[ActionContext], Any] | None = None
) -> ActionWorker:
    client = FloClient(endpoint, retry=RetryPolicy(max_attempts=1))
    worker = client.new_action_worker(block_ms=30000, concurrency=2)

    async def run(ctx: ActionContext) -> bytes:
        if handler is not None:
            await handler(ctx)
        return b"done"

    worker.register_action(ACTION, run)
    return worker


def _stream_worker(
    endpoint: str, handler: Callable[[StreamContext], Any] | None = None, concurrency: int = 2
) -> StreamWorker:
    client = FloClient(endpoint, retry=RetryPolicy(max_attempts=1))

    async def run(ctx: StreamContext) -> None:
        if handler is not None:
            await handler(ctx)

    return client.new_stream_worker(
        stream="s",
        group="g",
        handler=run,
        block_ms=30000,
        concurrency=concurrency,
        redeliver_pending_on_reconnect=False,
    )


def _held(seconds: float, started: asyncio.Event, finished: asyncio.Event) -> Callable[[Any], Any]:
    async def handler(ctx: Any) -> None:
        started.set()
        await asyncio.sleep(seconds)
        finished.set()

    return handler


@pytest.fixture(autouse=True)
def fast_intervals(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_mod, "_HEARTBEAT_INTERVAL_S", 0.05)
    monkeypatch.setattr(worker_mod, "_DRAIN_NOTICE_INTERVAL_S", 0.1)


async def test_drain_lets_the_running_handler_finish_heartbeating_and_tells_the_server() -> None:
    reg = Registry(_task_then_park)
    started, finished = asyncio.Event(), asyncio.Event()
    worker = _action_worker(await reg.listen(), _held(0.4, started, finished))
    run = asyncio.create_task(worker.start())
    try:
        await asyncio.wait_for(started.wait(), 5)
        worker.drain()
        # start() returns by itself once the handler has reported.
        await asyncio.wait_for(run, 5)
    finally:
        await reg.close()
    assert finished.is_set()
    ops = reg.ops()
    drain, complete = ops.index(OpCode.WORKER_DRAIN), ops.index(OpCode.ACTION_COMPLETE)
    assert drain < complete
    assert ops[drain:].count(OpCode.ACTION_AWAIT) == 0
    # Heartbeats go on while the handler finishes.
    assert ops[drain:complete].count(OpCode.WORKER_HEARTBEAT) >= 3
    assert ops[-1] == OpCode.WORKER_DEREGISTER
    assert len(reg.of(OpCode.WORKER_DRAIN)) == 1


@pytest.mark.parametrize("make", [_action_worker, _stream_worker])
async def test_a_stop_keeps_heartbeating_while_running_handlers_finish(make: Any) -> None:
    first: Poll = (
        (lambda n: _task() if n == 0 else "park")
        if make is _action_worker
        else (lambda n: _records(1) if n == 0 else "park")
    )
    reg = Registry(first)
    started, finished = asyncio.Event(), asyncio.Event()
    worker = make(await reg.listen(), _held(0.4, started, finished))
    run = asyncio.create_task(worker.start())
    try:
        await asyncio.wait_for(started.wait(), 5)
        worker.stop()
        at_stop = len(reg.requests)
        await asyncio.wait_for(run, 5)
    finally:
        await reg.close()
    assert finished.is_set()
    after = reg.ops()[at_stop:]
    report = OpCode.ACTION_COMPLETE if make is _action_worker else OpCode.STREAM_GROUP_ACK
    assert after[: after.index(report)].count(OpCode.WORKER_HEARTBEAT) >= 3
    assert OpCode.WORKER_DRAIN not in reg.ops()


async def test_a_draining_heartbeat_drains_the_worker_and_it_tells_the_server_again() -> None:
    reg = Registry(_task_then_park)
    started, finished = asyncio.Event(), asyncio.Event()
    worker = _action_worker(await reg.listen(), _held(0.4, started, finished))
    run = asyncio.create_task(worker.start())
    try:
        await asyncio.wait_for(started.wait(), 5)
        reg.status = WorkerStatus.DRAINING  # an operator drains it
        await asyncio.wait_for(run, 5)
    finally:
        await reg.close()
    assert finished.is_set()
    assert len(reg.of(OpCode.ACTION_COMPLETE)) == 1
    # The notice goes even for a drain the server announced: a reconnect may
    # have registered the worker again since.
    assert len(reg.of(OpCode.WORKER_DRAIN)) == 1
    assert reg.ops()[-1] == OpCode.WORKER_DEREGISTER


async def test_a_drain_notice_is_tried_again_while_its_connection_is_down() -> None:
    reg = Registry(_task_then_park, hang_up=lambda op, n: op == OpCode.WORKER_DRAIN and n < 2)
    started, finished = asyncio.Event(), asyncio.Event()
    worker = _action_worker(await reg.listen(), _held(0.6, started, finished))
    run = asyncio.create_task(worker.start())
    try:
        await asyncio.wait_for(started.wait(), 5)
        worker.drain()
        await asyncio.wait_for(run, 5)
    finally:
        await reg.close()
    assert len(reg.of(OpCode.WORKER_DRAIN)) == 3  # two lost, one landed
    assert len(reg.of(OpCode.ACTION_COMPLETE)) == 1


async def test_an_idle_worker_tries_the_drain_notice_once() -> None:
    reg = Registry(lambda n: None, hang_up=lambda op, n: op == OpCode.WORKER_DRAIN)
    worker = _action_worker(await reg.listen())
    run = asyncio.create_task(worker.start())
    try:
        await _until(lambda: OpCode.ACTION_AWAIT in reg.ops())
        worker.drain()
        await asyncio.wait_for(run, 5)
    finally:
        await reg.close()
    assert len(reg.of(OpCode.WORKER_DRAIN)) == 1


async def test_a_draining_worker_the_server_removed_registers_and_tells_the_server_again() -> None:
    reg = Registry(_task_then_park)
    started, finished = asyncio.Event(), asyncio.Event()
    worker = _action_worker(await reg.listen(), _held(0.5, started, finished))
    run = asyncio.create_task(worker.start())
    try:
        await asyncio.wait_for(started.wait(), 5)
        worker.drain()
        await _until(lambda: reg.status == WorkerStatus.DRAINING)
        reg.status = None  # removed: the next heartbeat is not found
        await asyncio.wait_for(run, 5)
        ops = reg.ops()
    finally:
        await reg.close()
    again = len(ops) - 1 - ops[::-1].index(OpCode.WORKER_REGISTER)
    assert again > ops.index(OpCode.WORKER_DRAIN)
    assert OpCode.WORKER_DRAIN in ops[again:]
    first, second = reg.of(OpCode.WORKER_REGISTER)
    assert _value(first) == _value(second)


@pytest.mark.parametrize("make", [_action_worker, _stream_worker])
async def test_a_drain_before_start_ends_the_run_once_registered_without_polling(make: Any) -> None:
    reg = Registry(_task_then_park)
    worker = make(await reg.listen())
    worker.drain()
    try:
        await asyncio.wait_for(worker.start(), 5)
    finally:
        await reg.close()
    ops = reg.ops()
    assert OpCode.ACTION_AWAIT not in ops and OpCode.STREAM_GROUP_READ not in ops
    assert ops.count(OpCode.WORKER_DRAIN) == 1
    assert ops[-1] == OpCode.WORKER_DEREGISTER


@pytest.mark.parametrize("make", [_action_worker, _stream_worker])
async def test_a_stop_before_start_ends_the_run_at_once(make: Any) -> None:
    reg = Registry(_task_then_park)
    worker = make(await reg.listen())
    worker.stop()
    try:
        await asyncio.wait_for(worker.start(), 5)
    finally:
        await reg.close()
    assert reg.ops() == []


@pytest.mark.parametrize(
    "make, poll_op",
    [(_action_worker, OpCode.ACTION_AWAIT), (_stream_worker, OpCode.STREAM_GROUP_READ)],
)
@pytest.mark.parametrize("end", ["stop", "drain"])
async def test_a_stop_or_drain_ends_a_reconnect_at_once(make: Any, poll_op: int, end: str) -> None:
    reg = Registry(lambda n: "park")

    def drop_and_refuse(op: int, n: int) -> bool:
        # The first poll loses its connection, and every reconnect is
        # refused: the worker waits between tries.
        if op == poll_op and n == 0:
            reg.refuse()
            return True
        return False

    reg.hang_up = drop_and_refuse
    worker = make(await reg.listen())
    run = asyncio.create_task(worker.start())
    try:
        await _until(lambda: poll_op in reg.ops())
        await asyncio.sleep(0.3)
        getattr(worker, end)()
        await asyncio.wait_for(run, 3)
    finally:
        await reg.close()


async def test_a_drained_worker_started_again_polls_again() -> None:
    reg = Registry(lambda n: None)
    worker = _action_worker(await reg.listen())
    try:
        for _ in range(2):
            run = asyncio.create_task(worker.start())
            before = len(reg.of(OpCode.ACTION_AWAIT))
            await _until(lambda before=before: len(reg.of(OpCode.ACTION_AWAIT)) > before)
            worker.drain()
            await asyncio.wait_for(run, 5)
    finally:
        await reg.close()
    assert len(reg.of(OpCode.WORKER_REGISTER)) == 2
    assert len(reg.of(OpCode.WORKER_DRAIN)) == 2


async def test_a_stream_worker_registers_heartbeats_and_deregisters() -> None:
    reg = Registry(lambda n: "park")
    worker = _stream_worker(await reg.listen(), concurrency=3)
    run = asyncio.create_task(worker.start())
    try:
        await _until(lambda: len(reg.of(OpCode.WORKER_HEARTBEAT)) >= 2)
        worker.stop()
        await asyncio.wait_for(run, 5)
    finally:
        await reg.close()
    (register,) = reg.of(OpCode.WORKER_REGISTER)
    value = _value(register)
    # [type:u8][max_concurrency:u32][count:u16]([len:u16][name][kind:u8])*
    assert value[0] == 1  # stream
    assert struct.unpack_from("<IH", value, 1) == (3, 1)
    name_len = struct.unpack_from("<H", value, 7)[0]
    assert value[9 : 9 + name_len] == b"s/g"
    assert value[9 + name_len] == 1  # a stream consumer
    ops = reg.ops()
    assert ops[-2:] == [OpCode.STREAM_GROUP_LEAVE, OpCode.WORKER_DEREGISTER]


async def test_a_stream_drain_finishes_its_records_then_leaves_the_group() -> None:
    reg = Registry(lambda n: _records(1) if n == 0 else "park")
    started, finished = asyncio.Event(), asyncio.Event()
    worker = _stream_worker(await reg.listen(), _held(0.4, started, finished))
    run = asyncio.create_task(worker.start())
    try:
        await asyncio.wait_for(started.wait(), 5)
        worker.drain()
        await asyncio.wait_for(run, 5)
    finally:
        await reg.close()
    assert finished.is_set()
    ops = reg.ops()
    drain, ack = ops.index(OpCode.WORKER_DRAIN), ops.index(OpCode.STREAM_GROUP_ACK)
    assert drain < ack
    assert ops[drain:ack].count(OpCode.WORKER_HEARTBEAT) >= 3
    assert ops[-2:] == [OpCode.STREAM_GROUP_LEAVE, OpCode.WORKER_DEREGISTER]


async def test_a_draining_heartbeat_drains_a_stream_worker() -> None:
    reg = Registry(lambda n: _records(1) if n == 0 else "park")
    started, finished = asyncio.Event(), asyncio.Event()
    worker = _stream_worker(await reg.listen(), _held(0.4, started, finished))
    run = asyncio.create_task(worker.start())
    try:
        await asyncio.wait_for(started.wait(), 5)
        reg.status = WorkerStatus.DRAINING
        await asyncio.wait_for(run, 5)
    finally:
        await reg.close()
    assert finished.is_set()
    assert len(reg.of(OpCode.STREAM_GROUP_ACK)) == 1


async def test_a_stream_stop_hands_back_the_records_it_has_not_started() -> None:
    reg = Registry(lambda n: _records(3) if n == 0 else "park")
    started, finished = asyncio.Event(), asyncio.Event()
    worker = _stream_worker(await reg.listen(), _held(0.3, started, finished), concurrency=1)
    run = asyncio.create_task(worker.start())
    try:
        await asyncio.wait_for(started.wait(), 5)
        worker.stop()
        await asyncio.wait_for(run, 5)
    finally:
        await reg.close()
    assert len(reg.of(OpCode.STREAM_GROUP_ACK)) == 1
    (nack,) = reg.of(OpCode.STREAM_GROUP_NACK)
    # [group_len:u16][group][consumer_len:u16][consumer][count:u32][ts:u64][seq:u64]*
    value = _value(nack)
    group_len = struct.unpack_from("<H", value)[0]
    consumer_len = struct.unpack_from("<H", value, 2 + group_len)[0]
    count = struct.unpack_from("<I", value, 4 + group_len + consumer_len)[0]
    assert count == 2


@pytest.mark.parametrize(
    "make, poll_op",
    [(_action_worker, OpCode.ACTION_AWAIT), (_stream_worker, OpCode.STREAM_GROUP_READ)],
)
async def test_a_drain_with_a_handler_running_and_the_server_gone_ends_soon_after_it(
    make: Any, poll_op: int
) -> None:
    work: Poll = (
        (lambda n: _task() if n == 0 else "park")
        if make is _action_worker
        else (lambda n: _records(1) if n == 0 else "park")
    )
    reg = Registry(work)
    started, finished = asyncio.Event(), asyncio.Event()
    worker = make(await reg.listen(), _held(0.5, started, finished))
    run = asyncio.create_task(worker.start())
    try:
        await asyncio.wait_for(started.wait(), 5)
        reg.refuse()  # the server is gone: every reconnect is refused
        loop = asyncio.get_running_loop()
        t0 = loop.time()
        worker.drain()
        # The handler, then its result's bounded resends.
        await asyncio.wait_for(run, 10)
        assert loop.time() - t0 < 5
    finally:
        await reg.close()
    assert finished.is_set()


@pytest.mark.parametrize("op", [OpCode.ACTION_REGISTER, OpCode.WORKER_REGISTER])
@pytest.mark.parametrize("end", ["stop", "drain"])
async def test_a_stop_or_drain_while_a_startup_registration_is_out_ends_the_run_cleanly(
    op: int, end: str
) -> None:
    reg = Registry(_task_then_park, delay={op: 0.3})
    worker = _action_worker(await reg.listen())
    run = asyncio.create_task(worker.start())
    try:
        await _until(lambda: op in reg.ops())
        getattr(worker, end)()
        await asyncio.wait_for(run, 5)  # raises if start() did
    finally:
        await reg.close()
    assert OpCode.ACTION_AWAIT not in reg.ops()


@pytest.mark.parametrize("make", [_action_worker, _stream_worker])
async def test_a_heartbeat_answered_not_found_after_the_run_ends_registers_nothing(
    make: Any,
) -> None:
    reg = Registry(lambda n: "park", delay={OpCode.WORKER_HEARTBEAT: 0.5})
    worker = make(await reg.listen())
    run = asyncio.create_task(worker.start())
    try:
        await _until(lambda: OpCode.WORKER_HEARTBEAT in reg.ops())
        reg.status = None  # its answer, late, will be not found
        worker.stop()
        await asyncio.wait_for(run, 5)
        await asyncio.sleep(0.7)
    finally:
        await reg.close()
    assert len(reg.of(OpCode.WORKER_REGISTER)) == 1


async def test_a_drain_notice_goes_at_once_though_every_slot_is_busy() -> None:
    reg = Registry(_task_then_park)
    started, finished = asyncio.Event(), asyncio.Event()
    client = FloClient(await reg.listen(), retry=RetryPolicy(max_attempts=1))
    worker = client.new_action_worker(block_ms=30000, concurrency=1)

    async def held(ctx: ActionContext) -> bytes:
        started.set()
        await asyncio.sleep(1.0)
        finished.set()
        return b"done"

    worker.register_action(ACTION, held)
    run = asyncio.create_task(worker.start())
    try:
        await asyncio.wait_for(started.wait(), 5)
        loop = asyncio.get_running_loop()
        t0 = loop.time()
        worker.drain()
        await _until(lambda: OpCode.WORKER_DRAIN in reg.ops())
        assert loop.time() - t0 < 0.5
        assert not finished.is_set()
        await asyncio.wait_for(run, 5)
    finally:
        await reg.close()


@pytest.mark.parametrize("answer", [b"", b"\x00\x00", bytes([9])])
async def test_a_heartbeat_answer_that_isnt_one_known_status_byte_is_refused(answer: bytes) -> None:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        header = await reader.readexactly(HEADER_SIZE)
        _, payload_len, request_id, *_ = struct.unpack(REQUEST_HEADER_FORMAT, header)
        await reader.readexactly(payload_len)
        writer.write(_frame(request_id, StatusCode.OK, answer))

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    client = FloClient(f"127.0.0.1:{server.sockets[0].getsockname()[1]}")
    await client.connect()
    try:
        with pytest.raises(ValueError):
            await client.worker.heartbeat("w")
    finally:
        await client.close()
        server.close()


@pytest.mark.parametrize("op", [OpCode.WORKER_REGISTER, OpCode.STREAM_GROUP_JOIN])
@pytest.mark.parametrize("end", ["stop", "drain"])
async def test_a_stream_stop_or_drain_while_a_startup_request_is_out_ends_the_run_cleanly(
    op: int, end: str
) -> None:
    reg = Registry(lambda n: "park", delay={op: 0.3})
    worker = _stream_worker(await reg.listen())
    run = asyncio.create_task(worker.start())
    try:
        await _until(lambda: op in reg.ops())
        getattr(worker, end)()
        await asyncio.wait_for(run, 5)  # raises if start() did
    finally:
        await reg.close()
    assert OpCode.STREAM_GROUP_READ not in reg.ops()


async def test_a_drain_during_pending_redelivery_handles_every_claimed_record() -> None:
    # The first read loses its connection; the reconnect claims three pending
    # records, handled one at a time.
    reg = Registry(
        lambda n: "park",
        hang_up=lambda op, n: op == OpCode.STREAM_GROUP_READ and n == 0,
        claim=_records(3),
    )
    started = asyncio.Event()
    handled = 0

    async def slow(ctx: StreamContext) -> None:
        nonlocal handled
        started.set()
        await asyncio.sleep(0.2)
        handled += 1

    client = FloClient(await reg.listen(), retry=RetryPolicy(max_attempts=1))
    worker = client.new_stream_worker(
        stream="s",
        group="g",
        handler=slow,
        block_ms=30000,
        concurrency=1,
        redeliver_pending_on_reconnect=True,
    )
    run = asyncio.create_task(worker.start())
    try:
        await asyncio.wait_for(started.wait(), 5)
        worker.drain()
        await asyncio.wait_for(run, 5)
    finally:
        await reg.close()
    assert handled == 3
    assert len(reg.of(OpCode.STREAM_GROUP_ACK)) == 3
    assert reg.of(OpCode.STREAM_GROUP_NACK) == []


async def test_cancel_outlasts_a_task_that_swallows_the_first_cancel() -> None:
    # As asyncio.wait_for does before Python 3.12 when its call finishes as
    # the cancel arrives.
    async def stubborn() -> None:
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.sleep(10)
        await asyncio.sleep(10)

    task = asyncio.ensure_future(stubborn())
    await asyncio.sleep(0)
    await asyncio.wait_for(worker_mod._cancel(task), 2)
    assert task.cancelled()
