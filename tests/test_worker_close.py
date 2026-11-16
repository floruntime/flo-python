"""close() waits, bounded, for the run it stops, so in-flight outcomes are
sent; a handler's run is awaited when its task is cancelled; and a cancel
aimed at a worker task while a timed-out handler ends is not swallowed.
Against the in-process fake registry of test_worker_drain."""

import asyncio
import contextlib
import time
from typing import Any

import pytest

from flo import FloClient, RetryPolicy
from flo import worker as worker_mod
from flo.exceptions import NotFoundError
from flo.types import OpCode, OptionTag, Ran, Reason, StatusCode
from flo.wire import OptionsIterator
from flo.worker import StreamContext

from .test_group_ops import _value_and_options
from .test_result_resend import ACTION, _task
from .test_stream_worker_ack import _records
from .test_worker_drain import Registry, _action_worker, _held, _stream_worker


@pytest.fixture(autouse=True)
def fast_intervals(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_mod, "_HEARTBEAT_INTERVAL_S", 0.05)
    monkeypatch.setattr(worker_mod, "_DRAIN_NOTICE_INTERVAL_S", 0.1)


def _abandons(reg: Registry) -> list[bool]:
    """For each group nack sent, whether it hands back (abandon)."""
    out = []
    for payload in reg.of(OpCode.STREAM_GROUP_NACK):
        _, options = _value_and_options(payload)
        out.append(OptionsIterator(options).find(OptionTag.ABANDON) is not None)
    return out


def _process_tasks(worker: Any) -> list[asyncio.Task[Any]]:
    return [t for t in worker._tasks if "_process_record" in t.get_coro().__qualname__]


@pytest.mark.parametrize("make", [_action_worker, _stream_worker])
async def test_close_with_work_in_flight_sends_its_outcome_first(make: Any) -> None:
    first = (
        (lambda n: _task() if n == 0 else "park")
        if make is _action_worker
        else (lambda n: _records(1) if n == 0 else "park")
    )
    reg = Registry(first)
    started, finished = asyncio.Event(), asyncio.Event()
    worker = make(await reg.listen(), _held(0.3, started, finished))
    run = asyncio.create_task(worker.start())
    try:
        await asyncio.wait_for(started.wait(), 5)
        await asyncio.wait_for(worker.close(), 5)
        # start() had returned: close() waited for it.
        assert run.done()
        await run
    finally:
        await reg.close()
    assert finished.is_set()
    report = OpCode.ACTION_COMPLETE if make is _action_worker else OpCode.STREAM_GROUP_ACK
    assert len(reg.of(report)) == 1
    assert reg.ops()[-1] == OpCode.WORKER_DEREGISTER


async def test_close_hands_back_the_records_not_started() -> None:
    reg = Registry(lambda n: _records(3) if n == 0 else "park")
    started, finished = asyncio.Event(), asyncio.Event()
    worker = _stream_worker(await reg.listen(), _held(0.3, started, finished), concurrency=1)
    run = asyncio.create_task(worker.start())
    try:
        await asyncio.wait_for(started.wait(), 5)
        await asyncio.wait_for(worker.close(), 5)
        await run
    finally:
        await reg.close()
    assert len(reg.of(OpCode.STREAM_GROUP_ACK)) == 1
    assert _abandons(reg) == [True]


@pytest.mark.parametrize("make", [_action_worker, _stream_worker])
async def test_close_is_bounded_by_its_timeout(make: Any) -> None:
    first = (
        (lambda n: _task() if n == 0 else "park")
        if make is _action_worker
        else (lambda n: _records(1) if n == 0 else "park")
    )
    reg = Registry(first)
    started, finished = asyncio.Event(), asyncio.Event()
    worker = make(await reg.listen(), _held(30, started, finished))
    run = asyncio.create_task(worker.start())
    try:
        await asyncio.wait_for(started.wait(), 5)
        t0 = time.monotonic()
        await asyncio.wait_for(worker.close(timeout=0.2), 5)
        assert time.monotonic() - t0 < 2
        assert not finished.is_set()
    finally:
        run.cancel()
        with contextlib.suppress(BaseException):
            await run
        await reg.close()


async def test_close_with_nothing_running_returns_at_once() -> None:
    worker = _action_worker("127.0.0.1:1")
    await asyncio.wait_for(worker.close(), 1)


def _slow_to_cancel(
    started: asyncio.Event,
    ended: asyncio.Event,
    cleanup: float,
    cleaning: asyncio.Event | None = None,
) -> Any:
    """A handler that takes `cleanup` seconds to end once cancelled."""

    async def handler(ctx: Any) -> None:
        started.set()
        try:
            await asyncio.sleep(30)
        finally:
            if cleaning is not None:
                cleaning.set()
            await asyncio.sleep(cleanup)
            ended.set()

    return handler


async def test_a_cancelled_record_task_waits_for_its_handler_then_abandons() -> None:
    reg = Registry(lambda n: _records(1) if n == 0 else "park")
    started, ended = asyncio.Event(), asyncio.Event()
    worker = _stream_worker(await reg.listen(), _slow_to_cancel(started, ended, 0.2))
    run = asyncio.create_task(worker.start())
    try:
        await asyncio.wait_for(started.wait(), 5)
        [task] = _process_tasks(worker)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        # The task ended only after its handler did.
        assert ended.is_set()
        worker.stop()
        await asyncio.wait_for(run, 5)
    finally:
        await reg.close()
    assert _abandons(reg) == [True]
    assert reg.of(OpCode.STREAM_GROUP_ACK) == []


async def test_a_timed_out_handler_is_nacked_with_why() -> None:
    reg = Registry(lambda n: _records(1) if n == 0 else "park")
    started, ended = asyncio.Event(), asyncio.Event()
    client = FloClient(await reg.listen(), retry=RetryPolicy(max_attempts=1))

    handler = _slow_to_cancel(started, ended, 0.0)

    async def run_one(ctx: StreamContext) -> None:
        await handler(ctx)

    worker = client.new_stream_worker(
        stream="s", group="g", handler=run_one, block_ms=30000, message_timeout=0.1
    )
    run = asyncio.create_task(worker.start())
    try:
        await asyncio.wait_for(started.wait(), 5)
        await _until_ops(reg, OpCode.STREAM_GROUP_NACK)
        worker.stop()
        await asyncio.wait_for(run, 5)
    finally:
        await reg.close()
    assert ended.is_set()
    assert _abandons(reg) == [False]
    value, _ = _value_and_options(reg.of(OpCode.STREAM_GROUP_NACK)[0])
    assert b"handler timed out after 0.1s" in value


async def test_a_cancel_while_a_timed_out_handler_ends_is_not_swallowed() -> None:
    reg = Registry(lambda n: _records(1) if n == 0 else "park")
    started, ended = asyncio.Event(), asyncio.Event()
    cleaning = asyncio.Event()
    client = FloClient(await reg.listen(), retry=RetryPolicy(max_attempts=1))
    # It ends within the time the worker waits for a timed-out handler.
    handler = _slow_to_cancel(started, ended, 0.3, cleaning)

    async def run_one(ctx: StreamContext) -> None:
        await handler(ctx)

    worker = client.new_stream_worker(
        stream="s", group="g", handler=run_one, block_ms=30000, message_timeout=0.5
    )
    run = asyncio.create_task(worker.start())
    try:
        await asyncio.wait_for(started.wait(), 5)
        [task] = _process_tasks(worker)
        # Timed out, and the handler is taking its time to end.
        await asyncio.wait_for(cleaning.wait(), 5)
        assert not ended.is_set()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert task.cancelled()
        worker.stop()
        await asyncio.wait_for(run, 5)
    finally:
        await reg.close()
    # Handed back: the cancel was the worker's, not a failure.
    assert _abandons(reg) == [True]


async def test_a_cancelled_action_task_waits_for_its_handler() -> None:
    reg = Registry(lambda n: _task() if n == 0 else "park")
    started, ended = asyncio.Event(), asyncio.Event()
    worker = _action_worker(await reg.listen(), _slow_to_cancel(started, ended, 0.2))
    run = asyncio.create_task(worker.start())
    try:
        await asyncio.wait_for(started.wait(), 5)
        [task] = [t for t in worker._tasks if "_execute_task" in t.get_coro().__qualname__]
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert ended.is_set()
        worker.stop()
        await asyncio.wait_for(run, 5)
    finally:
        await reg.close()
    assert len(reg.of(OpCode.ACTION_FAIL)) == 1
    assert reg.of(OpCode.ACTION_COMPLETE) == []


async def test_a_cancel_while_a_timed_out_action_ends_reports_then_raises() -> None:
    reg = Registry(lambda n: _task() if n == 0 else "park")
    started, ended = asyncio.Event(), asyncio.Event()
    cleaning = asyncio.Event()
    client = FloClient(await reg.listen(), retry=RetryPolicy(max_attempts=1))
    worker = client.new_action_worker(block_ms=30000, action_timeout=0.5)
    worker.register_action(ACTION, _slow_to_cancel(started, ended, 0.3, cleaning))
    run = asyncio.create_task(worker.start())
    try:
        await asyncio.wait_for(started.wait(), 5)
        [task] = [t for t in worker._tasks if "_execute_task" in t.get_coro().__qualname__]
        await asyncio.wait_for(cleaning.wait(), 5)
        assert not ended.is_set()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        worker.stop()
        await asyncio.wait_for(run, 5)
    finally:
        await reg.close()
    [fail] = reg.of(OpCode.ACTION_FAIL)
    assert b"Action timed out" in fail


async def test_a_stream_read_not_found_fails_start() -> None:
    # Not retried as if the stream were empty: start() fails with it.
    body = int(Reason.NOT_FOUND).to_bytes(2, "little") + bytes([Ran.NO]) + b"no such stream"
    reg = Registry(lambda n: (StatusCode.NOT_FOUND, body) if n == 0 else "park")
    worker = _stream_worker(await reg.listen())
    try:
        with pytest.raises(NotFoundError, match="no such stream"):
            await asyncio.wait_for(worker.start(), 5)
    finally:
        await reg.close()
    assert reg.ops().count(OpCode.STREAM_GROUP_READ) == 1
    assert reg.ops()[-1] == OpCode.WORKER_DEREGISTER


async def _until_ops(reg: Registry, op: int, timeout: float = 5) -> None:
    deadline = time.monotonic() + timeout
    while op not in reg.ops():
        assert time.monotonic() < deadline, f"no {op!r} within {timeout}s"
        await asyncio.sleep(0.01)
