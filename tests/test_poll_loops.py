"""A worker's poll loop gives cancellation back to its caller and keeps its
concurrency cap; the client refuses an answer longer than a server sends.
Runs against an in-process fake server."""

import asyncio
import struct
from typing import Any

import pytest

from flo import FloClient, RetryPolicy
from flo.exceptions import ProtocolError
from flo.types import MAGIC, MAX_ANSWER_BYTES, TABLE_HASH, VERSION, OpCode, StatusCode
from flo.wire import RESPONSE_HEADER_FORMAT
from flo.worker import ActionContext

from .test_result_resend import ACTION, _until
from .test_retry import _frame, _Scripted
from .test_stream_worker_ack import _one_record

_FAST = RetryPolicy(max_attempts=1)


def _ok_or_park(op: int, rid: int) -> bytes:
    if op in (OpCode.ACTION_AWAIT, OpCode.STREAM_GROUP_READ):
        return b""  # park, as a long poll holds its connection
    if op == OpCode.WORKER_HEARTBEAT:
        return _frame(rid, StatusCode.OK, b"\x00")
    return _frame(rid, StatusCode.OK, b"")


@pytest.mark.parametrize("kind", ["action", "stream"])
async def test_cancelling_a_running_worker_raises_cancelled_error(kind: str) -> None:
    polls = (OpCode.ACTION_AWAIT, OpCode.STREAM_GROUP_READ)
    fake = _Scripted(lambda _n, op, rid: _ok_or_park(op, rid))
    server, addr = await fake.serve()
    parent = FloClient(addr, timeout_ms=1000, retry=_FAST)
    worker: Any
    if kind == "action":
        worker = parent.new_action_worker(block_ms=30000)

        async def done(_ctx: ActionContext) -> bytes:
            return b"done"

        worker.register_action(ACTION, done)
    else:

        async def handler(_ctx: object) -> None:
            return None

        worker = parent.new_stream_worker(stream="s", group="g", handler=handler, block_ms=30000)
    run = asyncio.create_task(worker.start())
    try:
        await _until(lambda: any(op in polls for op in fake.ops), timeout=10)
        run.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(run, timeout=5)
        assert run.cancelled()
    finally:
        await fake.shut(server)


async def test_a_failed_dispatch_keeps_the_stream_workers_concurrency_cap() -> None:
    fake = _Scripted(
        lambda _n, op, rid: (
            _frame(rid, StatusCode.OK, _one_record())
            if op == OpCode.STREAM_GROUP_READ
            else _ok_or_park(op, rid)
        )
    )
    server, addr = await fake.serve()

    async def handler(_ctx: object) -> None:
        return None

    worker = FloClient(addr, timeout_ms=1000, retry=_FAST).new_stream_worker(
        stream="s", group="g", handler=handler, block_ms=100, concurrency=1
    )
    free_slots: list[int] = []
    dispatches = 0

    async def dispatch(_records: object) -> None:
        nonlocal dispatches
        dispatches += 1
        assert worker._semaphore is not None
        free_slots.append(worker._semaphore._value)
        if dispatches == 1:
            raise RuntimeError("dispatch failed")
        worker.stop()

    worker._dispatch = dispatch  # type: ignore[method-assign]
    try:
        await asyncio.wait_for(worker.start(), timeout=10)
        # The poll loop gave its slot back before each dispatch, so all of the
        # cap (one) is free at each, and a failed dispatch adds none.
        assert free_slots == [1, 1]
    finally:
        await fake.shut(server)


async def test_an_answer_longer_than_a_server_sends_is_refused_before_reading_it() -> None:
    too_long = MAX_ANSWER_BYTES - 32 + 1

    def reply(_n: int, _op: int, rid: int) -> bytes:
        # A header promising more than any answer holds, and no body.
        return struct.pack(
            RESPONSE_HEADER_FORMAT,
            MAGIC,
            too_long,
            rid,
            0,
            VERSION,
            StatusCode.OK,
            0,
            0,
            TABLE_HASH,
        )

    fake = _Scripted(reply)
    server, addr = await fake.serve()
    client = await FloClient(addr, timeout_ms=2000, retry=_FAST).connect()
    try:
        with pytest.raises(ProtocolError, match=str(too_long)):
            await asyncio.wait_for(client.kv.get("k"), timeout=1)
        # The connection is dropped: what follows on it can't be read.
        assert not client.is_connected
    finally:
        await client.close()
        await fake.shut(server)
