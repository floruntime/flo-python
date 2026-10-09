"""Workers back off when blocking polls come back empty at once, as the server
answers them when it has no room to park the wait. Runs against an in-process
fake server."""

import asyncio
import struct
from collections.abc import Callable

from flo import ActionContext, FloClient
from flo.types import HEADER_SIZE, MAGIC, VERSION, OpCode, StatusCode
from flo.wire import REQUEST_HEADER_FORMAT, RESPONSE_HEADER_FORMAT, compute_crc32
from flo.worker import ActionWorker, StreamContext, StreamWorker

POLL_OPS = (OpCode.ACTION_AWAIT, OpCode.STREAM_GROUP_READ)


def _ok_empty(request_id: int) -> bytes:
    def header(crc: int) -> bytes:
        return struct.pack(
            RESPONSE_HEADER_FORMAT,
            MAGIC,
            0,
            request_id,
            crc,
            VERSION,
            StatusCode.OK,
            0,
            0,
            b"\x00" * 8,
        )

    return header(compute_crc32(header(0), b""))


class PoolFullServer:
    """Answers every request OK and empty, at once."""

    def __init__(self) -> None:
        self.polls = 0

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                header = await reader.readexactly(HEADER_SIZE)
                _, payload_len, request_id, _, op_code, *_ = struct.unpack(
                    REQUEST_HEADER_FORMAT, header
                )
                await reader.readexactly(payload_len)
                if op_code in POLL_OPS:
                    self.polls += 1
                writer.write(_ok_empty(request_id))
        except (asyncio.IncompleteReadError, ConnectionError):
            pass


async def _polls_in(
    seconds: float, make_worker: Callable[[FloClient], ActionWorker | StreamWorker]
) -> int:
    fake = PoolFullServer()
    server = await asyncio.start_server(fake.handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    worker = make_worker(FloClient(f"127.0.0.1:{port}"))
    run = asyncio.create_task(worker.start())
    try:
        await asyncio.sleep(seconds)
    finally:
        worker.stop()
        await asyncio.wait_for(run, timeout=5)
        server.close()
        await server.wait_closed()
    return fake.polls


async def _noop_action(ctx: ActionContext) -> bytes:
    return b""


async def _noop_record(ctx: StreamContext) -> None:
    return None


def _action_worker(client: FloClient) -> ActionWorker:
    worker = client.new_action_worker(block_ms=5000)
    worker.register_action("a", _noop_action)
    return worker


def _stream_worker(client: FloClient) -> StreamWorker:
    return client.new_stream_worker(stream="s", group="g", handler=_noop_record, block_ms=5000)


# Backed off (50, 100, 200 ms, ...) a worker polls about half a dozen times in
# 0.6 s; without it, hundreds.


async def test_action_worker_backs_off_on_immediate_empty_awaits() -> None:
    assert await _polls_in(0.6, _action_worker) < 20


async def test_stream_worker_backs_off_on_immediate_empty_reads() -> None:
    assert await _polls_in(0.6, _stream_worker) < 20


async def test_one_early_empty_is_not_delayed() -> None:
    from flo.worker import _EmptyPollBackoff

    loop = asyncio.get_running_loop()
    backoff = _EmptyPollBackoff(block_ms=1000)

    started = loop.time()
    await backoff.empty(0.0)  # a wake-up: retried at once
    assert loop.time() - started < 0.03

    await backoff.empty(0.6)  # a poll that waited: not early, resets
    started = loop.time()
    await backoff.empty(0.0)
    assert loop.time() - started < 0.03

    started = loop.time()
    await backoff.empty(0.0)  # second early empty in a row: paused
    assert loop.time() - started >= 0.04
