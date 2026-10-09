"""Workers pace polls that come back empty early. The server answers a blocking
poll empty at once when it has no room to park it, and wakes every parked
group read empty on an append. Runs against an in-process fake server."""

import asyncio
import struct
from collections.abc import Callable
from dataclasses import dataclass

from flo import ActionContext, FloClient
from flo.types import HEADER_SIZE, MAGIC, VERSION, OpCode, StatusCode
from flo.wire import REQUEST_HEADER_FORMAT, RESPONSE_HEADER_FORMAT, compute_crc32
from flo.worker import ActionWorker, StreamContext, StreamWorker, _EmptyPollBackoff

POLL_OPS = (OpCode.ACTION_AWAIT, OpCode.STREAM_GROUP_READ)

# How a scripted server answers one poll: (delay in seconds, with a record?)
Answer = tuple[float, bool]


def _response(request_id: int, data: bytes) -> bytes:
    def header(crc: int) -> bytes:
        return struct.pack(
            RESPONSE_HEADER_FORMAT,
            MAGIC,
            len(data),
            request_id,
            crc,
            VERSION,
            StatusCode.OK,
            0,
            0,
            b"\x00" * 8,
        )

    return header(compute_crc32(header(0), data)) + data


def _one_record() -> bytes:
    return (
        struct.pack("<I", 1)  # count
        + struct.pack("<Qq", 1, 1000)  # sequence, timestamp_ms
        + bytes([0])  # tier
        + struct.pack("<I", 0)  # partition
        + bytes([0])  # no key
        + struct.pack("<I", 0)  # empty payload
        + struct.pack("<I", 0)  # no headers
    )


class FakeServer:
    """Answers polls from `script`, then at once and empty (a full waiter
    pool); every other request OK and empty at once."""

    def __init__(self, script: list[Answer] | None = None) -> None:
        self.script = list(script or [])
        self.polls = 0
        self.poll_times: list[float] = []  # when each poll arrived
        self.answer_times: list[float] = []  # when each poll was answered

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        loop = asyncio.get_running_loop()
        try:
            while True:
                header = await reader.readexactly(HEADER_SIZE)
                _, payload_len, request_id, _, op_code, *_ = struct.unpack(
                    REQUEST_HEADER_FORMAT, header
                )
                await reader.readexactly(payload_len)
                if op_code not in POLL_OPS:
                    writer.write(_response(request_id, b""))
                    continue
                self.polls += 1
                self.poll_times.append(loop.time())
                delay, record = self.script.pop(0) if self.script else (0.0, False)
                data = _one_record() if record else b""

                def answer(rid: int = request_id, body: bytes = data) -> None:
                    self.answer_times.append(loop.time())
                    writer.write(_response(rid, body))

                loop.call_later(delay, answer)
        except (asyncio.IncompleteReadError, ConnectionError):
            pass


@dataclass
class Running:
    fake: FakeServer
    server: asyncio.Server
    worker: ActionWorker | StreamWorker
    run: "asyncio.Task[None]"

    async def stop(self) -> float:
        """Stop the worker; return how long start() took to return."""
        loop = asyncio.get_running_loop()
        started = loop.time()
        self.worker.stop()
        await asyncio.wait_for(self.run, timeout=5)
        took = loop.time() - started
        self.server.close()
        await self.server.wait_closed()
        return took


async def _start(
    make_worker: Callable[[FloClient], ActionWorker | StreamWorker],
    script: list[Answer] | None = None,
) -> Running:
    fake = FakeServer(script)
    server = await asyncio.start_server(fake.handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    worker = make_worker(FloClient(f"127.0.0.1:{port}"))
    run = asyncio.create_task(worker.start())
    return Running(fake, server, worker, run)


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


# Backed off (0, 50, 100, 200 ms, ...) a worker polls about half a dozen
# times in 0.6 s; without it, hundreds.


async def test_action_worker_backs_off_when_the_pool_is_full() -> None:
    r = await _start(_action_worker)
    await asyncio.sleep(0.6)
    await r.stop()
    assert r.fake.polls < 20


async def test_stream_worker_backs_off_when_the_pool_is_full() -> None:
    r = await _start(_stream_worker)
    await asyncio.sleep(0.6)
    await r.stop()
    assert r.fake.polls < 20


async def test_stop_is_not_held_by_a_pause() -> None:
    # By 0.6 s of a full pool the worker is in a pause of 200-400 ms.
    r = await _start(_stream_worker)
    await asyncio.sleep(0.6)
    assert await r.stop() < 0.1


async def test_append_wake_is_reread_at_once() -> None:
    # Each round: the parked read is woken empty 20 ms in, and the re-read
    # gets the record. The handler must run within a round trip or two of
    # each wake, not after a pause (50 ms or more).
    handled: list[float] = []

    async def handler(ctx: StreamContext) -> None:
        handled.append(asyncio.get_running_loop().time())

    def make(client: FloClient) -> StreamWorker:
        return client.new_stream_worker(stream="s", group="g", handler=handler, block_ms=5000)

    rounds = 3
    script: list[Answer] = [(0.02, False), (0.0, True)] * rounds + [(5.0, False)]
    r = await _start(make, script)
    await asyncio.sleep(0.5)
    await r.stop()
    assert len(handled) == rounds
    wakes = r.fake.answer_times[0 : 2 * rounds : 2]
    for wake, done in zip(wakes, handled, strict=True):
        assert done - wake < 0.04


async def test_losers_of_a_reread_are_not_paused() -> None:
    # Woken by appends 300 ms apart and losing every re-read: each empty
    # answer came after a real wait, so the next read goes out at once.
    script: list[Answer] = [(0.3, False)] * 4 + [(5.0, False)]
    r = await _start(_stream_worker, script)
    await asyncio.sleep(1.35)
    await r.stop()
    gaps = [
        nxt - answered
        for answered, nxt in zip(r.fake.answer_times[:4], r.fake.poll_times[1:5], strict=True)
    ]
    assert max(gaps) < 0.03


async def test_backoff_schedule() -> None:
    loop = asyncio.get_running_loop()
    backoff = _EmptyPollBackoff(block_ms=5000, stop=asyncio.Event())

    async def took(elapsed_s: float) -> float:
        started = loop.time()
        await backoff.empty(elapsed_s)
        return loop.time() - started

    assert await took(0.0) < 0.03  # first early empty: at once
    assert 0.04 <= await took(0.0) < 0.09  # then 50 ms
    assert 0.09 <= await took(0.0) < 0.15  # doubling
    assert await took(0.3) < 0.03  # not early: no pause, resets
    assert await took(0.0) < 0.03
    backoff.reset()  # work: resets
    assert await took(0.0) < 0.03

    short = _EmptyPollBackoff(block_ms=100, stop=asyncio.Event())
    await short.empty(0.0)
    started = loop.time()
    await short.empty(0.06)  # over block_ms/2: not early
    assert loop.time() - started < 0.03
