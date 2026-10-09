"""Workers pace polls that come back empty early. Runs against an in-process
fake server."""

import asyncio
import struct
from collections.abc import Callable
from dataclasses import dataclass

import pytest

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


def _one_task() -> bytes:
    return (
        struct.pack("<H", 2)
        + b"t1"  # task id
        + struct.pack("<H", 1)
        + b"a"  # task type
        + struct.pack("<qI", 0, 1)  # created_at, attempt
        + bytes([0])  # no caller
    )


class FakeServer:
    """Answers polls from `script`, then at once and empty (a full waiter
    pool); every other request OK and empty at once."""

    def __init__(self, script: list[Answer] | None = None) -> None:
        self.script = list(script or [])
        self.polls = 0
        self.poll_times: list[float] = []  # when each poll arrived
        self.answer_times: list[float] = []  # when each poll was answered
        self.writers: set[asyncio.StreamWriter] = set()

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.writers.add(writer)
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
                body = _one_task() if op_code == OpCode.ACTION_AWAIT else _one_record()
                data = body if record else b""

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
        # Python 3.12+ wait_closed() also waits for open connections, and the
        # client may still hold one.
        for writer in self.fake.writers:
            writer.close()
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


@pytest.fixture
def pauses(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Every pause a worker's backoff decides on, in order."""
    recorded: list[float] = []
    next_pause = _EmptyPollBackoff._next_pause

    def spy(self: _EmptyPollBackoff, elapsed_s: float) -> float:
        pause = next_pause(self, elapsed_s)
        recorded.append(pause)
        return pause

    monkeypatch.setattr(_EmptyPollBackoff, "_next_pause", spy)
    return recorded


async def test_append_wake_is_reread_at_once(pauses: list[float]) -> None:
    # Each round: the parked read is woken empty 20 ms in, and the re-read
    # gets the record. The wake is re-read at once, not after a pause.
    handled = 0

    async def handler(ctx: StreamContext) -> None:
        nonlocal handled
        handled += 1

    def make(client: FloClient) -> StreamWorker:
        return client.new_stream_worker(stream="s", group="g", handler=handler, block_ms=5000)

    rounds = 3
    script: list[Answer] = [(0.02, False), (0.0, True)] * rounds + [(5.0, False)]
    r = await _start(make, script)
    await asyncio.sleep(0.5)
    await r.stop()
    assert handled == rounds
    assert pauses == [0.0] * rounds


async def test_losers_of_a_reread_are_not_paused(pauses: list[float]) -> None:
    # Woken by appends 300 ms apart and losing every re-read: each empty
    # answer came after a real wait, so the next read goes out at once.
    script: list[Answer] = [(0.3, False)] * 4 + [(5.0, False)]
    r = await _start(_stream_worker, script)
    await asyncio.sleep(1.35)
    await r.stop()
    assert pauses == [0.0] * 4


async def test_action_worker_resets_the_pause_after_work(pauses: list[float]) -> None:
    # Four early empties build a streak, a task arrives, then an early empty:
    # it is the first in a new streak, so it is re-polled at once.
    script: list[Answer] = [(0.0, False)] * 4 + [(0.0, True), (0.0, False), (5.0, False)]
    r = await _start(_action_worker, script)
    await asyncio.sleep(0.8)
    await r.stop()
    assert pauses == [0.0, 0.05, 0.1, 0.2, 0.0]


async def test_long_streak_pauses_the_cap() -> None:
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    backoff = _EmptyPollBackoff(block_ms=5000, stop=stop)
    stop.set()  # pauses end at once while the streak builds
    for _ in range(1100):
        await backoff.empty(0.0)
    stop.clear()
    started = loop.time()
    await backoff.empty(0.0)
    assert 0.95 <= loop.time() - started < 1.1


def test_backoff_schedule() -> None:
    backoff = _EmptyPollBackoff(block_ms=5000, stop=asyncio.Event())
    # First early empty at once, then 50 ms doubling to the 1 s cap.
    schedule = [backoff._next_pause(0.0) for _ in range(8)]
    assert schedule == [0.0, 0.05, 0.1, 0.2, 0.4, 0.8, 1.0, 1.0]
    assert backoff._next_pause(0.3) == 0.0  # not early: no pause, resets
    assert backoff._next_pause(0.0) == 0.0
    assert backoff._next_pause(0.0) == 0.05
    backoff.reset()  # work: resets
    assert backoff._next_pause(0.0) == 0.0

    short = _EmptyPollBackoff(block_ms=100, stop=asyncio.Event())
    assert short._next_pause(0.0) == 0.0
    assert short._next_pause(0.06) == 0.0  # over block_ms/2: not early
    assert short._next_pause(0.04) == 0.0  # streak restarted
    assert short._next_pause(0.04) == 0.05
