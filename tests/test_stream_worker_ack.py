"""StreamWorker acks go out on their own connection, which survives drops
and server restarts. Runs against an in-process fake server."""

import asyncio
import logging
import struct
from typing import Any

import pytest

from flo import FloClient
from flo import worker as worker_mod
from flo.exceptions import UnexpectedEofError, is_connection_error
from flo.types import HEADER_SIZE, MAGIC, VERSION, OpCode, StatusCode, StreamID
from flo.wire import REQUEST_HEADER_FORMAT, RESPONSE_HEADER_FORMAT, compute_crc32
from flo.worker import StreamContext


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


def _records(n: int) -> bytes:
    payload = b"hello"
    out = struct.pack("<I", n)  # count
    for seq in range(1, n + 1):
        out += (
            struct.pack("<Qq", seq, 1000)  # sequence, timestamp_ms
            + bytes([0])  # tier
            + struct.pack("<I", 0)  # partition
            + bytes([0])  # no key
            + struct.pack("<I", len(payload))
            + payload
            + struct.pack("<I", 0)  # no headers
        )
    return out


def _one_record() -> bytes:
    return _records(1)


class FakeStreamServer:
    """Hands out `count` records, then holds every group read for `hold`
    seconds."""

    def __init__(self, hold: float, count: int = 1, ack_delay: float = 0.0) -> None:
        self.hold = hold
        self.count = count
        self.ack_delay = ack_delay
        self.delivered = False
        self.connections = 0
        self.closed_by_client = 0
        self.acks = 0
        self.acked = asyncio.Event()
        self.nacked = asyncio.Event()
        self.writers: set[asyncio.StreamWriter] = set()

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.connections += 1
        self.writers.add(writer)
        loop = asyncio.get_running_loop()
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
                        writer.write(_response(request_id, _records(self.count)))
                    else:
                        loop.call_later(self.hold, self._answer_late, writer, request_id)
                    continue
                if op_code == OpCode.STREAM_GROUP_ACK:
                    self.acks += 1
                    self.acked.set()
                    if self.ack_delay:
                        loop.call_later(self.ack_delay, self._answer_late, writer, request_id)
                        continue
                if op_code == OpCode.STREAM_GROUP_NACK:
                    self.nacked.set()
                writer.write(_response(request_id, b""))
        except (asyncio.IncompleteReadError, ConnectionError):
            self.closed_by_client += 1

    @staticmethod
    def _answer_late(writer: asyncio.StreamWriter, request_id: int) -> None:
        if not writer.is_closing():
            writer.write(_response(request_id, b""))

    def drop_all(self) -> None:
        for writer in self.writers:
            writer.close()
        self.writers.clear()


async def _serve(fake: Any) -> tuple[asyncio.Server, int]:
    server = await asyncio.start_server(fake.handle, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()[1]


async def _shut(server: asyncio.Server, fake: Any) -> None:
    server.close()
    # Python 3.12+ wait_closed() also waits for open connections, and the
    # client may still hold one.
    for writer in fake.writers:
        writer.close()
    await server.wait_closed()


@pytest.mark.parametrize("outcome", ["ack", "nack"])
async def test_ack_is_not_queued_behind_the_long_poll(outcome: str) -> None:
    fake = FakeStreamServer(hold=5.0)
    server, port = await _serve(fake)

    async def handler(ctx: StreamContext) -> None:
        # Let the poll loop re-issue its blocking read first.
        await asyncio.sleep(0.1)
        if outcome == "nack":
            raise RuntimeError("handler failed")

    client = FloClient(f"127.0.0.1:{port}")
    worker = client.new_stream_worker(stream="s", group="g", handler=handler, block_ms=5000)
    run = asyncio.create_task(worker.start())
    try:
        done = fake.acked if outcome == "ack" else fake.nacked
        await asyncio.wait_for(done.wait(), timeout=1.0)
    finally:
        worker.stop()
        await asyncio.wait_for(run, timeout=10)
        await _shut(server, fake)


async def test_stop_lets_an_in_flight_ack_finish(caplog: pytest.LogCaptureFixture) -> None:
    # The server answers the ack only after stop() has been called.
    fake = FakeStreamServer(hold=5.0, ack_delay=0.2)
    server, port = await _serve(fake)

    client = FloClient(f"127.0.0.1:{port}")
    worker = client.new_stream_worker(stream="s", group="g", handler=_noop, block_ms=5000)
    run = asyncio.create_task(worker.start())
    try:
        with caplog.at_level(logging.ERROR, logger="flo"):
            await asyncio.wait_for(fake.acked.wait(), timeout=1.0)
            worker.stop()
            await asyncio.wait_for(run, timeout=10)
        assert not caplog.records
    finally:
        await _shut(server, fake)


async def test_stop_does_not_reconnect_under_draining_handlers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # stop() interrupts the poll read. Reconnecting then would also reopen
    # the ack connection while the handlers below are about to ack.
    open_connection = asyncio.open_connection

    async def slow_open_connection(*args: Any, **kw: Any) -> Any:
        await asyncio.sleep(0.05)
        return await open_connection(*args, **kw)

    monkeypatch.setattr(asyncio, "open_connection", slow_open_connection)
    n = 3
    fake = FakeStreamServer(hold=5.0, count=n)
    server, port = await _serve(fake)
    started = 0
    all_started = asyncio.Event()
    release = asyncio.Event()

    async def handler(ctx: StreamContext) -> None:
        nonlocal started
        started += 1
        if started == n:
            all_started.set()
        await release.wait()

    client = FloClient(f"127.0.0.1:{port}")
    worker = client.new_stream_worker(stream="s", group="g", handler=handler, block_ms=5000)
    run = asyncio.create_task(worker.start())
    try:
        await asyncio.wait_for(all_started.wait(), timeout=2.0)
        worker.stop()
        await asyncio.sleep(0.075)
        release.set()
        await asyncio.wait_for(run, timeout=10)
        assert fake.acks == n
        assert fake.connections == 2  # no reconnect after stop
        # start() closed both connections on its way out.
        await asyncio.sleep(0.05)
        assert fake.closed_by_client == 2
    finally:
        release.set()
        await _shut(server, fake)


async def test_acks_land_after_a_server_restart() -> None:
    n = 10
    fake = FakeStreamServer(hold=1.0, count=n)
    server, port = await _serve(fake)
    started = 0
    all_started = asyncio.Event()
    release = asyncio.Event()

    async def handler(ctx: StreamContext) -> None:
        nonlocal started
        started += 1
        if started == n:
            all_started.set()
        await release.wait()

    client = FloClient(f"127.0.0.1:{port}")
    worker = client.new_stream_worker(
        stream="s",
        group="g",
        handler=handler,
        block_ms=1000,
        concurrency=n + 1,  # leaves the poll loop a slot to notice the drop
        redeliver_pending_on_reconnect=False,
    )
    run = asyncio.create_task(worker.start())
    try:
        await asyncio.wait_for(all_started.wait(), timeout=2.0)
        fake.drop_all()
        # The poll loop reconnects both connections before any ack is sent.
        for _ in range(100):
            if fake.connections >= 4:
                break
            await asyncio.sleep(0.02)
        assert fake.connections == 4
        release.set()
        for _ in range(100):
            if fake.acks == n:
                break
            await asyncio.sleep(0.02)
        assert fake.acks == n
    finally:
        release.set()
        worker.stop()
        await asyncio.wait_for(run, timeout=10)
        await _shut(server, fake)


async def test_concurrent_ack_failures_reconnect_once() -> None:
    n = 10
    fake = FakeStreamServer(hold=5.0, count=n)
    server, port = await _serve(fake)
    started = 0
    all_started = asyncio.Event()
    release = asyncio.Event()

    async def handler(ctx: StreamContext) -> None:
        nonlocal started
        started += 1
        if started == n:
            all_started.set()
        await release.wait()

    client = FloClient(f"127.0.0.1:{port}")
    worker = client.new_stream_worker(
        stream="s", group="g", handler=handler, block_ms=5000, concurrency=n
    )
    run = asyncio.create_task(worker.start())
    try:
        await asyncio.wait_for(all_started.wait(), timeout=2.0)
        assert worker._ack_client is not None and worker._ack_client._writer is not None
        worker._ack_client._writer.close()
        release.set()
        for _ in range(100):
            if fake.acks == n:
                break
            await asyncio.sleep(0.02)
        assert fake.acks == n
        assert fake.connections == 3  # poll, ack, one reconnected ack
    finally:
        release.set()
        worker.stop()
        await asyncio.wait_for(run, timeout=10)
        await _shut(server, fake)


async def test_concurrent_reconnects_leave_one_connection_open() -> None:
    accepted = 0
    closed = 0

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal accepted, closed
        accepted += 1
        await reader.read()
        closed += 1
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    client = await FloClient(f"127.0.0.1:{port}").connect()
    await asyncio.gather(*[client.reconnect() for _ in range(10)])
    await asyncio.sleep(0.2)
    assert accepted - closed == 1
    await client.close()
    server.close()
    await server.wait_closed()


async def test_ack_that_keeps_failing_raises(caplog: pytest.LogCaptureFixture) -> None:
    class DeadClient:
        async def reconnect(self) -> None:
            pass

    async def send(_: Any) -> None:
        raise UnexpectedEofError("gone")

    worker = FloClient("127.0.0.1:1").new_stream_worker(stream="s", group="g", handler=_noop)
    worker._ack_client = DeadClient()  # type: ignore[assignment]
    worker._running = True
    record_id = StreamID(timestamp_ms=1000, sequence=7)
    with caplog.at_level(logging.WARNING, logger="flo"), pytest.raises(RuntimeError):
        await worker._on_ack_connection("ack", record_id, send)
    assert any(str(record_id) in r.getMessage() for r in caplog.records)


async def _noop(ctx: StreamContext) -> None:
    pass


async def test_start_closes_the_poll_client_when_the_ack_client_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clients: list[FloClient] = []
    closed: list[FloClient] = []

    class FlakyClient(FloClient):
        def __init__(self, *args: Any, **kw: Any) -> None:
            super().__init__(*args, **kw)
            clients.append(self)

        async def connect(self) -> "FloClient":
            if len(clients) > 1:
                raise ConnectionRefusedError
            return self

        async def close(self) -> None:
            closed.append(self)

    monkeypatch.setattr(worker_mod, "FloClient", FlakyClient)
    worker = FloClient("127.0.0.1:1").new_stream_worker(stream="s", group="g", handler=_noop)
    with pytest.raises(ConnectionRefusedError):
        await worker.start()
    assert closed == clients[:1]


@pytest.mark.parametrize("exc", [BrokenPipeError(), ConnectionResetError(), OSError()])
def test_os_errors_are_connection_errors(exc: Exception) -> None:
    assert is_connection_error(exc)


class NackDropServer:
    """Hands out a record every 50 ms and closes the connection that sent
    the first nack."""

    def __init__(self) -> None:
        self.nacks = 0
        self.dropped = False
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
                    await asyncio.sleep(0.05)
                    writer.write(_response(request_id, _one_record()))
                    continue
                if op_code == OpCode.STREAM_GROUP_NACK:
                    self.nacks += 1
                    if not self.dropped:
                        self.dropped = True
                        writer.write(_response(request_id, b""))
                        await writer.drain()
                        writer.close()
                        return
                writer.write(_response(request_id, b""))
        except (asyncio.IncompleteReadError, ConnectionError):
            pass


async def test_nacks_reconnect_a_dropped_ack_connection() -> None:
    fake = NackDropServer()
    server = await asyncio.start_server(fake.handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]

    async def failing(ctx: StreamContext) -> None:
        raise RuntimeError("handler failed")

    client = FloClient(f"127.0.0.1:{port}")
    worker = client.new_stream_worker(stream="s", group="g", handler=failing, block_ms=1000)
    run = asyncio.create_task(worker.start())
    try:
        await asyncio.sleep(0.6)
    finally:
        worker.stop()
        await asyncio.wait_for(run, timeout=10)
        server.close()
        for writer in fake.writers:
            writer.close()
        await server.wait_closed()
    # About 10 records failed; every nack after the dropped one must arrive.
    assert fake.nacks >= 5
