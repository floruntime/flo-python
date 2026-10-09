"""StreamWorker acks are not held up by the long poll. Runs against an
in-process fake server."""

import asyncio
import struct

from flo import FloClient
from flo.types import HEADER_SIZE, MAGIC, VERSION, OpCode, StatusCode
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


def _one_record() -> bytes:
    payload = b"hello"
    return (
        struct.pack("<I", 1)  # count
        + struct.pack("<Qq", 1, 1000)  # sequence, timestamp_ms
        + bytes([0])  # tier
        + struct.pack("<I", 0)  # partition
        + bytes([0])  # no key
        + struct.pack("<I", len(payload))
        + payload
        + struct.pack("<I", 0)  # no headers
    )


class FakeStreamServer:
    """Hands out one record, then holds every group read for `hold` seconds."""

    def __init__(self, hold: float) -> None:
        self.hold = hold
        self.delivered = False
        self.acked = asyncio.Event()
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
                if op_code == OpCode.STREAM_GROUP_READ:
                    if not self.delivered:
                        self.delivered = True
                        writer.write(_response(request_id, _one_record()))
                    else:
                        loop.call_later(self.hold, writer.write, _response(request_id, b""))
                    continue
                if op_code == OpCode.STREAM_GROUP_ACK:
                    self.acked.set()
                writer.write(_response(request_id, b""))
        except (asyncio.IncompleteReadError, ConnectionError):
            pass


async def test_ack_is_not_queued_behind_the_long_poll() -> None:
    fake = FakeStreamServer(hold=5.0)
    server = await asyncio.start_server(fake.handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]

    async def handler(ctx: StreamContext) -> None:
        # Let the poll loop re-issue its blocking read first.
        await asyncio.sleep(0.1)

    client = FloClient(f"127.0.0.1:{port}")
    worker = client.new_stream_worker(stream="s", group="g", handler=handler, block_ms=5000)
    run = asyncio.create_task(worker.start())
    try:
        await asyncio.wait_for(fake.acked.wait(), timeout=1.0)
    finally:
        worker.stop()
        await asyncio.wait_for(run, timeout=10)
        server.close()
        # Python 3.12+ wait_closed() also waits for open connections, and the
        # client may still hold one.
        for writer in fake.writers:
            writer.close()
        await server.wait_closed()


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
