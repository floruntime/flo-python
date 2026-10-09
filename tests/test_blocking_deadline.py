"""Blocking calls wait out block_ms, and a timed-out call's reply is never
handed to the next call. Runs against an in-process fake server."""

import asyncio
import struct
from collections.abc import AsyncIterator
from typing import Any

import pytest

from flo import FloClient, GetOptions
from flo import worker as worker_mod
from flo.exceptions import NotConnectedError
from flo.types import HEADER_SIZE, MAGIC, VERSION, StatusCode
from flo.wire import REQUEST_HEADER_FORMAT, RESPONSE_HEADER_FORMAT, compute_crc32
from flo.worker import ActionWorker, StreamWorker


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


class FakeServer:
    """Answers each KV get with its key as the value after `delays[i]` seconds."""

    def __init__(self, delays: list[float]) -> None:
        self.delays = delays
        self.served = 0
        self.port = 0
        self._server: asyncio.Server | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        assert self._server is not None
        self._server.close()
        await self._server.wait_closed()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                header = await reader.readexactly(HEADER_SIZE)
                _, payload_len, request_id, *_ = struct.unpack(REQUEST_HEADER_FORMAT, header)
                payload = await reader.readexactly(payload_len)
                # Payload: [ns_len:u16][ns][key_len:u16][key]...
                ns_len = struct.unpack_from("<H", payload, 0)[0]
                key_len = struct.unpack_from("<H", payload, 2 + ns_len)[0]
                key = payload[4 + ns_len : 4 + ns_len + key_len]
                delay = self.delays[min(self.served, len(self.delays) - 1)]
                self.served += 1
                asyncio.get_running_loop().call_later(
                    delay, writer.write, _response(request_id, struct.pack("<Q", 1) + key)
                )
        except (asyncio.IncompleteReadError, ConnectionError):
            pass


@pytest.fixture
async def server_factory() -> AsyncIterator[list[FakeServer]]:
    servers: list[FakeServer] = []
    yield servers
    for s in servers:
        await s.stop()


async def _client(servers: list[FakeServer], delays: list[float], timeout_ms: int) -> FloClient:
    server = FakeServer(delays)
    await server.start()
    servers.append(server)
    return await FloClient(f"127.0.0.1:{server.port}", timeout_ms=timeout_ms).connect()


async def test_blocking_call_waits_past_the_operation_timeout(
    server_factory: list[FakeServer],
) -> None:
    # The server answers after 0.4 s: past the 0.2 s operation timeout, but
    # within the 0.5 s the call asked the server to block.
    client = await _client(server_factory, [0.4], timeout_ms=200)
    result = await client.kv.get("k", GetOptions(block_ms=500))
    assert result is not None and result.value == b"k"
    await client.close()


async def test_blocking_call_still_times_out_after_its_wait(
    server_factory: list[FakeServer],
) -> None:
    client = await _client(server_factory, [1.0], timeout_ms=100)
    with pytest.raises(asyncio.TimeoutError):
        await client.kv.get("k", GetOptions(block_ms=200))


async def test_late_reply_is_not_read_by_the_next_call(
    server_factory: list[FakeServer],
) -> None:
    # The first reply arrives after the call gave up; the second is prompt.
    client = await _client(server_factory, [0.3, 0.0], timeout_ms=100)
    with pytest.raises(asyncio.TimeoutError):
        await client.kv.get("first")
    await asyncio.sleep(0.4)  # the late reply has been sent by now

    assert not client.is_connected
    await client.reconnect()
    result = await client.kv.get("second")
    assert result is not None and result.value == b"second"
    await client.close()


async def test_cancelled_call_does_not_leak_its_reply(
    server_factory: list[FakeServer],
) -> None:
    client = await _client(server_factory, [0.2, 0.0], timeout_ms=5000)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(client.kv.get("first"), timeout=0.05)
    await asyncio.sleep(0.3)

    assert not client.is_connected
    await client.reconnect()
    result = await client.kv.get("second")
    assert result is not None and result.value == b"second"
    await client.close()


async def test_call_queued_behind_a_timed_out_call_is_not_connected(
    server_factory: list[FakeServer],
) -> None:
    client = await _client(server_factory, [0.3, 0.0], timeout_ms=100)
    first = asyncio.create_task(client.kv.get("first"))
    await asyncio.sleep(0.01)  # the first call holds the connection
    second = asyncio.create_task(client.kv.get("second"))
    results = await asyncio.gather(first, second, return_exceptions=True)
    assert isinstance(results[0], asyncio.TimeoutError)
    assert isinstance(results[1], NotConnectedError)


class _RecordedError(Exception):
    pass


@pytest.mark.parametrize("kind", ["action", "stream"])
async def test_worker_connections_use_the_client_timeout(
    kind: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The client adds block_ms to a poll's deadline itself, so a worker that
    # also added it would wait twice as long on a dead server.
    timeouts: list[int] = []

    class RecordingClient(FloClient):
        def __init__(self, endpoint: str, *, timeout_ms: int = 5000, **kw: Any) -> None:
            timeouts.append(timeout_ms)
            super().__init__(endpoint, timeout_ms=timeout_ms, **kw)

        async def connect(self) -> "FloClient":
            raise _RecordedError

    monkeypatch.setattr(worker_mod, "FloClient", RecordingClient)
    parent = FloClient("127.0.0.1:1", timeout_ms=1234)
    worker: ActionWorker | StreamWorker
    if kind == "action":
        worker = parent.new_action_worker(block_ms=30000)
        worker.register_action("a", _noop)
    else:
        worker = parent.new_stream_worker(stream="s", group="g", handler=_noop, block_ms=30000)
    with pytest.raises(_RecordedError):
        await worker.start()
    assert timeouts == [1234]


async def _noop(ctx: Any) -> Any:
    return b""


@pytest.mark.parametrize(
    ("block_ms", "expected"),
    [(None, 30000), (0, 0), (1500, 1500)],
)
def test_action_await_wait(block_ms: int | None, expected: int) -> None:
    from flo.types import OpCode, OptionTag
    from flo.wire import OptionsBuilder

    builder = OptionsBuilder()
    if block_ms is not None:
        builder.add_u32(OptionTag.BLOCK_MS, block_ms)
    assert FloClient._server_wait_ms(OpCode.ACTION_AWAIT, builder.build()) == expected
    assert FloClient._server_wait_ms(OpCode.KV_GET, builder.build()) == (block_ms or 0)
