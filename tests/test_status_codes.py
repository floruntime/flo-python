"""Status 12 (unavailable) is a retryable error, and a status this SDK doesn't
know is surfaced without leaving its body on the connection."""

import asyncio
import struct

import pytest

from flo import FloClient
from flo.exceptions import (
    GenericServerError,
    ServerError,
    UnavailableError,
    raise_for_status,
)
from flo.types import HEADER_SIZE, MAGIC, VERSION, StatusCode
from flo.wire import (
    REQUEST_HEADER_FORMAT,
    RESPONSE_HEADER_FORMAT,
    compute_crc32,
    parse_response,
)


def _frame(request_id: int, status: int, data: bytes) -> bytes:
    def header(crc: int) -> bytes:
        return struct.pack(
            RESPONSE_HEADER_FORMAT,
            MAGIC,
            len(data),
            request_id,
            crc,
            VERSION,
            status,
            0,
            0,
            b"\x00" * 8,
        )

    return header(compute_crc32(header(0), data)) + data


def test_unavailable_maps_to_unavailable_error_with_the_server_message() -> None:
    resp = parse_response(_frame(1, 12, b"shard 3 is offline"))
    assert resp.status == StatusCode.UNAVAILABLE
    with pytest.raises(UnavailableError) as raised:
        raise_for_status(resp.status, resp.data)
    assert str(raised.value) == "shard 3 is offline"
    assert raised.value.status_code == StatusCode.UNAVAILABLE


def test_unknown_status_maps_to_generic_error_naming_it() -> None:
    resp = parse_response(_frame(1, 200, b"something new"))
    assert int(resp.status) == 200
    assert resp.data == b"something new"
    with pytest.raises(GenericServerError) as raised:
        raise_for_status(resp.status, resp.data)
    assert str(raised.value) == "Unknown status 200: something new"
    assert int(raised.value.status_code) == 200


async def _client_answering(first_status: int) -> tuple[FloClient, asyncio.Server]:
    """Answers the first request with first_status, every later one with ok."""
    served = 0

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal served
        try:
            while True:
                header = await reader.readexactly(HEADER_SIZE)
                _, payload_len, request_id, *_ = struct.unpack(REQUEST_HEADER_FORMAT, header)
                await reader.readexactly(payload_len)
                if served == 0:
                    reply = _frame(request_id, first_status, b"first reply's message")
                else:
                    reply = _frame(request_id, 0, struct.pack("<Q", 7) + b"second")
                served += 1
                writer.write(reply)
        except (asyncio.IncompleteReadError, ConnectionError):
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return await FloClient(f"127.0.0.1:{port}", timeout_ms=1000).connect(), server


@pytest.mark.parametrize(
    ("status", "error"), [(12, UnavailableError), (200, GenericServerError)], ids=["12", "200"]
)
async def test_error_reply_body_is_not_read_by_the_next_call(
    status: int, error: type[ServerError]
) -> None:
    client, server = await _client_answering(status)
    with pytest.raises(error) as raised:
        await client.kv.get("k")
    assert "first reply's message" in str(raised.value)
    assert int(raised.value.status_code) == status
    assert client.is_connected
    result = await client.kv.get("k")
    assert result is not None
    assert result.value == b"second"
    assert result.version == 7
    await client.close()
    server.close()


def test_an_unknown_status_and_its_error_survive_pickle_and_copy() -> None:
    import copy
    import pickle

    status = StatusCode(200)
    assert pickle.loads(pickle.dumps(status)) == 200
    assert copy.deepcopy(status) == 200
    err = GenericServerError("Unknown status 200: boom", status)
    assert pickle.loads(pickle.dumps(err)).status_code == 200


def test_statuses_without_their_own_error_keep_their_code() -> None:
    for code in (4, 5, 6):
        with pytest.raises(ServerError) as caught:
            raise_for_status(StatusCode(code), b"why")
        assert caught.value.status_code == code
