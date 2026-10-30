"""Status 12 (unavailable) is a retryable error, and a status this SDK doesn't
know is surfaced without leaving its body on the connection."""

import asyncio
import struct

import pytest

from flo import FloClient, RetryPolicy, StreamID
from flo.exceptions import (
    GenericServerError,
    IncompleteResponseError,
    NoNamespaceError,
    NotFoundError,
    ServerError,
    TableMismatchError,
    UnavailableError,
    raise_for_status,
)
from flo.types import HEADER_SIZE, MAGIC, TABLE_HASH, VERSION, Ran, Reason, StatusCode
from flo.wire import (
    REQUEST_HEADER_FORMAT,
    RESPONSE_HEADER_FORMAT,
    compute_crc32c,
    parse_response,
)


def _refusal(message: bytes, reason: Reason = Reason.UNCLASSIFIED, ran: Ran = Ran.NO) -> bytes:
    """A refusal's body as the server writes it: [reason:u16][ran:u8][message]."""
    return int(reason).to_bytes(2, "little") + bytes([int(ran)]) + message


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
            TABLE_HASH,
        )

    return header(compute_crc32c(header(0), data)) + data


def test_unavailable_maps_to_unavailable_error_with_the_server_message() -> None:
    resp = parse_response(_frame(1, 12, _refusal(b"shard 3 is offline")))
    assert resp.status == StatusCode.UNAVAILABLE
    with pytest.raises(UnavailableError) as raised:
        raise_for_status(resp.status, resp.data)
    assert str(raised.value) == "shard 3 is offline"
    assert raised.value.status_code == StatusCode.UNAVAILABLE


def test_no_namespace_maps_to_its_own_error_not_not_found() -> None:
    resp = parse_response(_frame(1, 14, _refusal(b"namespace 'gone' doesn't exist")))
    assert resp.status == StatusCode.NO_NAMESPACE
    assert not resp.is_not_found()
    with pytest.raises(NoNamespaceError) as raised:
        raise_for_status(resp.status, resp.data)
    assert not isinstance(raised.value, NotFoundError)
    assert str(raised.value) == "namespace 'gone' doesn't exist"
    assert raised.value.status_code == StatusCode.NO_NAMESPACE


def test_unknown_status_maps_to_generic_error_naming_it() -> None:
    resp = parse_response(_frame(1, 200, _refusal(b"something new", ran=Ran.UNKNOWN)))
    assert int(resp.status) == 200
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
                    reply = _frame(request_id, first_status, _refusal(b"first reply's message"))
                else:
                    reply = _frame(
                        request_id, 0, struct.pack("<QI", 7, len(b"second")) + b"second\x00"
                    )
                served += 1
                writer.write(reply)
        except (asyncio.IncompleteReadError, ConnectionError):
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    # Not retried, so the second call reads the next reply.
    client = FloClient(f"127.0.0.1:{port}", timeout_ms=1000, retry=RetryPolicy(max_attempts=1))
    return await client.connect(), server


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
            raise_for_status(StatusCode(code), _refusal(b"why"))
        assert caught.value.status_code == code


async def test_an_answer_from_another_table_is_refused_unread_and_drops_the_connection() -> None:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        header = await reader.readexactly(HEADER_SIZE)
        _, payload_len, request_id, *_ = struct.unpack(REQUEST_HEADER_FORMAT, header)
        await reader.readexactly(payload_len)
        data = b"from elsewhere"

        def head(crc: int) -> bytes:
            return struct.pack(
                RESPONSE_HEADER_FORMAT,
                MAGIC,
                len(data),
                request_id,
                crc,
                VERSION,
                0,
                0,
                0,
                TABLE_HASH + 1,
            )

        writer.write(head(compute_crc32c(head(0), data)) + data)
        await writer.drain()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    client = await FloClient(f"127.0.0.1:{port}", timeout_ms=1000).connect()
    with pytest.raises(TableMismatchError) as raised:
        await client.kv.get("k")
    assert raised.value.server_table == TABLE_HASH + 1
    assert str(raised.value).endswith("upgrade the client")
    assert not client.is_connected
    await client.close()
    server.close()


def test_a_refusal_carries_its_reason_and_ran() -> None:
    body = _refusal(b"do not resend", Reason.COMMITTED_NOT_APPLIED, Ran.YES)
    with pytest.raises(ServerError) as raised:
        raise_for_status(StatusCode.INTERNAL_ERROR, body)
    assert raised.value.reason == Reason.COMMITTED_NOT_APPLIED
    assert raised.value.ran == Ran.YES
    assert str(raised.value) == "do not resend"


# b"\x01\x01" is reason 257: read as a byte it would pass as unknown_op.
@pytest.mark.parametrize("body", [b"\x01\x00", b"\xff\xff\x00", b"\x01\x00\x09", b"\x01\x01\x00"])
def test_a_body_that_is_not_a_refusal_is_refused(body: bytes) -> None:
    with pytest.raises(IncompleteResponseError):
        raise_for_status(StatusCode.BAD_REQUEST, body)


@pytest.mark.parametrize("op", ["group_ack", "group_nack"])
async def test_an_ack_or_nack_on_a_missing_group_raises(op: str) -> None:
    client, server = await _client_answering(int(StatusCode.NOT_FOUND))
    with pytest.raises(NotFoundError):
        await getattr(client.stream, op)("events", "workers", [StreamID(1, 0)])
    await client.close()
    server.close()
