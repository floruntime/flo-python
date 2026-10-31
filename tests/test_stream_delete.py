"""Stream delete sends the stream as the key and force as a void flag."""

from typing import Any

import pytest

from flo import ConflictError, FloClient, StreamDeleteOptions
from flo.types import OpCode, OptionTag, Ran, Reason, StatusCode
from flo.wire import OptionsIterator, RawResponse

Call = tuple[OpCode, str, bytes, bytes, bytes]


def _client(monkeypatch: pytest.MonkeyPatch, reply: RawResponse) -> tuple[FloClient, list[Call]]:
    client = FloClient("localhost:1", namespace="ns")
    calls: list[Call] = []

    async def fake_send(
        op_code: OpCode, namespace: str, key: bytes, value: bytes, options: bytes = b"", **_: Any
    ) -> RawResponse:
        calls.append((op_code, namespace, key, value, options))
        return reply

    monkeypatch.setattr(client, "_send_request", fake_send)
    return client, calls


_OK = RawResponse(status=StatusCode.OK, data=b"", request_id=0)


async def test_without_force_sends_no_options(monkeypatch: pytest.MonkeyPatch) -> None:
    client, calls = _client(monkeypatch, _OK)
    await client.stream.delete("events")
    assert calls == [(OpCode.STREAM_DELETE, "ns", b"events", b"", b"")]


async def test_force_sends_the_void_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    client, calls = _client(monkeypatch, _OK)
    await client.stream.delete("events", StreamDeleteOptions(namespace="other", force=True))
    op, namespace, key, value, options = calls[0]
    assert (op, namespace, key, value) == (OpCode.STREAM_DELETE, "other", b"events", b"")
    # One TLV: tag 0x2C, zero length.
    assert options == bytes([0x2C, 0x00])
    flag = OptionsIterator(options).find(OptionTag.FORCE)
    assert flag is not None and flag.data == b""


async def test_a_non_empty_stream_is_a_conflict(monkeypatch: pytest.MonkeyPatch) -> None:
    message = b"stream is not empty; pass --force to delete"
    body = int(Reason.UNCLASSIFIED).to_bytes(2, "little") + bytes([int(Ran.NO)]) + message
    client, _ = _client(
        monkeypatch, RawResponse(status=StatusCode.CONFLICT, data=body, request_id=0)
    )
    with pytest.raises(ConflictError, match="not empty"):
        await client.stream.delete("events")


def test_op_and_flag_codes() -> None:
    assert OpCode(0x140) is OpCode.STREAM_DELETE
    assert OptionTag(0x2C) is OptionTag.FORCE
