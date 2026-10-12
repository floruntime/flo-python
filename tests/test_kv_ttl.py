"""KV TTL goes on the wire as a u64 of milliseconds: kv.set's ttl_ms field, and touch's value."""

import struct
from typing import Any

import pytest

from flo import FloClient, PutOptions
from flo.kv_txn import Transaction
from flo.types import OpCode, OptionTag, StatusCode
from flo.wire import RawResponse

Call = tuple[OpCode, str, bytes, bytes, bytes]


@pytest.fixture
def sent(monkeypatch: pytest.MonkeyPatch) -> tuple[FloClient, list[Call]]:
    client = FloClient("localhost:1")
    calls: list[Call] = []

    async def fake_send(
        op_code: OpCode,
        namespace: str,
        key: bytes,
        value: bytes,
        options: bytes = b"",
        **_: Any,
    ) -> RawResponse:
        calls.append((op_code, namespace, key, value, options))
        # Fits both: kv.set's answer, and the touch answer nothing reads.
        return RawResponse(status=StatusCode.OK, data=b"\x01" + struct.pack("<Q", 1), request_id=0)

    monkeypatch.setattr(client, "_send_and_check", fake_send)
    return client, calls


def set_ttl(body: bytes) -> bytes:
    """kv.set's ttl_ms field: after the value's [len:u32][value]."""
    (n,) = struct.unpack("<I", body[:4])
    rest = body[4 + n :]
    return rest[: 9 if rest[0] == 1 else 1]


def test_tag_is_0x01() -> None:
    assert OptionTag.TTL_MS == 0x01


async def test_put_sends_ttl_ms_as_8_bytes(sent: tuple[FloClient, list[Call]]) -> None:
    client, calls = sent
    await client.kv.put("k", b"v", PutOptions(ttl_ms=1500))
    (op, _, _, body, options) = calls[0]
    assert op == OpCode.KV_PUT
    assert options == b""
    assert set_ttl(body) == b"\x01" + struct.pack("<Q", 1500)


async def test_put_without_ttl_sends_ttl_absent(sent: tuple[FloClient, list[Call]]) -> None:
    client, calls = sent
    await client.kv.put("k", b"v")
    await client.kv.put("k", b"v", PutOptions(ttl_ms=0))
    assert set_ttl(calls[0][3]) == b"\x00"
    assert set_ttl(calls[1][3]) == b"\x00"


async def test_touch_sends_ms_value(sent: tuple[FloClient, list[Call]]) -> None:
    client, calls = sent
    await client.kv.touch("k", 1500)
    await client.kv.touch("k", 0)
    assert [(c[0], c[3]) for c in calls] == [
        (OpCode.KV_TOUCH, struct.pack("<Q", 1500)),
        (OpCode.KV_TOUCH, struct.pack("<Q", 0)),
    ]


async def test_txn_put_and_touch_send_ms(sent: tuple[FloClient, list[Call]]) -> None:
    client, calls = sent
    txn = Transaction(client, "default", "k", txn_id=7, pinned_hash=0)
    await txn.put("k", b"v", PutOptions(ttl_ms=2500))
    await txn.touch("k", 2500)
    assert set_ttl(calls[0][3]) == b"\x01" + struct.pack("<Q", 2500)
    assert calls[1][0] == OpCode.KV_TOUCH
    assert calls[1][3] == struct.pack("<Q", 2500)
