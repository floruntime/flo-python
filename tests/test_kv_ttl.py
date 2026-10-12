"""KV TTL goes on the wire as a u64 of milliseconds: kv.set's and kv.expire's ttl_ms fields."""

import struct
from typing import Any

import pytest

from flo import FloClient, PutOptions
from flo.exceptions import ValidationError
from flo.kv_body import MAX_TTL_MS
from flo.kv_txn import Transaction
from flo.types import OpCode, StatusCode
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
        # kv.expire answers nothing; kv.set its version.
        data = b"" if op_code == OpCode.KV_EXPIRE else b"\x01" + struct.pack("<Q", 1)
        return RawResponse(status=StatusCode.OK, data=data, request_id=0)

    monkeypatch.setattr(client, "_send_and_check", fake_send)
    return client, calls


def set_ttl(body: bytes) -> bytes:
    """kv.set's ttl_ms field: after the value's [len:u32][value]."""
    (n,) = struct.unpack("<I", body[:4])
    rest = body[4 + n :]
    return rest[: 9 if rest[0] == 1 else 1]


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


async def test_expire_sends_ms_and_zero_clears(sent: tuple[FloClient, list[Call]]) -> None:
    client, calls = sent
    await client.kv.expire("k", 1500)
    await client.kv.expire("k", 0)
    assert [(c[0], c[3][:8]) for c in calls] == [
        (OpCode.KV_EXPIRE, struct.pack("<Q", 1500)),
        (OpCode.KV_EXPIRE, struct.pack("<Q", 0)),
    ]


async def test_txn_put_and_expire_send_ms(sent: tuple[FloClient, list[Call]]) -> None:
    client, calls = sent
    txn = Transaction(client, "default", "k", txn_id=7, pinned_hash=0)
    await txn.put("k", b"v", PutOptions(ttl_ms=2500))
    await txn.expire("k", 2500)
    assert set_ttl(calls[0][3]) == b"\x01" + struct.pack("<Q", 2500)
    assert calls[1][0] == OpCode.KV_EXPIRE
    assert calls[1][3][:8] == struct.pack("<Q", 2500)


async def test_expire_refuses_a_ttl_too_long_before_a_round_trip(
    sent: tuple[FloClient, list[Call]],
) -> None:
    client, calls = sent
    with pytest.raises(ValidationError):
        await client.kv.expire("k", MAX_TTL_MS + 1)
    with pytest.raises(ValidationError):
        await client.kv.expire("k", -1)
    assert calls == []
