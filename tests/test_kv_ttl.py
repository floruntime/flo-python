"""KV TTL goes on the wire as a u64 of milliseconds."""

import struct
from typing import Any

import pytest

from flo import FloClient, PutOptions
from flo.kv_txn import Transaction
from flo.types import OpCode, OptionTag, StatusCode
from flo.wire import OptionsIterator, RawResponse

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
        return RawResponse(status=StatusCode.OK, data=struct.pack("<Q", 1), request_id=0)

    monkeypatch.setattr(client, "_send_and_check", fake_send)
    return client, calls


def ttl_option(options: bytes) -> bytes:
    opt = OptionsIterator(options).find(OptionTag.TTL_MS)
    assert opt is not None
    return opt.data


def test_tag_is_0x01() -> None:
    assert OptionTag.TTL_MS == 0x01


async def test_put_sends_ttl_ms_as_8_bytes(sent: tuple[FloClient, list[Call]]) -> None:
    client, calls = sent
    await client.kv.put("k", b"v", PutOptions(ttl_ms=1500))
    (op, _, _, _, options) = calls[0]
    assert op == OpCode.KV_PUT
    assert ttl_option(options) == struct.pack("<Q", 1500)


async def test_put_without_ttl_sends_no_ttl_option(sent: tuple[FloClient, list[Call]]) -> None:
    client, calls = sent
    await client.kv.put("k", b"v")
    assert OptionsIterator(calls[0][4]).find(OptionTag.TTL_MS) is None


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
    assert ttl_option(calls[0][4]) == struct.pack("<Q", 2500)
    assert calls[1][0] == OpCode.KV_TOUCH
    assert calls[1][3] == struct.pack("<Q", 2500)
