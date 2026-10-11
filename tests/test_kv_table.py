"""kv.get and kv.set as table ops: schema-coded bodies out, the ops'
response schemas back."""

import struct
from typing import Any

import pytest

from flo import Condition, FloClient, GetOptions, PutOptions, WaitFor
from flo.exceptions import (
    BlockTooLongError,
    IncompleteResponseError,
    NotFoundError,
    ValidationError,
)
from flo.kv_body import decode_get_answer, decode_set_answer
from flo.kv_txn import Transaction
from flo.types import MAX_KV_VALUE_SIZE, MAX_WAIT_MS, OpCode, OptionTag, Ran, Reason, StatusCode
from flo.wire import RawResponse

Call = tuple[OpCode, str, bytes, bytes, bytes]


def _client(monkeypatch: pytest.MonkeyPatch, answer: RawResponse) -> tuple[FloClient, list[Call]]:
    client = FloClient("localhost:1")
    calls: list[Call] = []

    async def fake_send(
        op_code: OpCode, namespace: str, key: bytes, value: bytes, options: bytes = b"", **_: Any
    ) -> RawResponse:
        calls.append((op_code, namespace, key, value, options))
        if answer.status != StatusCode.OK and not (
            answer.status == StatusCode.NOT_FOUND and _.get("allow_not_found")
        ):
            from flo.exceptions import raise_for_status

            raise_for_status(answer.status, answer.data)
        return answer

    monkeypatch.setattr(client, "_send_and_check", fake_send)
    return client, calls


def _ok(data: bytes) -> RawResponse:
    return RawResponse(status=StatusCode.OK, data=data, request_id=0)


def _refused(status: StatusCode, reason: Reason) -> RawResponse:
    return RawResponse(
        status=status, data=struct.pack("<HB", reason, Ran.NO) + b"why", request_id=0
    )


def _get_answer(version: int, value: bytes) -> RawResponse:
    return _ok(struct.pack("<QI", version, len(value)) + value)


async def test_get_sends_wait_ms_wait_for_routing_key_and_txn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _client(monkeypatch, _get_answer(4, b"hi"))
    got = await client.kv.get(
        "k", GetOptions(wait_ms=5000, wait_for=WaitFor.CHANGE, routing_key="rk")
    )
    assert (got.version, got.value) == (4, b"hi")
    op, _, key, body, options = calls[0]
    assert (op, key, options) == (OpCode.KV_GET, b"k", b"")
    assert body == b"\x01" + struct.pack("<Q", 5000) + b"\x01\x01" + b"\x01\x02\x00rk" + b"\x00"


async def test_get_is_absent_on_not_found_and_raises_on_any_other(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _ = _client(monkeypatch, _refused(StatusCode.NOT_FOUND, Reason.NOT_FOUND))
    assert await client.kv.get("k") is None
    client, _ = _client(monkeypatch, _refused(StatusCode.NOT_FOUND, Reason.NO_SUCH_TRANSACTION))
    with pytest.raises(NotFoundError) as raised:
        await client.kv.get("k")
    assert raised.value.reason == Reason.NO_SUCH_TRANSACTION


async def test_set_sends_condition_and_cas_and_reads_its_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _client(monkeypatch, _ok(b"\x01" + struct.pack("<Q", 9)))
    res = await client.kv.put("k", b"v", PutOptions(condition=Condition.IF_ABSENT, cas_version=0))
    assert res.version == 9
    assert (
        calls[0][3]
        == struct.pack("<I", 1)
        + b"v"
        + b"\x00"
        + b"\x01\x01"
        + b"\x01"
        + struct.pack("<Q", 0)
        + b"\x00\x00"
    )


async def test_out_of_bound_fields_are_refused_before_a_round_trip() -> None:
    client = FloClient("localhost:1")
    with pytest.raises(BlockTooLongError):
        await client.kv.get("k", GetOptions(wait_ms=MAX_WAIT_MS + 1))
    with pytest.raises(ValidationError):
        await client.kv.put("k", bytes(MAX_KV_VALUE_SIZE + 1))
    with pytest.raises(ValueError):
        await client.kv.put("k", b"v", PutOptions(condition=3))  # type: ignore[arg-type]


async def test_a_transaction_sends_its_id_and_commits_by_id_alone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    txn_id = (3 << 48) | 7
    client, calls = _client(monkeypatch, _get_answer(0, b"5"))
    txn = Transaction(client, "ns", "rk", txn_id=txn_id, pinned_hash=0)
    await txn.get("k")
    assert calls[0][3] == b"\x01" + struct.pack(
        "<Q", 0
    ) + b"\x01\x00" + b"\x01\x02\x00rk" + b"\x01" + struct.pack("<Q", txn_id)

    client, calls = _client(monkeypatch, _ok(b"\x00"))
    txn = Transaction(client, "ns", "rk", txn_id=txn_id, pinned_hash=0)
    assert (await txn.put("k", b"v")).version == 0
    assert calls[0][3].endswith(b"\x01\x02\x00rk" + b"\x01" + struct.pack("<Q", txn_id))

    client, calls = _client(monkeypatch, _ok(b"\x01" + struct.pack("<QH", 5, 1)))
    await Transaction(client, "ns", "rk", txn_id=txn_id, pinned_hash=0).commit()
    _, _, key, value, options = calls[0]
    assert (key, value) == (b"", b"")
    assert options == bytes([OptionTag.TXN_ID, 8]) + struct.pack("<Q", txn_id)


def test_answers_decode_whole_or_not_at_all() -> None:
    with pytest.raises(IncompleteResponseError):
        decode_get_answer(struct.pack("<QI", 1, 3) + b"x")
    with pytest.raises(IncompleteResponseError):
        decode_set_answer(b"\x02")
    assert decode_set_answer(b"\x00").version == 0


def test_new_reasons_are_known() -> None:
    assert [r.value for r in Reason if r.value >= 13] == list(range(13, 24))
