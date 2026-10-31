"""The other KV ops as table ops: bodies and answers byte for byte against the
server's schemas, and the behaviour built on them."""

import struct
from collections.abc import Callable
from typing import Any

import pytest

from flo import (
    DeleteOptions,
    FloClient,
    HistoryOptions,
    KVExpireOptions,
    KVIncrOptions,
    KVJsonOptions,
    KVMGetOptions,
    ScanOptions,
)
from flo import kv_body as kb
from flo.exceptions import (
    IncompleteResponseError,
    NoNamespaceError,
    NotFoundError,
    ValidationError,
)
from flo.kv_txn import Transaction, begin
from flo.types import (
    HEADER_SIZE,
    ExistsResult,
    GetResult,
    HistoryResult,
    MGetEntry,
    OpCode,
    Ran,
    Reason,
    StatusCode,
    VersionEntry,
)
from flo.wire import RawResponse, serialize_request

Call = tuple[OpCode, str, bytes, bytes, bytes]
Answer = RawResponse | Callable[[OpCode, bytes, bytes], RawResponse]


def _client(monkeypatch: pytest.MonkeyPatch, *answers: Answer) -> tuple[FloClient, list[Call]]:
    """A client whose requests are recorded and answered in turn (the last answer repeats)."""
    client = FloClient("localhost:1")
    calls: list[Call] = []

    async def fake_send(
        op_code: OpCode, namespace: str, key: bytes, value: bytes, options: bytes = b"", **kw: Any
    ) -> RawResponse:
        calls.append((op_code, namespace, key, value, options))
        answer = answers[min(len(calls), len(answers)) - 1]
        if callable(answer):
            answer = answer(op_code, key, value)
        if answer.status != StatusCode.OK and not (
            answer.status == StatusCode.NOT_FOUND and kw.get("allow_not_found")
        ):
            from flo.exceptions import raise_for_status

            raise_for_status(answer.status, answer.data)
        return answer

    monkeypatch.setattr(client, "_send_and_check", fake_send)
    return client, calls


def _ok(data: bytes | str) -> RawResponse:
    return RawResponse(
        status=StatusCode.OK,
        data=bytes.fromhex(data) if isinstance(data, str) else data,
        request_id=0,
    )


def _refused(status: StatusCode, reason: Reason) -> RawResponse:
    return RawResponse(
        status=status, data=struct.pack("<HB", reason, Ran.NO) + b"why", request_id=0
    )


H = bytes.fromhex


# ── Golden vectors ──────────────────────────────────────────────────────────


def test_request_bodies_match_the_servers_schemas() -> None:
    assert kb.encode_delete_body(7, "rk", None) == H("010700000000000000010200726b00")
    assert kb.encode_scan_body(2, b"c1") == H("010200000002006331")
    assert kb.encode_history_body(2, None, None) == H("01020000000000")
    assert kb.encode_history_body(2, 7, "rk") == H("0102000000010700000000000000010200726b")
    assert kb.encode_mget_body([b"a", b"b"]) == H("020001006101006200")
    assert kb.encode_mget_body([b"a", b"b"], "rk") == H("0200010061010062010200726b")
    assert kb.encode_incr_body(-5, None, 9) == H("01fbffffffffffffff00010900000000000000")
    assert kb.encode_expire_body(60000, None, None, None) == H("60ea000000000000000000")
    assert kb.encode_exists_body(None, None) == H("0000")
    assert kb.encode_json_get_body("$.a", None) == H("010300242e6100")
    assert kb.encode_json_set_body("$", b"{}", "rk") == H("01010024020000007b7d010200726b")
    assert kb.encode_json_del_body("$.b", None) == H("010300242e6200")
    assert kb.encode_txn_body(1) == H("0100000000000000")


def test_answers_match_the_servers_schemas() -> None:
    assert kb.decode_delete_answer(H("0101")) is True
    assert kb.decode_delete_answer(H("00")) is None
    assert kb.decode_scan_answer(H("0200010061020062620102006332")) == (["a", "bb"], b"c2")
    assert kb.decode_scan_answer(H("000000")) == ([], None)
    assert kb.decode_history_answer(H("010300000000000000e8030000000000000002000000763301")) == (
        [VersionEntry(version=3, at_ms=1000, deleted=False, value=b"v3")],
        True,
    )
    # next 2 of two keys asked: all were answered.
    assert kb.decode_mget_answer(
        H("0200010200000000000000010000007800000000000000000000000000010200"), ["a", "b"]
    ) == (
        [
            MGetEntry(key="a", found=True, version=2, value=b"x"),
            MGetEntry(key="b", found=False, version=0, value=b""),
        ],
        None,
    )
    assert kb.decode_incr_answer(H("010400000000000000012a00000000000000")) == (4, 42)
    assert kb.decode_incr_answer(H("0000")) == (None, None)
    kb.decode_empty_answer("kv.expire", b"")
    assert kb.decode_exists_answer(H("010500000000000000")) == ExistsResult(exists=True, version=5)
    assert kb.decode_json_get_answer(H("0300000000000000020000007b7d")) == (3, b"{}")
    assert kb.decode_json_set_answer(H("0300000000000000")) == 3
    assert kb.decode_json_del_answer(H("00")) is None
    assert kb.decode_txn_begin_answer(H("01000000000000000200000000000000")) == (1, 2)
    assert kb.decode_txn_commit_answer(H("0a000000000000000200")) == (10, 2)
    kb.decode_empty_answer("kv.txn_rollback", b"")


def test_get_answer_carries_expiry_when_the_key_has_a_ttl() -> None:
    assert kb.decode_get_answer(H("030000000000000002000000763300")) == GetResult(
        value=b"v3", version=3, expires_at_ms=None
    )
    assert kb.decode_get_answer(H("030000000000000002000000763301e803000000000000")) == GetResult(
        value=b"v3", version=3, expires_at_ms=1000
    )


@pytest.mark.parametrize(
    "answer",
    [
        "030000000000000002000000763302e803000000000000",  # presence byte not 0 or 1
        "030000000000000002000000763301e8030000",  # cut inside the i64
        "0300000000000000020000007633",  # no presence byte
        "03000000000000000200000076330000",  # trailing byte
    ],
)
def test_a_malformed_get_answer_is_refused(answer: str) -> None:
    with pytest.raises(IncompleteResponseError):
        kb.decode_get_answer(H(answer))


def test_history_marks_a_deleted_version() -> None:
    # Newest first: v3 set, v2 deleted (empty value), v1 set.
    answer = (
        "03"
        "0300000000000000"
        "e803000000000000"
        "00"
        "020000007633"
        "0200000000000000"
        "e703000000000000"
        "01"
        "00000000"
        "0100000000000000"
        "e603000000000000"
        "00"
        "020000007631"
        "00"  # more
    )
    assert kb.decode_history_answer(H(answer)) == (
        [
            VersionEntry(version=3, at_ms=1000, deleted=False, value=b"v3"),
            VersionEntry(version=2, at_ms=999, deleted=True, value=b""),
            VersionEntry(version=1, at_ms=998, deleted=False, value=b"v1"),
        ],
        False,
    )


def test_a_history_entry_with_a_bad_deleted_byte_is_refused() -> None:
    with pytest.raises(IncompleteResponseError):
        kb.decode_history_answer(H("010300000000000000e8030000000000000202000000763300"))


def test_mget_golden_answer_with_next() -> None:
    # The golden answer says it answered two keys and resumes at key 2: of a
    # longer request, the first two.
    entries, next_index = kb.decode_mget_answer(
        H("0200010200000000000000010000007800000000000000000000000000010200"), ["a", "b", "c"]
    )
    assert entries == [
        MGetEntry(key="a", found=True, version=2, value=b"x"),
        MGetEntry(key="b", found=False, version=0, value=b""),
    ]
    assert next_index == 2


@pytest.mark.parametrize(
    "decode,data",
    [
        (kb.decode_delete_answer, "01"),
        (kb.decode_delete_answer, "010100"),
        (kb.decode_delete_answer, "0102"),
        (kb.decode_scan_answer, "0200010061"),
        (kb.decode_scan_answer, "00000000"),
        (kb.decode_history_answer, "0103000000000000"),
        (kb.decode_history_answer, "00"),  # no more
        (kb.decode_history_answer, "0002"),  # more not 0 or 1
        (kb.decode_history_answer, "000000"),  # trailing byte
        (kb.decode_incr_answer, "00"),
        (kb.decode_incr_answer, "000000"),
        (kb.decode_exists_answer, "01050000000000000000"),
        (kb.decode_json_get_answer, "0300000000000000030000007b7d"),
        (kb.decode_json_set_answer, "03000000000000"),
        (kb.decode_json_del_answer, "02"),
        (kb.decode_txn_begin_answer, "010000000000000002000000000000"),
        (kb.decode_txn_commit_answer, "0a00000000000000020000"),
    ],
)
def test_answers_decode_whole_or_not_at_all(decode: Callable[[bytes], Any], data: str) -> None:
    with pytest.raises(IncompleteResponseError):
        decode(H(data))


def test_empty_answers_refuse_bytes() -> None:
    with pytest.raises(IncompleteResponseError):
        kb.decode_empty_answer("kv.expire", b"\x00")


def test_every_kv_op_is_a_table_op() -> None:
    kv_ops = [op for op in OpCode if op.name.startswith("KV_")]
    assert len(kv_ops) == 15
    assert all(kb.is_table_op(op) for op in kv_ops)
    assert not kb.is_table_op(OpCode.STREAM_APPEND)
    request = serialize_request(1, OpCode.KV_DELETE, b"ns", b"k", H("000000"), b"")
    assert request[HEADER_SIZE:] == b"\x02\x00ns\x01\x00k" + H("000000")


# ── Ops through the client ──────────────────────────────────────────────────


async def test_delete_says_whether_the_key_existed(monkeypatch: pytest.MonkeyPatch) -> None:
    client, calls = _client(monkeypatch, _ok("0101"), _ok("0100"))
    assert await client.kv.delete("k", DeleteOptions(cas_version=7, routing_key="rk")) is True
    assert await client.kv.delete("missing") is False
    assert calls[0][:4] == (OpCode.KV_DELETE, "default", b"k", H("010700000000000000010200726b00"))
    assert calls[1][3] == H("000000")


async def test_scan_sends_the_prefix_as_key_and_pages_by_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _client(monkeypatch, _ok("0200010061020062620102006332"), _ok("000000"))
    page = await client.kv.scan("p", ScanOptions(limit=2))
    assert (page.names, page.cursor, page.has_more) == (["a", "bb"], b"c2", True)
    page = await client.kv.scan("p", ScanOptions(limit=2, cursor=page.cursor))
    assert (page.names, page.cursor, page.has_more) == ([], None, False)
    assert [(c[2], c[3]) for c in calls] == [
        (b"p", H("01020000000000")),
        (b"p", H("010200000002006332")),
    ]
    await client.kv.scan()
    assert calls[2][2:4] == (b"", b"\x01" + struct.pack("<I", 1024) + b"\x00\x00")


async def test_a_scan_cursor_holding_a_whole_key_round_trips(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # [shard:u16][last key]: the longest a 4096-byte key makes.
    cursor = struct.pack("<H", 3) + b"k" * 4096
    answer = H("0100") + H("01006b") + b"\x01" + struct.pack("<H", len(cursor)) + cursor
    client, calls = _client(monkeypatch, _ok(answer), _ok("000000"))
    page = await client.kv.scan("p", ScanOptions(limit=1))
    assert (page.names, page.cursor) == (["k"], cursor)
    await client.kv.scan("p", ScanOptions(limit=1, cursor=page.cursor))
    assert calls[1][3] == H("0101000000") + struct.pack("<H", len(cursor)) + cursor


async def test_history_carries_each_versions_time(monkeypatch: pytest.MonkeyPatch) -> None:
    client, calls = _client(monkeypatch, _ok("010300000000000000e8030000000000000002000000763301"))
    assert await client.kv.history("k", HistoryOptions(limit=2)) == HistoryResult(
        versions=[VersionEntry(version=3, at_ms=1000, deleted=False, value=b"v3")],
        more=True,
        stale=False,
    )
    await client.kv.history("k", HistoryOptions(routing_key="rk", before_version=3))
    assert [c[3] for c in calls] == [
        H("01020000000000"),
        b"\x01" + struct.pack("<I", 10) + b"\x01" + struct.pack("<Q", 3) + H("010200726b"),
    ]


async def test_history_of_a_missing_key_is_empty_and_other_not_founds_raise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _ = _client(monkeypatch, _refused(StatusCode.NOT_FOUND, Reason.NOT_FOUND))
    assert await client.kv.history("missing") == HistoryResult(versions=[], more=False)
    client, _ = _client(monkeypatch, _refused(StatusCode.NOT_FOUND, Reason.NO_SUCH_TRANSACTION))
    with pytest.raises(NotFoundError):
        await client.kv.history("k")


def _mget_answer(entries: list[tuple[bool, int, bytes]], next_index: int | None) -> bytes:
    out = struct.pack("<H", len(entries))
    for found, version, value in entries:
        out += bytes([found]) + struct.pack("<QI", version, len(value)) + value
    return out + (b"\x00" if next_index is None else b"\x01" + struct.pack("<H", next_index))


async def test_mget_follows_next_until_every_key_is_answered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _client(
        monkeypatch,
        _ok(_mget_answer([(True, 2, b"x"), (False, 0, b"")], 2)),
        _ok(_mget_answer([(True, 5, b"y")], 1)),
        _ok(_mget_answer([(True, 6, b"z")], None)),
    )
    got = await client.kv.mget(["a", b"b", "c", "d"])
    assert got == [
        MGetEntry(key="a", found=True, version=2, value=b"x"),
        MGetEntry(key="b", found=False, version=0, value=b""),
        MGetEntry(key="c", found=True, version=5, value=b"y"),
        MGetEntry(key="d", found=True, version=6, value=b"z"),
    ]
    assert [(c[0], c[2], c[3]) for c in calls] == [
        (OpCode.KV_MGET, b"", kb.encode_mget_body([b"a", b"b", b"c", b"d"])),
        (OpCode.KV_MGET, b"", kb.encode_mget_body([b"c", b"d"])),
        (OpCode.KV_MGET, b"", kb.encode_mget_body([b"d"])),
    ]


async def test_mget_splits_keys_so_each_request_fits_its_frame(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    keys = [f"{i:04d}".encode().ljust(2000, b"x") for i in range(200)]
    client, calls = _client(
        monkeypatch,
        lambda _op, _key, body: _ok(
            _mget_answer([(True, 1, b"v")] * struct.unpack("<H", body[:2])[0], None)
        ),
    )
    got = await client.kv.mget(list(keys), KVMGetOptions(routing_key="rk"))
    assert [e.key for e in got] == [k.decode() for k in keys]
    bodies = [c[3] for c in calls]
    # 126 * 2002 = 252252 bytes of key elements fit; one more key doesn't.
    assert bodies == [
        kb.encode_mget_body(keys[:126], "rk"),
        kb.encode_mget_body(keys[126:], "rk"),
    ]
    assert all(b.endswith(H("010200726b")) for b in bodies)
    with pytest.raises(ValidationError):
        kb.encode_mget_body(keys[:127])


async def test_mget_follows_next_within_a_split_run(monkeypatch: pytest.MonkeyPatch) -> None:
    keys = [f"{i:04d}".encode().ljust(2000, b"x") for i in range(130)]
    client, calls = _client(
        monkeypatch,
        _ok(_mget_answer([(True, 1, b"")] * 100, 100)),
        _ok(_mget_answer([(True, 1, b"")] * 26, None)),
        _ok(_mget_answer([(True, 1, b"")] * 4, None)),
    )
    assert len(await client.kv.mget(list(keys))) == 130
    assert [c[3] for c in calls] == [
        kb.encode_mget_body(keys[:126]),
        kb.encode_mget_body(keys[100:126]),
        kb.encode_mget_body(keys[126:]),
    ]


async def test_mget_refuses_an_answer_that_makes_no_progress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _ = _client(monkeypatch, _ok(_mget_answer([], 0)))
    with pytest.raises(IncompleteResponseError):
        await client.kv.mget(["a"])


async def test_incr_returns_the_new_value_and_version(monkeypatch: pytest.MonkeyPatch) -> None:
    client, calls = _client(monkeypatch, _ok("010400000000000000012a00000000000000"))
    res = await client.kv.incr("n", KVIncrOptions(delta=-5))
    assert (res.value, res.version) == (42, 4)
    await client.kv.incr("n")
    assert [c[3] for c in calls] == [
        H("01fbffffffffffffff0000"),
        b"\x01" + struct.pack("<q", 1) + b"\x00\x00",
    ]


async def test_incr_on_the_wrong_type_raises_wrong_type(monkeypatch: pytest.MonkeyPatch) -> None:
    client, _ = _client(monkeypatch, _refused(StatusCode.BAD_REQUEST, Reason.WRONG_TYPE))
    with pytest.raises(Exception) as raised:
        await client.kv.incr("n")
    assert raised.value.reason == Reason.WRONG_TYPE  # type: ignore[attr-defined]


async def test_expire_sends_cas_and_routing_key(monkeypatch: pytest.MonkeyPatch) -> None:
    client, calls = _client(monkeypatch, _ok(b""))
    await client.kv.expire("k", 60000)
    await client.kv.expire("k", 0, KVExpireOptions(cas_version=3, routing_key="rk"))
    assert calls[0][:4] == (OpCode.KV_EXPIRE, "default", b"k", H("60ea000000000000000000"))
    assert calls[1][3] == struct.pack("<Q", 0) + b"\x01" + struct.pack("<Q", 3) + H("010200726b00")


async def test_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    client, calls = _client(monkeypatch, _ok("010500000000000000"), _ok("000000000000000000"))
    assert await client.kv.exists("k") == ExistsResult(exists=True, version=5)
    assert await client.kv.exists("k") == ExistsResult(exists=False, version=0)
    assert calls[0][3] == H("0000")


async def test_an_exists_result_is_truthy_only_when_the_key_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _ = _client(monkeypatch, _ok("010500000000000000"), _ok("000000000000000000"))
    # `if await kv.exists(key):` keeps meaning what it did when exists was a bool.
    assert await client.kv.exists("k")
    assert not await client.kv.exists("k")
    assert bool(ExistsResult(exists=False, version=7, stale=True)) is False


async def test_json_ops(monkeypatch: pytest.MonkeyPatch) -> None:
    client, calls = _client(
        monkeypatch,
        _ok("0300000000000000020000007b7d"),
        _ok("0300000000000000"),
        _ok("00"),
        _ok("01" + "0400000000000000"),
    )
    got = await client.kv.json_get("k", "$.a")
    assert got is not None and (got.value, got.version) == ({}, 3)
    assert (await client.kv.json_set("k", "$", b"{}", KVJsonOptions(routing_key="rk"))).version == 3
    assert await client.kv.json_del("k", "$.b") is None
    res = await client.kv.json_del("k", "$.c")
    assert res is not None and res.version == 4
    assert [(c[0], c[3]) for c in calls] == [
        (OpCode.KV_JSON_GET, H("010300242e6100")),
        (OpCode.KV_JSON_SET, H("01010024020000007b7d010200726b")),
        (OpCode.KV_JSON_DEL, H("010300242e6200")),
        (OpCode.KV_JSON_DEL, H("010300242e6300")),
    ]


async def test_json_get_is_absent_on_not_found_and_raises_on_any_other(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _ = _client(monkeypatch, _refused(StatusCode.NOT_FOUND, Reason.NOT_FOUND))
    assert await client.kv.json_get("k") is None
    client, _ = _client(monkeypatch, _refused(StatusCode.NOT_FOUND, Reason.NO_SUCH_TRANSACTION))
    with pytest.raises(NotFoundError):
        await client.kv.json_get("k")


async def test_a_missing_namespace_raises_though_its_reason_is_not_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The server sends no_namespace with reason NOT_FOUND: only the status
    # tells it from an absent key.
    client, _ = _client(monkeypatch, _refused(StatusCode.NO_NAMESPACE, Reason.NOT_FOUND))
    with pytest.raises(NoNamespaceError):
        await client.kv.json_get("k")
    with pytest.raises(NoNamespaceError):
        await client.kv.get("k")


async def test_bounds_are_refused_before_a_round_trip(monkeypatch: pytest.MonkeyPatch) -> None:
    client, calls = _client(monkeypatch, _ok(b""))
    refused = [
        client.kv.scan("p", ScanOptions(limit=0)),
        client.kv.scan("p", ScanOptions(limit=1025)),
        client.kv.scan("p", ScanOptions(cursor=bytes(4099))),
        client.kv.history("k", HistoryOptions(limit=66)),
        client.kv.history("k", HistoryOptions(limit=0)),
        client.kv.mget([f"k{i}" for i in range(257)]),
        client.kv.mget(["k", "x" * 4097]),
        client.kv.mget(["k"], KVMGetOptions(routing_key="r" * 4097)),
        client.kv.incr("n", KVIncrOptions(delta=1 << 63)),
        client.kv.delete("k", DeleteOptions(routing_key="r" * 4097)),
        client.kv.delete("k", DeleteOptions(cas_version=-1)),
        client.kv.json_get("k", "$" + "a" * 1024),
        client.kv.json_set("k", "$", bytes(252_820)),
    ]
    for call in refused:
        with pytest.raises(ValidationError):
            await call
    assert await client.kv.mget([]) == []
    with pytest.raises(ValidationError):
        await begin(client, "ns", "")
    assert calls == []


# ── Transactions ────────────────────────────────────────────────────────────


async def test_begin_sends_the_routing_key_as_key_and_an_empty_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _client(monkeypatch, _ok("01000000000000000200000000000000"))
    txn = await client.kv.begin("rk")
    assert (txn.id, txn.pinned_hash) == (1, 2)
    assert calls[0][:4] == (OpCode.KV_BEGIN_TXN, "default", b"rk", b"")


def _txn_answer(op: OpCode, key: bytes, body: bytes) -> RawResponse:
    return {
        OpCode.KV_DELETE: _ok("00"),
        OpCode.KV_INCR: _ok("0000"),
        OpCode.KV_EXPIRE: _ok(b""),
        OpCode.KV_EXISTS: _ok("010000000000000000"),
        OpCode.KV_COMMIT_TXN: _ok("0a000000000000000200"),
        OpCode.KV_ROLLBACK_TXN: _ok(b""),
    }[op]


async def test_txn_ops_carry_the_txn_and_its_routing_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _client(monkeypatch, _txn_answer)
    txn = Transaction(client, "ns", "rk", txn_id=1, pinned_hash=0)
    assert await txn.delete("a", DeleteOptions(routing_key="ignored")) is None
    assert await txn.incr("b", -5) is None
    await txn.expire("c", 60000)
    assert (await txn.exists("d")).exists is True
    res = await txn.commit()
    assert (res.commit_index, res.op_count) == (10, 2)

    tail = H("010200726b") + H("01") + struct.pack("<Q", 1)
    assert [(c[0], c[2], c[3]) for c in calls] == [
        (OpCode.KV_DELETE, b"a", b"\x00" + tail),
        (OpCode.KV_INCR, b"b", H("01fbffffffffffffff") + tail),
        (OpCode.KV_EXPIRE, b"c", struct.pack("<Q", 60000) + b"\x00" + tail),
        (OpCode.KV_EXISTS, b"d", tail),
        (OpCode.KV_COMMIT_TXN, b"", H("0100000000000000")),
    ]


async def test_rollback_sends_the_txn_with_an_empty_key(monkeypatch: pytest.MonkeyPatch) -> None:
    client, calls = _client(monkeypatch, _txn_answer)
    await Transaction(client, "ns", "rk", txn_id=1, pinned_hash=0).rollback()
    assert calls[0][:4] == (OpCode.KV_ROLLBACK_TXN, "ns", b"", H("0100000000000000"))


async def test_txn_op_on_an_unknown_txn_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    client, _ = _client(monkeypatch, _refused(StatusCode.NOT_FOUND, Reason.NO_SUCH_TRANSACTION))
    with pytest.raises(NotFoundError):
        await Transaction(client, "ns", "rk", txn_id=1, pinned_hash=0).delete("k")
