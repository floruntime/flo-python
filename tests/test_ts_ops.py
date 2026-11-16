"""ts.read's body and answer, the pages it walks, and ts.write's ns time."""

import struct
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from flo import (
    FloClient,
    TSOrder,
    TSPoint,
    TSReadOptions,
    TSWriteOptions,
    datetime_from_ns,
    ns_from_datetime,
)
from flo.exceptions import (
    BadRequestError,
    IncompleteResponseError,
    NotFoundError,
    ValidationError,
)
from flo.kv_body import is_table_op
from flo.ts import (
    decode_floql_answer,
    decode_read_answer,
    decode_write_answer,
    encode_read_body,
    encode_write_options,
)
from flo.types import OpCode, OptionTag, Ran, Reason, StatusCode
from flo.wire import OptionsIterator, RawResponse

# Not a whole millisecond, nor a whole microsecond.
ODD_NS = 1_700_000_000_123_456_789
CURSOR = bytes(range(16))


def _answer(points: list[tuple[int, float]], cursor: bytes | None) -> bytes:
    out = struct.pack("<H", len(points)) + b"".join(struct.pack("<qd", t, v) for t, v in points)
    if cursor is None:
        return out + b"\x00"
    return out + b"\x01" + bytes([len(cursor)]) + cursor


def _refusal(status: StatusCode, message: bytes) -> RawResponse:
    data = struct.pack("<HB", Reason.MALFORMED, Ran.NO) + message
    return RawResponse(status=status, data=data, request_id=0)


# ── Request ──────────────────────────────────────────────────────────────


def test_ts_read_is_a_table_op() -> None:
    assert is_table_op(OpCode.TS_READ)
    assert not is_table_op(OpCode.TS_WRITE)


def test_read_body_carries_every_field() -> None:
    body = encode_read_body(
        TSReadOptions(
            field="idle",
            tags={"host": "a", "dc": "x"},
            from_ns=5,
            to_ns=ODD_NS,
            limit=100,
            order=TSOrder.NEWEST_FIRST,
            cursor=CURSOR,
        )
    )
    assert body == (
        b"\x01" + struct.pack("<H", 4) + b"idle"
        + b"\x01" + struct.pack("<H", 11) + b"host=a,dc=x"
        + b"\x01" + struct.pack("<q", 5)
        + b"\x01" + struct.pack("<q", ODD_NS)
        + b"\x01" + struct.pack("<I", 100)
        + b"\x01\x01"
        + b"\x01\x10" + CURSOR
    )  # fmt: skip


def test_read_body_defaults_are_sent_present() -> None:
    assert encode_read_body(TSReadOptions()) == (
        b"\x01" + struct.pack("<H", 5) + b"value"
        + b"\x01" + struct.pack("<H", 0)
        + b"\x00"
        + b"\x00"
        + b"\x01" + struct.pack("<I", 4096)
        + b"\x01\x00"
        + b"\x01\x00"
    )  # fmt: skip


@pytest.mark.parametrize(
    "opts",
    [
        TSReadOptions(limit=0),
        TSReadOptions(limit=4097),
        TSReadOptions(from_ns=-1),
        TSReadOptions(to_ns=1 << 63),
        TSReadOptions(cursor=bytes(17)),
        TSReadOptions(tags="k=" + "v" * 1023),
    ],
)
def test_read_body_refuses_what_the_server_would(opts: TSReadOptions) -> None:
    with pytest.raises(ValidationError):
        encode_read_body(opts)


# ── Answer ───────────────────────────────────────────────────────────────


def test_read_answer_with_a_cursor() -> None:
    points, cursor = decode_read_answer(_answer([(ODD_NS, 1.5), (ODD_NS + 1, -2.0)], CURSOR))
    assert points == [TSPoint(ODD_NS, 1.5), TSPoint(ODD_NS + 1, -2.0)]
    assert cursor == CURSOR


def test_read_answer_without_a_cursor() -> None:
    assert decode_read_answer(_answer([(7, 0.25)], None)) == ([TSPoint(7, 0.25)], None)
    assert decode_read_answer(_answer([], None)) == ([], None)


@pytest.mark.parametrize("cut", range(1, len(_answer([(1, 1.0)], CURSOR)) + 1))
def test_read_answer_cut_short_is_refused(cut: int) -> None:
    with pytest.raises(IncompleteResponseError):
        decode_read_answer(_answer([(1, 1.0)], CURSOR)[:-cut])


def test_read_answer_with_bytes_left_over_is_refused() -> None:
    with pytest.raises(IncompleteResponseError, match="left over"):
        decode_read_answer(_answer([], None) + b"\x00")


# ── Paging ───────────────────────────────────────────────────────────────


def _paged(
    monkeypatch: pytest.MonkeyPatch, pages: list[RawResponse]
) -> tuple[FloClient, list[tuple[OpCode, bytes, bytes, bytes]]]:
    client = FloClient("localhost:1")
    calls: list[tuple[OpCode, bytes, bytes, bytes]] = []

    async def fake_send(
        op_code: OpCode, namespace: str, key: bytes, value: bytes, options: bytes = b"", **_: Any
    ) -> RawResponse:
        calls.append((op_code, key, value, options))
        return pages[len(calls) - 1]

    monkeypatch.setattr(client, "_send_request", fake_send)
    return client, calls


def _ok(data: bytes) -> RawResponse:
    return RawResponse(status=StatusCode.OK, data=data, request_id=0)


async def test_read_sends_the_measurement_as_key(monkeypatch: pytest.MonkeyPatch) -> None:
    client, calls = _paged(monkeypatch, [_ok(_answer([(1, 1.0)], None))])
    page = await client.ts.read("cpu", TSReadOptions(limit=10))
    assert (page.points, page.cursor, page.has_more) == ([TSPoint(1, 1.0)], None, False)
    [(op, key, value, options)] = calls
    assert (op, key, options) == (OpCode.TS_READ, b"cpu", b"")
    assert value == encode_read_body(TSReadOptions(limit=10))


async def test_iterator_stops_on_a_none_cursor_not_a_short_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    c1, c2 = b"\x01" * 16, b"\x02" * 16
    # Short pages that still carry a cursor, then a full page that doesn't.
    client, calls = _paged(
        monkeypatch,
        [
            _ok(_answer([(1, 1.0)], c1)),
            _ok(_answer([], c2)),
            _ok(_answer([(2, 2.0), (3, 3.0)], None)),
        ],
    )
    got = [p async for p in client.ts.iter_points("cpu", TSReadOptions(limit=2))]
    assert [p.time_ns for p in got] == [1, 2, 3]
    assert [_body(value)["cursor"] for _, _, value, _ in calls] == [b"", c1, c2]


def _body(body: bytes) -> dict[str, Any]:
    """A ts.read request body read back, field by field."""
    at = 0

    def take(n: int) -> bytes:
        nonlocal at
        at += n
        return body[at - n : at]

    def present() -> bool:
        return take(1) == b"\x01"

    out: dict[str, Any] = {}
    for name in ("field", "tags"):
        assert present()
        (n,) = struct.unpack("<H", take(2))
        out[name] = take(n)
    for name in ("from_ns", "to_ns"):
        out[name] = struct.unpack("<q", take(8))[0] if present() else None
    assert present()
    out["limit"] = struct.unpack("<I", take(4))[0]
    assert present()
    out["order"] = TSOrder(take(1)[0])
    assert present()
    out["cursor"] = take(take(1)[0])
    assert at == len(body)
    return out


async def test_iterator_newest_first_keeps_its_order_on_every_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _paged(
        monkeypatch,
        [_ok(_answer([(9, 9.0)], CURSOR)), _ok(_answer([(8, 8.0)], None))],
    )
    opts = TSReadOptions(order=TSOrder.NEWEST_FIRST, limit=1)
    got = [p.time_ns async for p in client.ts.iter_points("cpu", opts)]
    assert got == [9, 8]
    sent = [_body(value) for _, _, value, _ in calls]
    assert [(b["order"], b["limit"], b["cursor"]) for b in sent] == [
        (TSOrder.NEWEST_FIRST, 1, b""),
        (TSOrder.NEWEST_FIRST, 1, CURSOR),
    ]


async def test_a_bad_cursor_is_refused_with_the_servers_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    text = b"bad request: a cursor is the 16 bytes a page answered"
    client, _ = _paged(monkeypatch, [_refusal(StatusCode.BAD_REQUEST, text)])
    with pytest.raises(BadRequestError, match="a cursor is the 16 bytes") as e:
        await client.ts.read("cpu", TSReadOptions(cursor=b"short"))
    assert e.value.reason == Reason.MALFORMED


async def test_a_not_found_page_raises_from_the_iterator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, calls = _paged(
        monkeypatch,
        [_ok(_answer([(1, 1.0)], CURSOR)), _refusal(StatusCode.NOT_FOUND, b"not found")],
    )
    got: list[int] = []
    with pytest.raises(NotFoundError):
        async for p in client.ts.iter_points("cpu"):
            got.append(p.time_ns)
    assert got == [1]
    assert len(calls) == 2


# ── Write ────────────────────────────────────────────────────────────────


def test_write_time_is_an_i64_of_ns() -> None:
    opts = encode_write_options(TSWriteOptions(time_ns=ODD_NS))
    opt = OptionsIterator(opts).find(OptionTag.TS_TIMESTAMP_NS)
    assert opt is not None
    assert opt.data == struct.pack("<q", ODD_NS)


def test_write_without_a_time_sends_none() -> None:
    assert OptionsIterator(encode_write_options(TSWriteOptions())).find(0x67) is None


def test_write_options_carry_field_and_tags() -> None:
    opts = OptionsIterator(encode_write_options(TSWriteOptions(field="idle", tags={"h": "a"})))
    field, tags = opts.find(OptionTag.TS_FIELD), opts.find(OptionTag.TS_TAGS)
    assert field is not None and field.data == b"idle"
    assert tags is not None and tags.data == b"h=a"


def test_a_negative_time_goes_to_the_server_to_refuse() -> None:
    # The server's text says why; the encoder sends any i64.
    opt = OptionsIterator(encode_write_options(TSWriteOptions(time_ns=-1))).find(0x67)
    assert opt is not None and opt.data == struct.pack("<q", -1)


def test_a_time_past_i64_is_refused() -> None:
    with pytest.raises(ValidationError):
        encode_write_options(TSWriteOptions(time_ns=1 << 63))


def test_write_answer_time_is_ns() -> None:
    r = decode_write_answer(struct.pack("<QqQ", 0xABCD, ODD_NS, 42))
    assert (r.time_ns, r.sequence, r.series_hash) == (ODD_NS, 42, 0xABCD)
    with pytest.raises(IncompleteResponseError):
        decode_write_answer(b"\x00" * 16)


async def test_write_refusal_carries_the_servers_text(monkeypatch: pytest.MonkeyPatch) -> None:
    text = b"ts write: a point's time must not be before 1970"
    client, calls = _paged(monkeypatch, [_refusal(StatusCode.BAD_REQUEST, text)])
    with pytest.raises(BadRequestError, match="before 1970"):
        await client.ts.write("cpu", 1.0, TSWriteOptions(time_ns=-5))
    [(op, key, value, _)] = calls
    assert (op, key, value) == (OpCode.TS_WRITE, b"cpu", struct.pack("<d", 1.0))


async def test_write_refuses_a_value_that_isnt_finite() -> None:
    with pytest.raises(ValidationError):
        await FloClient("localhost:1").ts.write("cpu", float("nan"))


# ── Times ────────────────────────────────────────────────────────────────


def test_datetime_helpers_round_trip_to_the_microsecond() -> None:
    dt = datetime_from_ns(ODD_NS)
    assert dt.tzinfo == timezone.utc
    assert ns_from_datetime(dt) == ODD_NS - ODD_NS % 1_000
    assert TSPoint(ODD_NS, 0.0).time == dt
    offset = datetime(2024, 1, 1, 1, tzinfo=timezone(timedelta(hours=1)))
    assert ns_from_datetime(offset) == ns_from_datetime(datetime(2024, 1, 1, tzinfo=timezone.utc))


def test_a_naive_datetime_is_refused() -> None:
    with pytest.raises(ValueError):
        ns_from_datetime(datetime(2024, 1, 1))


# ── FloQL ────────────────────────────────────────────────────────────────


def test_floql_answer() -> None:
    def s(b: bytes) -> bytes:
        return struct.pack("<I", len(b)) + b

    data = struct.pack("<I", 1) + s(b"cpu") + s(b"value") + struct.pack("<Iqd", 1, 5, 0.5)
    [series] = decode_floql_answer(data)
    assert (series.key, series.field) == ("cpu", "value")
    # The server answers ms; the SDK gives ns.
    assert [(p.time_ns, p.value) for p in series.points] == [(5_000_000, 0.5)]
    with pytest.raises(IncompleteResponseError):
        decode_floql_answer(data[:-1])
