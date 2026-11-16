"""Flo time-series operations: write, read (paged) and FloQL.

Every time is Unix nanoseconds, an i64.
"""

from __future__ import annotations

import math
import struct
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import TYPE_CHECKING

from .exceptions import IncompleteResponseError, ValidationError, not_found_error
from .types import (
    MAX_TS_READ,
    MAX_TS_TAG_FILTER,
    FloQLPoint,
    FloQLSeries,
    OpCode,
    OptionTag,
    TSAggregation,
    TSBucket,
    TSInfo,
    TSOrder,
    TSPoint,
    TSQueryOptions,
    TSReadOptions,
    TSReadResult,
    TSWriteOptions,
    TSWriteResult,
    ns_from_timedelta,
)
from .wire import OptionsBuilder

if TYPE_CHECKING:
    from .client import FloClient

_I64_MAX = (1 << 63) - 1
_I64_MIN = -(1 << 63)
_MAX_FIELD = 0xFFFF
_CURSOR_LEN = 16


def _tags(tags: str | dict[str, str]) -> bytes:
    if isinstance(tags, dict):
        tags = ",".join(f"{k}={v}" for k, v in tags.items())
    return tags.encode("utf-8")


def _time(name: str, v: int | None) -> bytes:
    """An optional read bound: a presence byte, then an i64 of ns, 0 or more."""
    if v is None:
        return b"\x00"
    if not isinstance(v, int) or isinstance(v, bool) or not 0 <= v <= _I64_MAX:
        raise ValidationError(f"{name} must be Unix ns, 0..2^63-1, got {v!r}")
    return b"\x01" + struct.pack("<q", v)


def encode_read_body(opts: TSReadOptions) -> bytes:
    """ts.read's request: field, tags, from_ns, to_ns, limit, order, cursor.
    Every defaulted field is sent present."""
    field = opts.field.encode("utf-8")
    if len(field) > _MAX_FIELD:
        raise ValidationError(f"field is {len(field)} bytes, at most {_MAX_FIELD}")
    tags = _tags(opts.tags)
    if len(tags) > MAX_TS_TAG_FILTER:
        raise ValidationError(f"tag filter is {len(tags)} bytes, at most {MAX_TS_TAG_FILTER}")
    limit = opts.limit
    if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= MAX_TS_READ:
        raise ValidationError(f"limit must be 1..{MAX_TS_READ}, got {limit!r}")
    order = TSOrder(opts.order)
    cursor = bytes(opts.cursor or b"")
    if len(cursor) > _CURSOR_LEN:
        raise ValidationError(f"cursor is {len(cursor)} bytes, at most {_CURSOR_LEN}")
    return b"".join(
        (
            b"\x01" + struct.pack("<H", len(field)) + field,
            b"\x01" + struct.pack("<H", len(tags)) + tags,
            _time("from_ns", opts.from_ns),
            _time("to_ns", opts.to_ns),
            b"\x01" + struct.pack("<I", limit),
            b"\x01" + bytes([order]),
            b"\x01" + bytes([len(cursor)]) + cursor,
        )
    )


def decode_read_answer(data: bytes) -> tuple[list[TSPoint], bytes | None]:
    """ts.read's answer: [count:u16]([time_ns:i64][value:f64])*, then the
    cursor: [present:u8]([len:u8][bytes]), absent on the last page."""
    view = memoryview(data)

    def cut() -> IncompleteResponseError:
        return IncompleteResponseError("ts.read answer is cut short")

    if len(view) < 2:
        raise cut()
    (count,) = struct.unpack_from("<H", view, 0)
    at = 2
    if len(view) < at + 16 * count:
        raise cut()
    points = [
        TSPoint(time_ns=t, value=v)
        for t, v in struct.iter_unpack("<qd", view[at : at + 16 * count])
    ]
    at += 16 * count
    if len(view) < at + 1:
        raise cut()
    present = view[at]
    at += 1
    cursor: bytes | None = None
    if present == 1:
        if len(view) < at + 1:
            raise cut()
        n = view[at]
        at += 1
        if len(view) < at + n:
            raise cut()
        cursor = bytes(view[at : at + n])
        at += n
    elif present != 0:
        raise IncompleteResponseError(f"ts.read answer has a presence byte of {present}")
    if at != len(view):
        raise IncompleteResponseError(f"ts.read answer has {len(view) - at} bytes left over")
    return points, cursor


def encode_write_options(opts: TSWriteOptions) -> bytes:
    """ts.write's options: field, tags, and the point's time as an i64 of ns."""
    b = OptionsBuilder()
    if opts.field and opts.field != "value":
        b.add_bytes(OptionTag.TS_FIELD, opts.field.encode("utf-8"))
    tags = _tags(opts.tags)
    if tags:
        b.add_bytes(OptionTag.TS_TAGS, tags)
    t = opts.time_ns
    if t is not None:
        if not isinstance(t, int) or isinstance(t, bool) or not _I64_MIN <= t <= _I64_MAX:
            raise ValidationError(f"time_ns must be an i64 of Unix ns, got {t!r}")
        b.add_bytes(OptionTag.TS_TIMESTAMP_NS, struct.pack("<q", t))
    return b.build()


def decode_write_answer(data: bytes) -> TSWriteResult:
    """ts.write's answer: [series_hash:u64][time_ns:i64][sequence:u64]."""
    if len(data) != 24:
        raise IncompleteResponseError(f"ts.write answer is {len(data)} bytes, not 24")
    series_hash, time_ns, sequence = struct.unpack("<QqQ", data)
    return TSWriteResult(time_ns=time_ns, sequence=sequence, series_hash=series_hash)


def decode_floql_answer(data: bytes) -> list[FloQLSeries]:
    """A FloQL answer: [count:u32]([key_len:u32][key][field_len:u32][field]
    [points:u32]([time_ns:i64][value:f64])*)*."""
    view = memoryview(data)
    at = 0

    def take(n: int) -> memoryview:
        nonlocal at
        if at + n > len(view):
            raise IncompleteResponseError("ts.floql answer is cut short")
        b = view[at : at + n]
        at += n
        return b

    def text() -> str:
        (n,) = struct.unpack("<I", take(4))
        try:
            return bytes(take(n)).decode("utf-8")
        except UnicodeDecodeError as e:
            raise IncompleteResponseError("ts.floql answer has a name that isn't UTF-8") from e

    out: list[FloQLSeries] = []
    (count,) = struct.unpack("<I", take(4))
    for _ in range(count):
        key = text()
        field = text()
        (n,) = struct.unpack("<I", take(4))
        raw = take(16 * n)
        pts = [FloQLPoint(time_ns=t, value=v) for t, v in struct.iter_unpack("<qd", raw)]
        out.append(FloQLSeries(key=key, field=field, points=pts))
    if at != len(view):
        raise IncompleteResponseError(f"ts.floql answer has {len(view) - at} bytes left over")
    return out


def _query_ns(name: str, v: int) -> bytes:
    if not isinstance(v, int) or isinstance(v, bool) or not 0 <= v <= _I64_MAX:
        raise ValidationError(f"{name} must be ns, 0..2^63-1, got {v!r}")
    return struct.pack("<q", v)


def encode_query_options(opts: TSQueryOptions) -> bytes:
    """ts.query's options: field, tags, from_ns, to_ns and window_ns (each
    an i64 of ns), and the aggregation's name."""
    b = OptionsBuilder()
    if opts.field and opts.field != "value":
        b.add_bytes(OptionTag.TS_FIELD, opts.field.encode("utf-8"))
    tags = _tags(opts.tags)
    if tags:
        b.add_bytes(OptionTag.TS_TAGS, tags)
    if opts.from_ns is not None:
        b.add_bytes(OptionTag.TS_FROM_NS, _query_ns("from_ns", opts.from_ns))
    if opts.to_ns is not None:
        b.add_bytes(OptionTag.TS_TO_NS, _query_ns("to_ns", opts.to_ns))
    if opts.window is not None:
        w = opts.window
        ns = ns_from_timedelta(w) if isinstance(w, timedelta) else w
        if isinstance(ns, bool) or not isinstance(ns, int) or ns <= 0:
            raise ValidationError(f"a window is more than 0 ns, got {w!r}")
        b.add_bytes(OptionTag.TS_WINDOW_NS, _query_ns("window", ns))
    b.add_bytes(OptionTag.TS_AGGREGATION, TSAggregation(opts.aggregation).value.encode())
    return b.build()


def decode_query_answer(data: bytes) -> list[TSBucket]:
    """ts.query's answer: [series:u32]([key_len:u32][key][buckets:u32]
    ([start_ns:i64][value:f64])*)*. The server answers one series, or none
    when no window holds a point."""
    view = memoryview(data)
    at = 0

    def take(n: int) -> memoryview:
        nonlocal at
        if at + n > len(view):
            raise IncompleteResponseError("ts.query answer is cut short")
        b = view[at : at + n]
        at += n
        return b

    out: list[TSBucket] = []
    (series,) = struct.unpack("<I", take(4))
    for _ in range(series):
        (n,) = struct.unpack("<I", take(4))
        take(n)  # the measurement, as asked
        (count,) = struct.unpack("<I", take(4))
        raw = take(16 * count)
        out.extend(TSBucket(start_ns=t, value=v) for t, v in struct.iter_unpack("<qd", raw))
    if at != len(view):
        raise IncompleteResponseError(f"ts.query answer has {len(view) - at} bytes left over")
    return out


def decode_info_answer(data: bytes) -> TSInfo:
    """ts.info's answer: series u32, field_count u32, fields_capped bool,
    fields [count:u16]([len:u16][name])*, points u64, then oldest_ns and
    newest_ns, each [present:u8][i64]."""
    view = memoryview(data)
    at = 0

    def take(n: int) -> memoryview:
        nonlocal at
        if at + n > len(view):
            raise IncompleteResponseError("ts.info answer is cut short")
        b = view[at : at + n]
        at += n
        return b

    def flag() -> bool:
        b = take(1)[0]
        if b > 1:
            raise IncompleteResponseError(f"ts.info answer has a bool of {b}")
        return b == 1

    def opt_i64() -> int | None:
        return int(struct.unpack("<q", take(8))[0]) if flag() else None

    series, field_count = struct.unpack("<II", take(8))
    capped = flag()
    fields: list[str] = []
    (count,) = struct.unpack("<H", take(2))
    for _ in range(count):
        (n,) = struct.unpack("<H", take(2))
        try:
            fields.append(bytes(take(n)).decode("utf-8"))
        except UnicodeDecodeError as e:
            raise IncompleteResponseError("ts.info answer has a field that isn't UTF-8") from e
    (points,) = struct.unpack("<Q", take(8))
    oldest, newest = opt_i64(), opt_i64()
    if at != len(view):
        raise IncompleteResponseError(f"ts.info answer has {len(view) - at} bytes left over")
    return TSInfo(series, field_count, capped, fields, points, oldest, newest)


class TSOperations:
    """Time-series operations for the Flo client."""

    def __init__(self, client: FloClient) -> None:
        self._client = client

    async def write(
        self,
        measurement: str,
        value: float,
        options: TSWriteOptions | None = None,
    ) -> TSWriteResult:
        """Write one point to a measurement's field, at ``time_ns`` (Unix
        nanoseconds) or, without one, at the server's time.

        The server refuses a time before 1970 (BadRequestError); a value
        that isn't finite is refused before it is sent.

        Example:
            await client.ts.write("cpu", 0.42, TSWriteOptions(
                tags={"host": "a"}, time_ns=time.time_ns()))
        """
        opts = options or TSWriteOptions()
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise ValidationError(f"a point's value is a number, got {value!r}")
        if not math.isfinite(value):
            raise ValidationError(f"a point's value must be finite, got {value!r}")
        response = await self._client._send_and_check(
            OpCode.TS_WRITE,
            self._client.get_namespace(opts.namespace),
            measurement.encode("utf-8"),
            struct.pack("<d", float(value)),
            encode_write_options(opts),
        )
        return decode_write_answer(response.data)

    async def read(
        self,
        measurement: str,
        options: TSReadOptions | None = None,
    ) -> TSReadResult:
        """One page of a measurement field's points, oldest or newest first.

        The page holds every matching series' points, merged by time and
        then by write order. Ask again with ``cursor`` set to the page's
        cursor until it is None: a short page doesn't mean the last one. A
        page continues strictly after the cursor (before it, newest first),
        so a point there for the whole read is read exactly once. The
        cursor is opaque.

        A refused filter or cursor raises BadRequestError. A read that
        matches no series of the field (a measurement never written, or a
        tag filter no series has) raises NotFoundError; a series with no
        points in the range answers an empty page. StaleNotFoundError (a
        NotFoundError) is a not found from a node that may be behind: the
        series may exist, so read again rather than treat it as gone.

        Example:
            opts = TSReadOptions(tags="host=a")
            while True:
                page = await client.ts.read("cpu", opts)
                for p in page.points:
                    print(p.time_ns, p.value)
                if page.cursor is None:
                    break
                opts.cursor = page.cursor
        """
        opts = options or TSReadOptions()
        response = await self._client._send_and_check(
            OpCode.TS_READ,
            self._client.get_namespace(opts.namespace),
            measurement.encode("utf-8"),
            encode_read_body(opts),
            allow_not_found=True,
        )
        if response.is_not_found():
            raise not_found_error(response.status, response.data, response.stale)
        points, cursor = decode_read_answer(response.data)
        return TSReadResult(points=points, cursor=cursor, stale=response.stale)

    async def iter_points(
        self,
        measurement: str,
        options: TSReadOptions | None = None,
    ) -> AsyncIterator[TSPoint]:
        """Every matching point, page by page, until a page answers no
        cursor. ``options.cursor`` starts it part way; ``options.limit`` is
        the page size. A refusal on any page, not-found included, is raised
        from the iterator rather than ending it: an end means no more
        points. A reader that may start before its series exists should
        catch NotFoundError and start again.

        Example:
            async for p in client.ts.iter_points("cpu", TSReadOptions(
                    order=TSOrder.NEWEST_FIRST)):
                print(p.time_ns, p.value)
        """
        base = options or TSReadOptions()
        cursor = base.cursor
        while True:
            page = await self.read(
                measurement,
                TSReadOptions(
                    namespace=base.namespace,
                    field=base.field,
                    tags=base.tags,
                    from_ns=base.from_ns,
                    to_ns=base.to_ns,
                    limit=base.limit,
                    order=base.order,
                    cursor=cursor,
                ),
            )
            for p in page.points:
                yield p
            if page.cursor is None:
                return
            cursor = page.cursor

    async def query(
        self,
        measurement: str,
        options: TSQueryOptions | None = None,
    ) -> list[TSBucket]:
        """One value per window of a measurement field's points: each
        window's start in Unix ns, oldest first, for the windows that hold a
        point. Windows start at multiples of the width from the epoch.

        A range of more than 1024 windows of the width is refused
        (BadRequestError, with the server's text): widen the window or
        narrow the range.

        Example:
            buckets = await client.ts.query("cpu", TSQueryOptions(
                from_ns=start, to_ns=end, window=timedelta(minutes=5),
                aggregation=TSAggregation.MAX))
        """
        opts = options or TSQueryOptions()
        response = await self._client._send_and_check(
            OpCode.TS_QUERY,
            self._client.get_namespace(opts.namespace),
            measurement.encode("utf-8"),
            b"",
            encode_query_options(opts),
        )
        return decode_query_answer(response.data)

    async def info(self, measurement: str, namespace: str | None = None) -> TSInfo:
        """A measurement's series, fields, points, and its oldest and newest
        points' times in Unix ns. A measurement with no series raises
        NotFoundError (StaleNotFoundError from a node that may be behind)."""
        response = await self._client._send_and_check(
            OpCode.TS_INFO,
            self._client.get_namespace(namespace),
            measurement.encode("utf-8"),
            b"",
            allow_not_found=True,
        )
        if response.is_not_found():
            raise not_found_error(response.status, response.data, response.stale)
        return decode_info_answer(response.data)

    async def floql(self, query: str, namespace: str | None = None) -> list[FloQLSeries]:
        """Run a FloQL query. Its answer's times are Unix nanoseconds. In the
        query text, a bare number in a range is epoch milliseconds, and
        durations (``5m``, ``30s``) are as FloQL writes them.

        An answer of more than 4096 raw points is refused (BadRequestError,
        with the server's text): aggregate them, narrow the query, or page
        through them with :meth:`read`.
        """
        if not query:
            raise ValidationError("a FloQL query is required")
        response = await self._client._send_and_check(
            OpCode.TS_FLOQL,
            self._client.get_namespace(namespace),
            b"",
            query.encode("utf-8"),
        )
        return decode_floql_answer(response.data)
