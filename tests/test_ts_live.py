"""Time-series writes and paged reads against a live server: times are ns,
and a read pages through every point once, in order, either way."""

import secrets
import struct
import time
from dataclasses import dataclass

import pytest
import pytest_asyncio

from flo import (
    FloClient,
    TSAggregation,
    TSBucket,
    TSOrder,
    TSQueryOptions,
    TSReadOptions,
    TSWriteOptions,
)
from flo.exceptions import BadRequestError, NotFoundError
from flo.types import OpCode, OptionTag
from flo.wire import OptionsBuilder

ODD_NS = 1_700_000_000_123_456_789
POINTS = 5000
SERIES = 3


@dataclass
class Written:
    measurement: str
    # (time_ns, value) in the order written; each value is its index.
    points: list[tuple[int, float]]
    hosts: list[int]


@pytest_asyncio.fixture(scope="module")
async def written(client: FloClient) -> Written:
    """5000 points over 3 series, written one at a time. Each pair of
    neighbours shares a time, in different series, so the read's tie on time
    falls to write order. Within the last hour, for FloQL."""
    measurement = f"py-ts-{secrets.token_hex(4)}"
    base = time.time_ns() - 600 * 10**9
    points: list[tuple[int, float]] = []
    hosts: list[int] = []
    for i in range(POINTS):
        t = base + (i // 2) * 1_000_003
        host = i % SERIES
        r = await client.ts.write(
            measurement, float(i), TSWriteOptions(tags={"host": str(host)}, time_ns=t)
        )
        assert r.time_ns == t
        points.append((t, float(i)))
        hosts.append(host)
    return Written(measurement, points, hosts)


def _oldest_first(w: Written) -> list[tuple[int, float]]:
    # Python's sort is stable: equal times keep write order.
    return sorted(w.points, key=lambda p: p[0])


@pytest.mark.parametrize("limit", [333, 4096])
async def test_live_pages_every_point_once_oldest_first(
    client: FloClient, written: Written, limit: int
) -> None:
    got = [
        (p.time_ns, p.value)
        async for p in client.ts.iter_points(written.measurement, TSReadOptions(limit=limit))
    ]
    assert got == _oldest_first(written)


@pytest.mark.parametrize("limit", [333, 4096])
async def test_live_pages_every_point_once_newest_first(
    client: FloClient, written: Written, limit: int
) -> None:
    opts = TSReadOptions(limit=limit, order=TSOrder.NEWEST_FIRST)
    got = [(p.time_ns, p.value) async for p in client.ts.iter_points(written.measurement, opts)]
    assert got == list(reversed(_oldest_first(written)))


async def test_live_pages_by_hand_until_the_cursor_is_none(
    client: FloClient, written: Written
) -> None:
    opts = TSReadOptions(limit=1000)
    pages = 0
    total = 0
    while True:
        page = await client.ts.read(written.measurement, opts)
        pages += 1
        total += len(page.points)
        if page.cursor is None:
            break
        assert len(page.cursor) == 16
        opts.cursor = page.cursor
    assert total == POINTS
    # 5000 is five full pages; a sixth, empty one may close it.
    assert pages in (5, 6)


async def test_live_newest_first_limit_one_is_the_last_point_written(
    client: FloClient, written: Written
) -> None:
    page = await client.ts.read(
        written.measurement, TSReadOptions(limit=1, order=TSOrder.NEWEST_FIRST)
    )
    assert [(p.time_ns, p.value) for p in page.points] == [written.points[-1]]
    assert page.cursor is not None


async def test_live_a_tag_filter_takes_only_its_series(client: FloClient, written: Written) -> None:
    want = [p for p, h in zip(written.points, written.hosts, strict=True) if h == 1]
    got = [
        (p.time_ns, p.value)
        async for p in client.ts.iter_points(
            written.measurement, TSReadOptions(tags="host=1", limit=500)
        )
    ]
    assert got == sorted(want, key=lambda p: p[0])


async def test_live_a_range_is_inclusive(client: FloClient, written: Written) -> None:
    lo, hi = written.points[10][0], written.points[19][0]
    page = await client.ts.read(written.measurement, TSReadOptions(from_ns=lo, to_ns=hi))
    assert [(p.time_ns, p.value) for p in page.points] == [
        p for p in _oldest_first(written) if lo <= p[0] <= hi
    ]
    assert page.cursor is None


async def test_live_a_raw_floql_answer_past_4096_points_is_refused(
    client: FloClient, written: Written
) -> None:
    with pytest.raises(BadRequestError) as e:
        await client.ts.floql(f"{written.measurement}[1h]")
    assert str(e.value) == (
        "bad request: the answer holds more than 4096 points; aggregate them, "
        "narrow the query, or page through them with ts read"
    )


async def test_live_floql_aggregates_within_its_cap(client: FloClient, written: Written) -> None:
    [series] = await client.ts.floql(f"{written.measurement}[1h] | count()")
    assert sum(p.value for p in series.points) == POINTS


async def test_live_a_time_that_isnt_a_whole_ms_reads_back_exactly(client: FloClient) -> None:
    measurement = f"py-ts-ns-{secrets.token_hex(4)}"
    r = await client.ts.write(measurement, 3.25, TSWriteOptions(time_ns=ODD_NS))
    assert r.time_ns == ODD_NS
    page = await client.ts.read(measurement, TSReadOptions(from_ns=ODD_NS, to_ns=ODD_NS))
    assert [(p.time_ns, p.value) for p in page.points] == [(ODD_NS, 3.25)]
    page = await client.ts.read(measurement, TSReadOptions(to_ns=ODD_NS - 1))
    assert page.points == []


async def test_live_no_time_takes_the_servers_in_ns(client: FloClient) -> None:
    before = time.time_ns()
    r = await client.ts.write(f"py-ts-now-{secrets.token_hex(4)}", 1.0)
    # Same host: the server's clock and this one agree to well within a minute.
    assert abs(r.time_ns - before) < 60 * 10**9


async def test_live_a_time_before_1970_is_refused(client: FloClient) -> None:
    with pytest.raises(BadRequestError) as e:
        await client.ts.write("py-ts-neg", 1.0, TSWriteOptions(time_ns=-1))
    assert str(e.value) == "ts write: a point's time must not be before 1970"


async def test_live_a_time_option_that_isnt_8_bytes_is_refused(client: FloClient) -> None:
    # The SDK's encoder never sends one: built by hand.
    options = OptionsBuilder().add_bytes(OptionTag.TS_TIMESTAMP_NS, b"\x01\x02\x03\x04").build()
    with pytest.raises(BadRequestError) as e:
        await client._send_and_check(
            OpCode.TS_WRITE, client.namespace, b"py-ts-short", struct.pack("<d", 1.0), options
        )
    assert str(e.value) == "ts write: the timestamp option must be an 8-byte i64 of nanoseconds"


async def test_live_a_cursor_that_isnt_16_bytes_is_refused(client: FloClient) -> None:
    measurement = f"py-ts-cur-{secrets.token_hex(4)}"
    await client.ts.write(measurement, 1.0, TSWriteOptions(time_ns=ODD_NS))
    with pytest.raises(BadRequestError) as e:
        await client.ts.read(measurement, TSReadOptions(cursor=b"short"))
    assert str(e.value) == "bad request: a cursor is the 16 bytes a page answered"


async def test_live_a_bad_tag_filter_is_refused(client: FloClient) -> None:
    measurement = f"py-ts-tag-{secrets.token_hex(4)}"
    await client.ts.write(measurement, 1.0, TSWriteOptions(time_ns=ODD_NS))
    with pytest.raises(BadRequestError, match="is not key=value"):
        await client.ts.read(measurement, TSReadOptions(tags="host"))


async def test_live_an_empty_measurement_is_refused(client: FloClient) -> None:
    with pytest.raises(BadRequestError) as e:
        await client.ts.read("")
    assert str(e.value) == "bad request: measurement name is required"


async def _small(client: FloClient, kind: str) -> tuple[str, list[tuple[int, float]]]:
    """A measurement of 20 points at known ns times, none a whole ms, within
    the last hour, in time order."""
    measurement = f"py-ts-{kind}-{secrets.token_hex(4)}"
    base = time.time_ns() - 1800 * 10**9 + 123_457
    points = [(base + i * 700_000_001, float(i)) for i in range(20)]
    for t, v in points:
        await client.ts.write(measurement, v, TSWriteOptions(time_ns=t))
    return measurement, points


async def test_live_a_query_buckets_by_an_ns_window(client: FloClient) -> None:
    measurement, points = await _small(client, "q")
    window = 2_000_000_003  # not a whole ms
    lo, hi = points[2][0], points[17][0]
    got = await client.ts.query(
        measurement,
        TSQueryOptions(from_ns=lo, to_ns=hi, window=window, aggregation=TSAggregation.SUM),
    )
    want: dict[int, float] = {}
    for t, v in points:
        if lo <= t <= hi:
            start = t // window * window
            want[start] = want.get(start, 0.0) + v
    assert got == [TSBucket(start, v) for start, v in sorted(want.items())]


async def test_live_a_query_of_more_than_1024_windows_is_refused(
    client: FloClient, written: Written
) -> None:
    # 2500 distinct times, a 1 ns window: a window for each.
    with pytest.raises(BadRequestError) as e:
        await client.ts.query(written.measurement, TSQueryOptions(window=1))
    assert str(e.value) == (
        "bad request: the range holds more than 1024 windows of this width; "
        "widen the window (it is in nanoseconds) or narrow the range"
    )


async def test_live_floql_answers_the_ns_times_written(client: FloClient) -> None:
    measurement, points = await _small(client, "fq")
    [series] = await client.ts.floql(f"{measurement}[1h]")
    assert [(p.time_ns, p.value) for p in series.points] == points


async def test_live_info_oldest_and_newest_are_the_first_and_last_write(
    client: FloClient,
) -> None:
    measurement, points = await _small(client, "info")
    info = await client.ts.info(measurement)
    assert (info.oldest_ns, info.newest_ns) == (points[0][0], points[-1][0])
    assert (info.series, info.points, info.fields) == (1, len(points), ["value"])


async def test_live_info_of_a_measurement_never_written_is_not_found(client: FloClient) -> None:
    with pytest.raises(NotFoundError):
        await client.ts.info(f"py-ts-none-{secrets.token_hex(4)}")
