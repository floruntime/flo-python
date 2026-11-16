"""A read of a stream or series that doesn't exist is not found, against a
live server; one that exists with nothing to read is ok and empty, and a
blocking read of a stream not created yet waits for its first append."""

import asyncio
import secrets

import pytest

from flo import FloClient, StreamDeleteOptions, StreamReadOptions, TSReadOptions, TSWriteOptions
from flo.exceptions import NotFoundError


def _name(kind: str) -> str:
    return f"py-nf-{kind}-{secrets.token_hex(4)}"


async def test_live_a_stream_never_written_is_not_found(client: FloClient) -> None:
    with pytest.raises(NotFoundError) as e:
        await client.stream.read(_name("never"))
    assert str(e.value) == "stream not found"


async def test_live_a_stream_read_past_its_end_is_empty(client: FloClient) -> None:
    stream = _name("past")
    await client.stream.append(stream, b"r")
    assert len((await client.stream.read(stream)).records) == 1
    assert (await client.stream.read(stream, StreamReadOptions(tail=True))).records == []
    await client.stream.delete(stream, StreamDeleteOptions(force=True))
    # Deleted: not found again.
    with pytest.raises(NotFoundError):
        await client.stream.read(stream)


async def test_live_a_blocking_read_of_a_missing_stream_waits_for_its_first_append(
    client: FloClient,
) -> None:
    stream = _name("block")
    reader = FloClient(client._endpoint, namespace=client.namespace)
    await reader.connect()
    try:
        read = asyncio.create_task(reader.stream.read(stream, StreamReadOptions(block_ms=5000)))
        await asyncio.sleep(0.2)
        assert not read.done()
        await client.stream.append(stream, b"first")
        got = await asyncio.wait_for(read, 10)
        assert [r.payload for r in got.records] == [b"first"]
    finally:
        await reader.close()
    await client.stream.delete(stream, StreamDeleteOptions(force=True))


async def test_live_a_short_blocking_read_of_a_missing_stream_is_not_not_found(
    client: FloClient,
) -> None:
    got = await client.stream.read(_name("short"), StreamReadOptions(block_ms=100))
    assert got.records == []


async def test_live_a_measurement_never_written_is_not_found(client: FloClient) -> None:
    measurement = _name("ts")
    with pytest.raises(NotFoundError) as e:
        await client.ts.read(measurement)
    assert str(e.value) == f"not found: '{measurement}' has no series of field 'value'"
    # The page walker gives the error, not an empty walk.
    with pytest.raises(NotFoundError):
        async for _ in client.ts.iter_points(measurement):
            pass


async def test_live_a_range_with_no_points_on_a_real_series_is_empty(client: FloClient) -> None:
    measurement = _name("ts-real")
    await client.ts.write(measurement, 1.0, TSWriteOptions(tags={"h": "a"}, time_ns=10**18))
    page = await client.ts.read(measurement, TSReadOptions(to_ns=10**18 - 1))
    assert (page.points, page.cursor) == ([], None)
    # A tag filter no series matches is not found.
    with pytest.raises(NotFoundError):
        await client.ts.read(measurement, TSReadOptions(tags="h=b"))
