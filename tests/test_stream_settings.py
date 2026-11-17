"""Stream create and alter send the settings the server reads, and info
answers them."""

import struct

import pytest

from flo import StreamAlterOptions, StreamCreateOptions
from flo.exceptions import ValidationError
from flo.types import OpCode, OptionTag
from flo.wire import OptionsIterator, parse_stream_info_response

from .test_stream_trim import Call, FloClient, sent  # noqa: F401


def _u64(options: bytes, tag: OptionTag) -> int | None:
    opt = OptionsIterator(options).find(tag)
    return None if opt is None else int(struct.unpack("<Q", opt.data)[0])


async def test_create_sends_partitions_in_the_value_and_retention_as_options(
    sent: tuple[FloClient, list[Call]],  # noqa: F811
) -> None:
    client, calls = sent
    await client.stream.create(
        "s", StreamCreateOptions(partitions=4, retention_age_s=60, retention_count=1000)
    )
    await client.stream.create("s")
    (op, _, key, value, options), (_, _, _, bare_value, bare_options) = calls
    assert (op, key, value) == (OpCode.STREAM_CREATE, b"s", struct.pack("<I", 4))
    assert (_u64(options, OptionTag.RETENTION_AGE), _u64(options, OptionTag.RETENTION_COUNT)) == (
        60,
        1000,
    )
    assert (bare_value, bare_options) == (b"", b"")
    with pytest.raises(ValidationError):
        await client.stream.create("s", StreamCreateOptions(partitions=0))


async def test_alter_sends_only_the_retention_it_changes(
    sent: tuple[FloClient, list[Call]],  # noqa: F811
) -> None:
    client, calls = sent
    await client.stream.alter("s", StreamAlterOptions(retention_count=5))
    op, _, _, value, options = calls[0]
    assert (op, value) == (OpCode.STREAM_ALTER, b"")
    assert _u64(options, OptionTag.RETENTION_COUNT) == 5
    assert _u64(options, OptionTag.RETENTION_AGE) is None


def test_info_answers_the_streams_retention() -> None:
    data = struct.pack("<QQQQQQIQQQ", 1, 2, 3, 4, 5, 6, 1, 3600, 100, 0)
    info = parse_stream_info_response(data)
    assert (info.partition_count, info.retention_age_s, info.retention_count) == (1, 3600, 100)
