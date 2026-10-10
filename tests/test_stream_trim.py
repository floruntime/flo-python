"""Stream trim sends the bounds the server reads and returns the count."""

import struct
from typing import Any

import pytest

from flo import FloClient, StreamTrimOptions
from flo.types import OpCode, OptionTag, StatusCode, StreamID
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
        return RawResponse(status=StatusCode.OK, data=struct.pack("<QQ", 6, 42), request_id=0)

    monkeypatch.setattr(client, "_send_and_check", fake_send)
    return client, calls


async def test_max_len_dry_run(sent: tuple[FloClient, list[Call]]) -> None:
    client, calls = sent
    result = await client.stream.trim("s", StreamTrimOptions(max_len=4, dry_run=True))
    assert (result.removed, result.first_seq) == (6, 42)
    opts = OptionsIterator(calls[0][4])
    limit = opts.find(OptionTag.LIMIT)
    assert limit is not None and limit.data == struct.pack("<Q", 4)
    assert OptionsIterator(calls[0][4]).find(OptionTag.DRY_RUN) is not None


async def test_max_age_and_before(sent: tuple[FloClient, list[Call]]) -> None:
    client, calls = sent
    await client.stream.trim("s", StreamTrimOptions(max_age_seconds=60))
    age = OptionsIterator(calls[0][4]).find(OptionTag.MAX_AGE_SECONDS)
    assert age is not None and age.data == struct.pack("<Q", 60)

    await client.stream.trim("s", StreamTrimOptions(before=StreamID(7, 2)))
    start = OptionsIterator(calls[1][4]).find(OptionTag.STREAM_START)
    assert start is not None and start.data == StreamID(7, 2).to_bytes()


def test_retention_tags_are_gone() -> None:
    for name in ("RETENTION_COUNT", "RETENTION_AGE", "RETENTION_BYTES", "MAX_BYTES"):
        assert not hasattr(OptionTag, name)
