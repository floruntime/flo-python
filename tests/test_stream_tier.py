"""A stream record's tier byte is read as the server sends it."""

import struct

import pytest

from flo.exceptions import IncompleteResponseError
from flo.types import StorageTier
from flo.wire import parse_stream_read_response


def _one_record(tier: int) -> bytes:
    # [count=1][seq][ts][tier][partition][key_present=0][payload_len=0][header_count=0]
    return struct.pack("<IQqBIBII", 1, 1, 1000, tier, 0, 0, 0, 0)


@pytest.mark.parametrize(("byte", "tier"), [(0, StorageTier.HOT), (1, StorageTier.WARM)])
def test_tier_bytes_read_as_the_server_sends_them(byte: int, tier: StorageTier) -> None:
    assert parse_stream_read_response(_one_record(byte)).records[0].tier == tier


def test_an_unknown_tier_refuses_the_answer() -> None:
    with pytest.raises(IncompleteResponseError):
        parse_stream_read_response(_one_record(2))
