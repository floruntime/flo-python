"""Processing status and list answers decode per the server's layouts."""

import struct

import pytest

from flo.exceptions import IncompleteResponseError
from flo.processing import _parse_processing_list, _parse_processing_status
from flo.types import ProcessingListEntry, ProcessingRuntime, ProcessingStatusResult


def _s(v: str) -> bytes:
    return struct.pack("<H", len(v)) + v.encode()


YAML = b"name: pipe\n"
# This node's runtime of the job, then its definition.
RUNTIME = b"\x01" + struct.pack("<QQqQQQ", 10, 9, 500, 12, 40, 3000)
TAIL = RUNTIME + struct.pack("<I", len(YAML)) + YAML


def _status(status: int, reason: bytes, tail: bytes = TAIL) -> bytes:
    head = _s("job-1") + _s("pipe") + bytes([status]) + struct.pack("<IQq", 50, 1234, 99)
    return head + reason + tail


def test_status() -> None:
    assert _parse_processing_status(_status(0, _s(""))) == ProcessingStatusResult(
        job_id="job-1",
        name="pipe",
        status="running",
        batch_size=50,
        records_processed=1234,
        created_at=99,
        failure_reason=None,
        runtime=ProcessingRuntime(
            running=True,
            records_in=10,
            records_out=9,
            watermark_ms=500,
            tick_avg_us=12,
            tick_max_us=40,
            uptime_ms=3000,
        ),
        yaml=YAML,
    )


@pytest.mark.parametrize("cut", range(1, len(TAIL) + 1))
def test_status_refuses_a_runtime_or_definition_cut_short(cut: int) -> None:
    with pytest.raises(IncompleteResponseError):
        _parse_processing_status(_status(0, _s(""), TAIL[:-cut]))


def test_status_refuses_a_running_flag_that_isnt_one() -> None:
    with pytest.raises(IncompleteResponseError):
        _parse_processing_status(_status(0, _s(""), b"\x02" + TAIL[1:]))


def test_status_failed_carries_reason() -> None:
    result = _parse_processing_status(_status(3, _s("sink kv: key too large")))
    assert result.status == "failed"
    assert result.failure_reason == "sink kv: key too large"


def test_status_completed() -> None:
    assert _parse_processing_status(_status(4, _s(""))).status == "completed"


def test_status_refuses_reason_past_end() -> None:
    # Its own bounds check refuses it, not the trailing-bytes check.
    with pytest.raises(IncompleteResponseError, match="truncated"):
        _parse_processing_status(_status(3, struct.pack("<H", 10) + b"boo", b""))


def test_status_refuses_a_field_that_isnt_utf8() -> None:
    with pytest.raises(IncompleteResponseError, match="UTF-8"):
        _parse_processing_status(_status(3, struct.pack("<H", 2) + b"\xff\xfe"))


def test_status_refuses_trailing_bytes() -> None:
    with pytest.raises(IncompleteResponseError):
        _parse_processing_status(_status(0, _s("") + b"\x00"))


def test_status_refuses_unknown_status() -> None:
    with pytest.raises(IncompleteResponseError):
        _parse_processing_status(_status(5, _s("")))


def test_list() -> None:
    data = struct.pack("<I", 2)
    data += _s("a") + _s("j1") + _s("RUNNING") + struct.pack("<qIQ", 7, 100, 5)
    data += _s("b") + _s("j2") + _s("failed") + struct.pack("<qIQ", 8, 1, 0)
    data += b"\x00" + struct.pack("<H", 0)
    assert _parse_processing_list(data) == (
        [
            ProcessingListEntry(
                name="a",
                job_id="j1",
                status="RUNNING",
                created_at=7,
                batch_size=100,
                records_processed=5,
            ),
            ProcessingListEntry(
                name="b",
                job_id="j2",
                status="failed",
                created_at=8,
                batch_size=1,
                records_processed=0,
            ),
        ],
        None,
    )


def _list(cursor: bytes | None) -> bytes:
    entry = _s("a") + _s("j1") + _s("FAILED: boom") + struct.pack("<qIQ", 7, 100, 5)
    data = struct.pack("<I", 1) + entry
    cursor = cursor or b""
    return data + bytes([1 if cursor else 0]) + struct.pack("<H", len(cursor)) + cursor


def test_list_carries_the_next_pages_cursor() -> None:
    entries, cursor = _parse_processing_list(_list(b"\x01\x80\x01\x00\x00\x00"))
    assert [e.status for e in entries] == ["FAILED: boom"]
    assert cursor == b"\x01\x80\x01\x00\x00\x00"


@pytest.mark.parametrize(
    "data",
    [
        b"",
        _list(None)[:-1],  # cut short
        _list(None) + b"\x00",  # a byte left over
        _list(None)[:-3] + b"\x01" + struct.pack("<H", 0),  # more, but no cursor
        _list(None)[:-3] + b"\x02" + struct.pack("<H", 0),  # has_more not 0 or 1
        _list(None)[:-3] + b"\x00" + struct.pack("<H", 1) + b"c",  # last page, yet a cursor
        struct.pack("<I", 1025),  # more than a page holds
    ],
    ids=[
        "empty",
        "short",
        "trailing",
        "more-without-cursor",
        "bad-has-more",
        "cursor-on-last-page",
        "too-many",
    ],
)
def test_list_refuses_a_malformed_answer(data: bytes) -> None:
    with pytest.raises(IncompleteResponseError):
        _parse_processing_list(data)
