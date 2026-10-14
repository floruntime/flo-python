"""Processing status and list answers decode per the server's layouts."""

import struct

from flo.processing import _parse_processing_list, _parse_processing_status
from flo.types import ProcessingListEntry, ProcessingStatusResult


def _s(v: str) -> bytes:
    return struct.pack("<H", len(v)) + v.encode()


def test_status() -> None:
    data = _s("job-1") + _s("pipe") + b"\x01" + struct.pack("<IQq", 50, 1234, 99)
    assert _parse_processing_status(data) == ProcessingStatusResult(
        job_id="job-1",
        name="pipe",
        status="stopped",
        batch_size=50,
        records_processed=1234,
        created_at=99,
    )


def test_list() -> None:
    data = struct.pack("<I", 2)
    data += _s("a") + _s("j1") + _s("RUNNING") + struct.pack("<q", 7)
    data += _s("b") + _s("j2") + _s("failed") + struct.pack("<q", 8)
    data += b"\x00" + struct.pack("<H", 0)
    assert _parse_processing_list(data) == [
        ProcessingListEntry(name="a", job_id="j1", status="RUNNING", created_at=7),
        ProcessingListEntry(name="b", job_id="j2", status="failed", created_at=8),
    ]
