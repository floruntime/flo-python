"""worker.info is a table op: an empty request keyed by the worker id, and an
answer of the worker's row (as worker.list answers it), its metadata, then its
processes."""

import struct
from typing import Any

import pytest

from flo import FloClient, ProcessKind, WorkerStatus, WorkerType
from flo.exceptions import IncompleteResponseError
from flo.kv_body import decode_worker_info_answer, is_table_op
from flo.types import OpCode, StatusCode, WorkerProcess
from flo.wire import RawResponse

from .test_worker_list import _row


def _process(name: bytes, kind: int, runs: int, fails: int, last_ms: int) -> bytes:
    return (
        struct.pack("<H", len(name))
        + name
        + bytes([kind])
        + struct.pack("<QQq", runs, fails, last_ms)
    )


ROW = _row(b"w-a", status=2)
FULL = (
    ROW
    + b"\x01"
    + struct.pack("<H", 8)
    + b'{"v": 1}'
    + struct.pack("<H", 2)
    + _process(b"charge", 0, 5, 1, 3000)
    + _process(b"orders", 1, 7, 0, -1)
)
BARE = _row(b"w-b", machine_id=None) + b"\x00" + struct.pack("<H", 0)


def test_worker_info_is_a_table_op() -> None:
    assert is_table_op(OpCode.WORKER_INFO)


def test_an_answer_with_metadata_and_a_process_of_each_kind() -> None:
    info = decode_worker_info_answer(FULL)
    w = info.worker
    assert (w.worker_id, w.worker_type, w.status) == (
        "w-a",
        WorkerType.STREAM,
        WorkerStatus.DRAINING,
    )
    assert (w.current_load, w.max_concurrency, w.tasks_completed, w.tasks_failed) == (2, 8, 40, 3)
    assert (w.registered_at_ms, w.last_heartbeat_ms, w.machine_id) == (1000, 2000, "m-1")
    assert info.metadata == '{"v": 1}'
    assert info.processes == [
        WorkerProcess(
            name="charge", kind=ProcessKind.ACTION, run_count=5, fail_count=1, last_run_at_ms=3000
        ),
        WorkerProcess(
            name="orders",
            kind=ProcessKind.STREAM_CONSUMER,
            run_count=7,
            fail_count=0,
            last_run_at_ms=-1,
        ),
    ]


def test_an_answer_with_no_metadata_and_no_processes() -> None:
    info = decode_worker_info_answer(BARE)
    assert (info.worker.worker_id, info.worker.machine_id) == ("w-b", None)
    assert info.metadata is None
    assert info.processes == []


def test_an_unknown_process_kind_is_refused() -> None:
    at = len(FULL) - len(_process(b"orders", 1, 7, 0, -1)) + 2 + len(b"orders")
    bad = bytearray(FULL)
    assert bad[at] == 1
    bad[at] = 2
    with pytest.raises(IncompleteResponseError, match="ProcessKind"):
        decode_worker_info_answer(bytes(bad))


@pytest.mark.parametrize("n", range(len(FULL)))
def test_an_answer_cut_short_is_refused(n: int) -> None:
    with pytest.raises(IncompleteResponseError):
        decode_worker_info_answer(FULL[:n])


def test_an_answer_with_bytes_left_over_is_refused() -> None:
    with pytest.raises(IncompleteResponseError):
        decode_worker_info_answer(BARE + b"\x00")


async def test_the_client_keys_the_request_by_worker_id(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FloClient("localhost:1")
    calls: list[tuple[OpCode, str, bytes, bytes, bytes]] = []

    async def fake_send(
        op_code: OpCode, namespace: str, key: bytes, value: bytes, options: bytes = b"", **_: Any
    ) -> RawResponse:
        calls.append((op_code, namespace, key, value, options))
        return RawResponse(status=StatusCode.OK, data=FULL, request_id=0)

    monkeypatch.setattr(client, "_send_and_check", fake_send)
    info = await client.worker.info("w-a", namespace="ns")
    assert calls == [(OpCode.WORKER_INFO, "ns", b"w-a", b"", b"")]
    assert [p.name for p in info.processes] == ["charge", "orders"]
