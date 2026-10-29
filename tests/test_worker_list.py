"""worker.list is a table op: its request is [limit][cursor] coded by its
schema, and its answer is a list of worker rows then the next page's cursor."""

import struct
from typing import Any

import pytest

from flo import FloClient, WorkerListOptions, WorkerStatus, WorkerType
from flo.exceptions import IncompleteResponseError, ValidationError
from flo.kv_body import decode_worker_list_answer, encode_worker_list_body, is_table_op
from flo.types import OpCode, StatusCode
from flo.wire import RawResponse, serialize_request


def _row(worker_id: bytes, status: int = 0, machine_id: bytes | None = b"m-1") -> bytes:
    return (
        struct.pack("<H", len(worker_id))
        + worker_id
        + bytes([1, status])  # stream worker
        + struct.pack("<II", 2, 8)  # current_load, max_concurrency
        + struct.pack("<QQ", 40, 3)  # tasks_completed, tasks_failed
        + struct.pack("<qq", 1000, 2000)  # registered_at_ms, last_heartbeat_ms
        + (b"\x00" if machine_id is None else b"\x01" + bytes([len(machine_id)]) + machine_id)
    )


PAGE = (
    struct.pack("<H", 2)
    + _row(b"w-a", status=2)
    + _row(b"w-b", status=3, machine_id=None)
    + b"\x01"
    + struct.pack("<H", 3)
    + b"w-b"
)
LAST_PAGE = struct.pack("<H", 1) + _row(b"w-c") + b"\x00"


def test_worker_list_is_a_table_op() -> None:
    assert is_table_op(OpCode.WORKER_LIST)
    request = serialize_request(1, OpCode.WORKER_LIST, b"ns", b"", encode_worker_list_body(5, None))
    # [ns][key][body], with no value length or options behind it.
    assert request[32:].endswith(b"\x00\x00" + encode_worker_list_body(5, None))


def test_the_request_is_a_present_limit_then_the_cursor() -> None:
    assert encode_worker_list_body(5, None) == (
        b"\x01" + struct.pack("<I", 5) + b"\x00\x00" + b"\x00\x00"
    )
    assert encode_worker_list_body(1024, b"w-b") == (
        b"\x01" + struct.pack("<I", 1024) + struct.pack("<H", 3) + b"w-b" + b"\x00\x00"
    )


def test_the_action_filter_follows_the_cursor() -> None:
    assert encode_worker_list_body(5, None, "charge") == (
        b"\x01" + struct.pack("<I", 5) + b"\x00\x00" + struct.pack("<H", 6) + b"charge"
    )
    assert encode_worker_list_body(5, None, None) == encode_worker_list_body(5, None, "")


@pytest.mark.parametrize("limit", [0, 1025, -1])
def test_a_limit_outside_1_to_1024_is_refused(limit: int) -> None:
    with pytest.raises(ValidationError):
        encode_worker_list_body(limit, None)


def test_a_cursor_past_4096_bytes_is_refused() -> None:
    encode_worker_list_body(1, b"x" * 4096)
    with pytest.raises(ValidationError):
        encode_worker_list_body(1, b"x" * 4097)


def test_a_page_decodes_every_row_and_the_next_cursor() -> None:
    workers, cursor = decode_worker_list_answer(PAGE)
    assert cursor == b"w-b"
    a, b = workers
    assert (a.worker_id, a.worker_type, a.status) == (
        "w-a",
        WorkerType.STREAM,
        WorkerStatus.DRAINING,
    )
    assert (a.current_load, a.max_concurrency, a.tasks_completed, a.tasks_failed) == (2, 8, 40, 3)
    assert (a.registered_at_ms, a.last_heartbeat_ms, a.machine_id) == (1000, 2000, "m-1")
    assert (b.worker_id, b.status, b.machine_id) == ("w-b", WorkerStatus.UNHEALTHY, None)


def test_the_last_page_has_no_cursor() -> None:
    workers, cursor = decode_worker_list_answer(LAST_PAGE)
    assert [w.worker_id for w in workers] == ["w-c"]
    assert cursor is None


@pytest.mark.parametrize("n", range(len(PAGE)))
def test_a_page_cut_short_is_refused(n: int) -> None:
    with pytest.raises(IncompleteResponseError):
        decode_worker_list_answer(PAGE[:n])


def test_a_page_with_bytes_left_over_is_refused() -> None:
    with pytest.raises(IncompleteResponseError):
        decode_worker_list_answer(LAST_PAGE + b"\x00")


@pytest.mark.parametrize("at, value", [(7, 2), (8, 4)])  # worker_type, status
def test_an_enum_value_this_client_doesnt_know_is_refused(at: int, value: int) -> None:
    bad = bytearray(LAST_PAGE)
    bad[at] = value
    with pytest.raises(IncompleteResponseError):
        decode_worker_list_answer(bytes(bad))


async def test_the_client_sends_the_table_body_and_returns_the_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FloClient("localhost:1")
    calls: list[tuple[OpCode, bytes, bytes, bytes]] = []

    async def fake_send(
        op_code: OpCode, namespace: str, key: bytes, value: bytes, options: bytes = b"", **_: Any
    ) -> RawResponse:
        calls.append((op_code, key, value, options))
        return RawResponse(status=StatusCode.OK, data=PAGE, request_id=0)

    monkeypatch.setattr(client, "_send_and_check", fake_send)
    result = await client.worker.list(WorkerListOptions(limit=2, cursor=b"w-0"))
    assert calls == [(OpCode.WORKER_LIST, b"", encode_worker_list_body(2, b"w-0"), b"")]
    assert [w.worker_id for w in result.workers] == ["w-a", "w-b"]
    assert result.cursor == b"w-b"
