"""List requests carry paging in the value as [limit:u32][cursor], never as options."""

import struct
from typing import Any

import pytest

from flo import (
    ActionListOptions,
    FloClient,
    ProcessingListOptions,
    ScanOptions,
    WorkerListOptions,
    WorkflowListDefinitionsOptions,
)
from flo.types import OpCode, OptionTag, StatusCode
from flo.wire import OptionsIterator, RawResponse

Call = tuple[OpCode, str, bytes, bytes, bytes]

# An empty last page: no entries, has_more=0, no cursor.
EMPTY_PAGE = struct.pack("<I", 0) + b"\x00" + struct.pack("<H", 0)


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
        return RawResponse(status=StatusCode.OK, data=EMPTY_PAGE, request_id=0)

    monkeypatch.setattr(client, "_send_and_check", fake_send)
    return client, calls


def assert_paging(call: Call, op: OpCode, limit: int, cursor: bytes) -> None:
    (sent_op, _, _, value, options) = call
    assert sent_op == op
    assert value == struct.pack("<I", limit) + cursor
    assert OptionsIterator(options).find(OptionTag.LIMIT) is None


async def test_kv_scan(sent: tuple[FloClient, list[Call]]) -> None:
    client, calls = sent
    await client.kv.scan("user:", ScanOptions(limit=25, cursor=b"\x01\x00abc"))
    assert_paging(calls[0], OpCode.KV_SCAN, 25, b"\x01\x00abc")


async def test_kv_scan_defaults_to_server_limit(sent: tuple[FloClient, list[Call]]) -> None:
    client, calls = sent
    await client.kv.scan("user:")
    assert_paging(calls[0], OpCode.KV_SCAN, 0, b"")


async def test_processing_list(sent: tuple[FloClient, list[Call]]) -> None:
    client, calls = sent
    await client.processing.list(ProcessingListOptions(limit=10, cursor=b"cur"))
    assert_paging(calls[0], OpCode.PROCESSING_LIST, 10, b"cur")


async def test_workflow_list_definitions(sent: tuple[FloClient, list[Call]]) -> None:
    client, calls = sent
    await client.workflow.list_definitions(WorkflowListDefinitionsOptions(limit=7, cursor=b"cur"))
    assert_paging(calls[0], OpCode.WORKFLOW_LIST_DEFINITIONS, 7, b"cur")


async def test_action_list(sent: tuple[FloClient, list[Call]]) -> None:
    client, calls = sent
    await client.action.list(ActionListOptions(limit=5))
    assert_paging(calls[0], OpCode.ACTION_LIST, 5, b"")


async def test_worker_list(sent: tuple[FloClient, list[Call]]) -> None:
    client, calls = sent
    await client.worker.list(WorkerListOptions(limit=5))
    assert_paging(calls[0], OpCode.WORKER_LIST, 5, b"")
