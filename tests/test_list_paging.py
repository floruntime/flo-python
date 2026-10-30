"""List requests that aren't table ops carry paging in the value as [limit:u32][cursor],
never as options."""

import struct
from typing import Any

import pytest

from flo import (
    ActionListOptions,
    FloClient,
    ProcessingListOptions,
    WorkflowListDefinitionsOptions,
)
from flo.exceptions import IncompleteResponseError
from flo.kv_body import decode_action_list_answer
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
    await client.action.list(ActionListOptions(limit=5, cursor=b"\x01\x80\x05\x00\x00\x00"))
    assert_paging(calls[1], OpCode.ACTION_LIST, 5, b"\x01\x80\x05\x00\x00\x00")


def _action_page(
    names: list[bytes], cursor: bytes | None, values: list[bytes] | None = None
) -> bytes:
    """action.list's answer: [count:u32]([name_len:u16][name][value_len:u32][value])*
    [has_more:u8][cursor_len:u16][cursor]."""
    out = struct.pack("<I", len(names))
    for i, name in enumerate(names):
        value = values[i] if values else b""
        out += struct.pack("<H", len(name)) + name + struct.pack("<I", len(value)) + value
    cursor = cursor or b""
    return out + bytes([1 if cursor else 0]) + struct.pack("<H", len(cursor)) + cursor


async def test_action_list_pages_by_the_cursor_it_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FloClient("localhost:1")
    cursor = b"\x01\x80\x02\x00\x00\x00"
    pages = [
        _action_page([b"resize", b"thumbnail"], cursor),
        # A single-shard server sends each action's type, version and enabled flag.
        _action_page([b"upload"], None, [b"\x00\x01\x00\x00\x00\x01"]),
    ]
    values: list[bytes] = []

    async def fake_send(
        op_code: OpCode, namespace: str, key: bytes, value: bytes, options: bytes = b"", **_: Any
    ) -> RawResponse:
        values.append(value)
        return RawResponse(status=StatusCode.OK, data=pages[len(values) - 1], request_id=0)

    monkeypatch.setattr(client, "_send_and_check", fake_send)

    page = await client.action.list(ActionListOptions(limit=2))
    assert (page.names, page.cursor, page.has_more) == (["resize", "thumbnail"], cursor, True)
    page = await client.action.list(ActionListOptions(limit=2, cursor=page.cursor))
    assert (page.names, page.cursor, page.has_more) == (["upload"], None, False)
    assert values == [struct.pack("<I", 2), struct.pack("<I", 2) + cursor]


@pytest.mark.parametrize(
    "data",
    [
        _action_page([b"a"], None)[:-1],  # cut short
        _action_page([b"a"], None) + b"\x00",  # a byte left over
        struct.pack("<I", 0) + b"\x01" + struct.pack("<H", 0),  # more, but no cursor
        struct.pack("<I", 1025),  # more than a page holds
    ],
    ids=["short", "trailing", "more-without-cursor", "too-many"],
)
def test_action_list_answer_is_refused_when_malformed(data: bytes) -> None:
    with pytest.raises(IncompleteResponseError):
        decode_action_list_answer(data)
