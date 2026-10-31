"""workflow.list_runs sends every field of its request, empty ones as length 0,
and decodes the page's runs with has_more and the cursor, refusing a malformed
answer."""

import struct
from typing import Any

import pytest

from flo import FloClient, WorkflowListRunsOptions, WorkflowRunEntry
from flo.exceptions import IncompleteResponseError
from flo.types import OpCode, StatusCode
from flo.wire import RawResponse
from flo.workflows import _parse_run_list

Run = tuple[bytes, bytes, bytes, int]


def _field(b: bytes) -> bytes:
    return struct.pack("<H", len(b)) + b


def _run_page(runs: list[Run], cursor: bytes | None, has_more: int | None = None) -> bytes:
    """[count:u32]([run_id][workflow][status] each u16-prefixed, [created_at:i64])*
    [has_more:u8][cursor_len:u16][cursor]."""
    out = struct.pack("<I", len(runs))
    for run_id, workflow, status, created_at in runs:
        out += _field(run_id) + _field(workflow) + _field(status) + struct.pack("<q", created_at)
    cursor = cursor or b""
    flag = (1 if cursor else 0) if has_more is None else has_more
    return out + bytes([flag]) + _field(cursor)


def _request(limit: int, status: bytes, cursor: bytes, search: bytes) -> bytes:
    return struct.pack("<I", limit) + _field(status) + _field(cursor) + _field(search)


async def test_list_runs_sends_every_field_and_pages_by_the_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FloClient("localhost:1")
    cursor = struct.pack("<q", 7) + b"run-2"
    pages = [
        # Short of the limit, yet more to come: another shard holds the rest.
        _run_page([(b"run-2", b"order", b"running", 7)], cursor),
        _run_page([(b"run-1", b"refund", b"completed", 5)], None),
    ]
    calls: list[tuple[OpCode, bytes, bytes]] = []

    async def fake_send(
        op_code: OpCode, namespace: str, key: bytes, value: bytes, options: bytes = b"", **_: Any
    ) -> RawResponse:
        calls.append((op_code, key, value))
        return RawResponse(status=StatusCode.OK, data=pages[len(calls) - 1], request_id=0)

    monkeypatch.setattr(client, "_send_and_check", fake_send)

    page = await client.workflow.list_runs(WorkflowListRunsOptions(limit=3))
    assert page.runs == [
        WorkflowRunEntry(run_id="run-2", workflow="order", status="running", created_at=7)
    ]
    assert (page.cursor, page.has_more) == (cursor, True)
    page = await client.workflow.list_runs(
        WorkflowListRunsOptions(
            workflow_name="refund", status="completed", search="ref", limit=3, cursor=page.cursor
        )
    )
    assert [(r.run_id, r.workflow) for r in page.runs] == [("run-1", "refund")]
    assert (page.cursor, page.has_more) == (None, False)
    assert calls == [
        (OpCode.WORKFLOW_LIST_RUNS, b"", _request(3, b"", b"", b"")),
        (OpCode.WORKFLOW_LIST_RUNS, b"refund", _request(3, b"completed", cursor, b"ref")),
    ]


@pytest.mark.parametrize(
    "data",
    [
        _run_page([(b"r", b"w", b"running", 0)], None)[:-1],  # cut short
        _run_page([(b"r", b"w", b"running", 0)], None) + b"\x00",  # a byte left over
        _run_page([], None, has_more=1),  # more, but no cursor
        _run_page([], None, has_more=2),  # has_more isn't a flag
        _run_page([], b"c", has_more=0),  # last page, yet a cursor
        _run_page([(b"\xff", b"w", b"running", 0)], None),  # a run id that isn't UTF-8
        struct.pack("<I", 1025),  # more than a page holds
    ],
    ids=[
        "short",
        "trailing",
        "more-without-cursor",
        "bad-flag",
        "cursor-on-last-page",
        "not-utf8",
        "too-many",
    ],
)
def test_list_runs_answer_is_refused_when_malformed(data: bytes) -> None:
    with pytest.raises(IncompleteResponseError):
        _parse_run_list(data)


@pytest.mark.asyncio
async def test_list_runs_refuses_a_limit_outside_u32() -> None:
    from flo import FloClient, WorkflowListRunsOptions
    from flo.exceptions import ValidationError

    client = FloClient("127.0.0.1:1")
    for limit in (-1, 1 << 32):
        with pytest.raises(ValidationError):
            await client.workflow.list_runs(WorkflowListRunsOptions(limit=limit))
