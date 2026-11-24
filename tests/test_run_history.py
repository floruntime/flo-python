"""workflow.history and action.history: one request, one page, on the op table;
and workflow.status's output preview. Bodies are checked byte for byte
against the server's schemas."""

import struct
from typing import Any

import pytest

from flo import FloClient, RunHistoryEvent, RunHistoryOptions, RunHistoryPage
from flo.exceptions import IncompleteResponseError, ValidationError
from flo.kv_body import (
    RUN_HISTORY_HOLD_MS,
    decode_run_history_answer,
    encode_run_history_body,
    is_table_op,
)
from flo.types import HEADER_SIZE, OpCode, StatusCode
from flo.wire import RawResponse, serialize_request
from flo.workflows import _parse_status


def _event(event_type: bytes, detail: bytes, extra: bytes, ts: int) -> bytes:
    return (
        struct.pack("<I", len(event_type))
        + event_type
        + struct.pack("<I", len(detail))
        + detail
        + struct.pack("<H", len(extra))
        + extra
        + struct.pack("<q", ts)
    )


def _page(events: list[bytes], cursor: int | None) -> bytes:
    tail = b"\x00\x00" if cursor is None else b"\x01\x01" + struct.pack("<Q", cursor)
    return struct.pack("<H", len(events)) + b"".join(events) + tail


# ── Request ────────────────────────────────────────────────────────────────


def test_both_history_ops_are_table_ops_at_the_servers_codes() -> None:
    assert (int(OpCode.WORKFLOW_HISTORY), int(OpCode.ACTION_HISTORY)) == (0x345, 0x310)
    assert is_table_op(OpCode.WORKFLOW_HISTORY) and is_table_op(OpCode.ACTION_HISTORY)
    assert not is_table_op(OpCode.WORKFLOW_STATUS)


def test_a_request_omits_what_the_caller_left_for_the_server() -> None:
    assert encode_run_history_body(None, None, None) == b"\x00\x00\x00"
    assert encode_run_history_body(5, 42, True) == (
        b"\x01" + struct.pack("<I", 5) + b"\x01" + struct.pack("<Q", 42) + b"\x01\x01"
    )
    assert encode_run_history_body(1000, None, False) == (
        b"\x01" + struct.pack("<I", 1000) + b"\x00" + b"\x01\x00"
    )


@pytest.mark.parametrize("limit", [0, -1, 1001])
def test_a_limit_outside_one_to_a_thousand_is_refused_before_sending(limit: int) -> None:
    # The server would clamp 0 to 1 and 1001 to 1000: neither is what was asked.
    with pytest.raises(ValidationError):
        encode_run_history_body(limit, None, None)


def test_a_history_request_is_framed_as_a_table_op() -> None:
    body = encode_run_history_body(None, 7, None)
    frame = serialize_request(1, OpCode.ACTION_HISTORY, b"ns", b"act-1", body)
    payload = frame[HEADER_SIZE:]
    # [ns_len:u16][ns][key_len:u16][key][body], no value length or options.
    assert payload == b"\x02\x00ns\x05\x00act-1" + body


def test_a_history_read_may_be_held_for_its_runs_history() -> None:
    body = encode_run_history_body(None, None, None)
    for op in (OpCode.WORKFLOW_HISTORY, OpCode.ACTION_HISTORY):
        assert FloClient._server_wait_ms(op, body, b"") == RUN_HISTORY_HOLD_MS == 5000


# ── Page ───────────────────────────────────────────────────────────────────


def test_an_empty_last_page() -> None:
    page = decode_run_history_answer("workflow.history", _page([], None))
    assert page == RunHistoryPage(events=[], has_more=False, cursor=None)


def test_a_page_carries_its_events_and_the_next_pages_cursor() -> None:
    data = _page(
        [
            _event(b"action_invoked", b'{"n":1}', b"", 10),
            _event(b"action_settled", b"done", b"approved", 20),
            _event(b"action_released", b"x" * 256, b"truncated", 30),
        ],
        cursor=99,
    )
    page = decode_run_history_answer("action.history", data)
    assert page.has_more and page.cursor == 99
    assert page.events == [
        RunHistoryEvent(type="action_invoked", detail=b'{"n":1}', extra="", timestamp_ms=10),
        RunHistoryEvent(type="action_settled", detail=b"done", extra="approved", timestamp_ms=20),
        RunHistoryEvent(
            type="action_released", detail=b"x" * 256, extra="truncated", timestamp_ms=30
        ),
    ]


def test_a_detail_past_a_u16_length_decodes_whole() -> None:
    whole = bytes(range(256)) * 256  # 65536 bytes, the bound
    page = decode_run_history_answer(
        "workflow.history", _page([_event(b"workflow_output", whole, b"", 1)], None)
    )
    assert page.events[0].detail == whole
    with pytest.raises(IncompleteResponseError):
        decode_run_history_answer(
            "workflow.history", _page([_event(b"workflow_output", whole + b"x", b"", 1)], None)
        )


def test_an_extra_past_its_bound_is_refused() -> None:
    with pytest.raises(IncompleteResponseError):
        decode_run_history_answer("action.history", _page([_event(b"e", b"", b"x" * 257, 1)], None))


@pytest.mark.parametrize(
    "tail",
    [
        b"\x01\x00",  # more, but no cursor
        b"\x00\x01" + struct.pack("<Q", 5),  # a cursor on the last page
        b"\x02\x00",  # a bool of 2
        b"\x00\x00\x00",  # a byte left over
        b"\x00",  # cut short
    ],
)
def test_a_page_whose_trailer_is_wrong_is_refused(tail: bytes) -> None:
    with pytest.raises(IncompleteResponseError):
        decode_run_history_answer("workflow.history", struct.pack("<H", 0) + tail)


def test_a_page_cut_short_anywhere_is_refused() -> None:
    data = _page([_event(b"step_started", b"start", b"", 5)], cursor=3)
    for n in range(len(data)):
        with pytest.raises(IncompleteResponseError):
            decode_run_history_answer("workflow.history", data[:n])


def test_a_page_listing_more_than_a_thousand_events_is_refused() -> None:
    with pytest.raises(IncompleteResponseError):
        decode_run_history_answer("workflow.history", struct.pack("<H", 1001) + b"\x00\x00")


@pytest.mark.parametrize("family", ["workflow", "action"])
async def test_both_families_send_the_body_and_answer_the_page(
    monkeypatch: pytest.MonkeyPatch, family: str
) -> None:
    client = FloClient("localhost:1")
    sent: list[tuple[Any, ...]] = []

    async def answer(*args: Any, **_: Any) -> RawResponse:
        sent.append(args)
        data = _page([_event(b"e", b"d", b"", 1)], cursor=8)
        return RawResponse(status=StatusCode.OK, data=data, request_id=0)

    monkeypatch.setattr(client, "_send_request", answer)
    ops = client.workflow if family == "workflow" else client.action
    page = await ops.history("run-1", RunHistoryOptions(limit=2, newest_first=True))
    assert (page.cursor, [e.type for e in page.events]) == (8, ["e"])
    op, _ns, key, body = sent[0][:4]
    expected = OpCode.WORKFLOW_HISTORY if family == "workflow" else OpCode.ACTION_HISTORY
    assert (op, key) == (expected, b"run-1")
    assert body == encode_run_history_body(2, None, True)


# ── workflow.status ────────────────────────────────────────────────────────


def _s16(b: bytes) -> bytes:
    return struct.pack("<H", len(b)) + b


def _status(tail: bytes, input_data: bytes = b"") -> bytes:
    return (
        _s16(b"wf-1")
        + _s16(b"order")
        + _s16(b"1.0.0")
        + bytes([3])
        + _s16(b"ship")
        + struct.pack("<I", len(input_data))
        + input_data
        + struct.pack("<q", 100)
        + b"\x01"
        + struct.pack("<q", 100)
        + b"\x01"
        + struct.pack("<q", 200)
        + b"\x00"
        + tail
    )


def test_an_ended_runs_status_carries_its_output_preview_and_whether_it_was_cut() -> None:
    preview = b"o" * 256
    s = _parse_status(_status(b"\x01" + struct.pack("<I", 256) + preview + b"\x01"))
    assert (s["status"], s["input"], s["completed_at"]) == ("completed", b"", 200)
    assert (s["output"], s["output_truncated"]) == (preview, True)
    s = _parse_status(_status(b"\x01" + struct.pack("<I", 2) + b"ok" + b"\x00"))
    assert (s["output"], s["output_truncated"]) == (b"ok", False)


def test_a_status_with_no_output_has_none() -> None:
    s = _parse_status(_status(b"\x00", input_data=b'{"a":1}'))
    assert "output" not in s and s["output_truncated"] is False
    assert s["input"] == b'{"a":1}'


def test_a_status_waiting_on_a_signal_names_it() -> None:
    data = _status(b"")[:-1] + b"\x01" + _s16(b"approve") + b"\x00"
    assert _parse_status(data)["wait_signal"] == "approve"


@pytest.mark.parametrize(
    "tail",
    [
        b"",  # no has_output byte
        b"\x00\x00",  # a byte after it
        b"\x01" + struct.pack("<I", 2) + b"ok",  # no output_truncated
        b"\x01" + struct.pack("<I", 2) + b"ok" + b"\x02",  # a bool of 2
        b"\x01" + struct.pack("<I", 2) + b"ok" + b"\x00\x00",  # a byte left over
    ],
)
def test_a_status_answer_that_isnt_whole_is_refused(tail: bytes) -> None:
    with pytest.raises(IncompleteResponseError):
        _parse_status(_status(tail))
