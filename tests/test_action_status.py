"""action.status and action.invoke answers, decoded."""

import struct

import pytest

from flo.exceptions import IncompleteResponseError
from flo.kv_body import decode_action_invoke_answer, decode_action_status_answer
from flo.types import RunStatus

# A completed run: started, completed, attempt 2, output "ok", no error.
COMPLETED = (
    bytes([2])
    + struct.pack("<q", 100)
    + b"\x01"
    + struct.pack("<q", 110)
    + b"\x01"
    + struct.pack("<q", 120)
    + struct.pack("<I", 2)
    + b"\x01"
    + struct.pack("<H", 2)
    + b"ok"
    + b"\x00"
)


def test_a_completed_runs_status_carries_its_times_attempt_and_output() -> None:
    s = decode_action_status_answer("run-1", COMPLETED)
    assert (s.run_id, s.status, s.created_at_ms, s.started_at_ms, s.completed_at_ms) == (
        "run-1",
        RunStatus.COMPLETED,
        100,
        110,
        120,
    )
    assert (s.attempt, s.output, s.error_message) == (2, b"ok", None)


def test_a_failed_runs_status_carries_its_error_and_no_output() -> None:
    data = (
        bytes([3])
        + struct.pack("<q", 100)
        + b"\x00\x00"
        + struct.pack("<I", 3)
        + b"\x00"
        + b"\x01"
        + struct.pack("<H", 4)
        + b"boom"
    )
    s = decode_action_status_answer("run-1", data)
    assert (s.status, s.started_at_ms, s.output, s.error_message) == (
        RunStatus.FAILED,
        None,
        None,
        "boom",
    )


@pytest.mark.parametrize("n", range(len(COMPLETED)))
def test_a_status_answer_cut_short_is_refused(n: int) -> None:
    with pytest.raises(IncompleteResponseError):
        decode_action_status_answer("run-1", COMPLETED[:n])


def test_a_status_answer_with_bytes_left_over_is_refused() -> None:
    with pytest.raises(IncompleteResponseError):
        decode_action_status_answer("run-1", COMPLETED + b"\x00")


def test_a_status_byte_this_client_doesnt_know_is_refused() -> None:
    with pytest.raises(IncompleteResponseError):
        decode_action_status_answer("run-1", bytes([200]) + COMPLETED[1:])


def test_an_invoke_answer_is_the_run_id_alone() -> None:
    assert decode_action_invoke_answer(struct.pack("<H", 5) + b"act-1" + b"\x00") == "act-1"
    run = struct.pack("<H", 5) + b"act-1"
    # The has_output byte is there and 0, and nothing follows it.
    for bad in (
        b"",
        b"\x00\x00\x00",
        struct.pack("<H", 9) + b"a",
        run,
        run + b"\x01",
        run + b"\x00\x00",
    ):
        with pytest.raises(IncompleteResponseError):
            decode_action_invoke_answer(bad)
