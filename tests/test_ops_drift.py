"""StatusCode and Reason are hand-written; they must equal the server's lists.

Set ``FLO_OPS_JSON`` to the ops.json that ``zig build gen`` writes for the flo
revision this SDK targets. Without it these tests skip, except in CI.
"""

import json
import os
from enum import IntEnum
from pathlib import Path
from typing import Any

import pytest

from flo.types import Reason, StatusCode, StorageTier

from .conftest import IN_CI


@pytest.fixture(scope="module")
def ops() -> dict[str, Any]:
    named = os.environ.get("FLO_OPS_JSON")
    if not named:
        if IN_CI:
            pytest.fail("FLO_OPS_JSON is not set")
        pytest.skip("FLO_OPS_JSON is not set")
    data: dict[str, Any] = json.loads(Path(named).read_text())
    return data


def _sdk(enum: type[IntEnum]) -> dict[int, str]:
    return {m.value: m.name for m in enum}


def _server(entries: list[dict[str, Any]]) -> dict[int, str]:
    # The server's snake_case names are the SDK's in upper case.
    return {e["code"]: e["name"].upper() for e in entries}


def test_status_codes_match_the_server(ops: dict[str, Any]) -> None:
    assert _sdk(StatusCode) == _server(ops["statuses"])


def test_reasons_match_the_server(ops: dict[str, Any]) -> None:
    assert _sdk(Reason) == _server(ops["reasons"])


def test_storage_tiers_match_the_server(ops: dict[str, Any]) -> None:
    assert _sdk(StorageTier) == _server(ops["storage_tiers"])


def test_every_table_op_has_its_code_here(ops: dict[str, Any]) -> None:
    # The table's ops carry typed bodies: each must be an op code this SDK
    # knows, at the server's code.
    from flo.types import OpCode

    missing = {e["name"]: hex(e["code"]) for e in ops["ops"] if e["code"] not in set(OpCode)}
    assert missing == {}


def _why_values(ops: dict[str, Any], op: str) -> dict[int, str]:
    entry = next(e for e in ops["ops"] if e["name"] == op)
    refused = next(f for f in entry["response"] if f["name"] == "refused")
    why = next(f for f in refused["type"]["of"]["fields"] if f["name"] == "why")
    return {v["value"]: v["name"].upper() for v in why["type"]["values"]}


def test_refusal_reasons_match_the_server(ops: dict[str, Any]) -> None:
    # Queue reports and group operator commands answer the same why.
    from flo.types import QueueRefusalReason

    for op in ("queue.complete", "queue.dlq_requeue", "stream.group.steal"):
        assert _sdk(QueueRefusalReason) == _why_values(ops, op)


def test_every_table_op_is_sent_as_one(ops: dict[str, Any]) -> None:
    from flo.kv_body import is_table_op

    # No client API sends the namespace ops.
    sent = [e for e in ops["ops"] if not e["name"].startswith("namespace.")]
    assert [e["name"] for e in sent if not is_table_op(e["code"])] == []
