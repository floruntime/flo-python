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

from flo.types import Reason, StatusCode

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
