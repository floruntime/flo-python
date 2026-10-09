"""The SDK hard-codes the wire protocol version; it must match the server's.

The test reads the server's ``proto.zig``: the file ``FLO_PROTO_ZIG`` names
(CI fetches it at the pinned flo release), else the Flo runtime source
checked out beside this repo (the workspace layout). With neither it is
skipped rather than guessed, except in CI, where that fails.
"""

import os
import re
from pathlib import Path

import pytest

from flo.types import MAGIC, VERSION

_CANDIDATES = [
    Path(__file__).resolve().parents[3] / "flo" / "src" / "protocol" / "proto.zig",
    Path(__file__).resolve().parents[2] / "flo" / "src" / "protocol" / "proto.zig",
]


def _server_proto() -> Path | None:
    named = os.environ.get("FLO_PROTO_ZIG")
    if named:
        return Path(named)
    for p in _CANDIDATES:
        if p.is_file():
            return p
    return None


def test_protocol_version_matches_server() -> None:
    src = _server_proto()
    if src is None:
        if os.environ.get("CI"):
            pytest.fail("no proto.zig to check against: set FLO_PROTO_ZIG")
        pytest.skip("flo runtime source not checked out beside the SDK")
    text = src.read_text()
    m = re.search(r"pub const VERSION: u8 = (0x[0-9A-Fa-f]+|\d+);", text)
    assert m, "could not find VERSION in proto.zig"
    assert int(m.group(1), 0) == VERSION, f"SDK VERSION {VERSION:#x} != server {m.group(1)}"


def test_magic_is_flo() -> None:
    assert MAGIC.to_bytes(4, "little") == b"FLO\0"
