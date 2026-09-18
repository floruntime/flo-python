"""The SDK hard-codes the wire protocol version; it must match the server's.

When the Flo runtime source is checked out beside this repo (the workspace
layout, or CI with a sibling checkout) this test reads ``proto.zig`` and fails
on drift. Elsewhere it is skipped rather than guessed.
"""

import re
from pathlib import Path

import pytest

from flo.types import MAGIC, VERSION

_CANDIDATES = [
    Path(__file__).resolve().parents[3] / "flo" / "src" / "protocol" / "proto.zig",
    Path(__file__).resolve().parents[2] / "flo" / "src" / "protocol" / "proto.zig",
]


def _server_proto() -> Path | None:
    for p in _CANDIDATES:
        if p.is_file():
            return p
    return None


@pytest.mark.skipif(
    _server_proto() is None, reason="flo runtime source not checked out beside the SDK"
)
def test_protocol_version_matches_server() -> None:
    src = _server_proto()
    assert src is not None
    text = src.read_text()
    m = re.search(r"pub const VERSION: u8 = (0x[0-9A-Fa-f]+|\d+);", text)
    assert m, "could not find VERSION in proto.zig"
    assert int(m.group(1), 0) == VERSION, f"SDK VERSION {VERSION:#x} != server {m.group(1)}"


def test_magic_is_flo() -> None:
    assert MAGIC.to_bytes(4, "little") == b"FLO\0"
