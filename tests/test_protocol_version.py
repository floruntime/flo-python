"""The SDK hard-codes the wire protocol version and op table hash; they must
match the server's.

When the Flo runtime source is checked out beside this repo (the workspace
layout, or CI with a sibling checkout) these tests read ``proto.zig`` and, if
it is built, ask ``flo -V`` for the table hash, and fail on drift. Elsewhere
they are skipped rather than guessed.
"""

import re
import subprocess
from pathlib import Path

import pytest

from flo.types import MAGIC, TABLE_HASH, VERSION

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


def _server_binary() -> Path | None:
    src = _server_proto()
    if src is None:
        return None
    binary = src.parents[2] / "zig-out" / "bin" / "flo"
    return binary if binary.is_file() else None


@pytest.mark.skipif(_server_binary() is None, reason="no built flo binary beside the SDK")
def test_table_hash_matches_server() -> None:
    binary = _server_binary()
    assert binary is not None
    try:
        out = subprocess.run([binary, "-V"], capture_output=True, text=True, check=True).stdout
    except OSError:
        pytest.skip(f"{binary} doesn't run here (built for another platform?)")
    m = re.search(r"table 0x([0-9a-f]{16})", out)
    assert m, f"could not find the table hash in {out!r}"
    server = int(m.group(1), 16)
    assert server == TABLE_HASH, f"SDK table {TABLE_HASH:#018x} != server {server:#018x}"


def test_magic_is_flo() -> None:
    assert MAGIC.to_bytes(4, "little") == b"FLO\0"
