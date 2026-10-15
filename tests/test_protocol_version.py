"""The SDK hard-codes the wire protocol version and op table hash; they must
match the server's.

Set ``FLO_BINARY`` to a flo binary built from the runtime revision this SDK
targets; these tests ask its ``flo version --verbose`` and fail on drift. Without it they are
skipped: a checkout found nearby may be on any branch, its build stale.
"""

import os
import re
import subprocess
from pathlib import Path

import pytest

from flo.types import MAGIC, TABLE_HASH, VERSION


def _server_binary() -> Path | None:
    named = os.environ.get("FLO_BINARY")
    return Path(named) if named else None


@pytest.fixture
def server_version(tmp_path: Path) -> tuple[int, int]:
    """The (protocol, table hash) the binary reports. It runs with a fresh
    HOME, so nothing of the user's is read or written."""
    binary = _server_binary()
    if binary is None:
        pytest.skip("FLO_BINARY is not set")
    if not binary.is_file():
        pytest.fail(f"FLO_BINARY={binary} is not a file")
    env = {**os.environ, "HOME": str(tmp_path)}
    try:
        out = subprocess.run(
            [binary, "version", "--verbose"], capture_output=True, text=True, check=True, env=env
        ).stdout
    except OSError:
        pytest.skip(f"{binary} doesn't run here (built for another platform?)")
    m = re.search(r"protocol (\d+), table 0x([0-9a-f]{16})", out)
    assert m, f"could not find the protocol and table hash in {out!r}"
    return int(m.group(1)), int(m.group(2), 16)


def test_protocol_version_matches_server(server_version: tuple[int, int]) -> None:
    server, _ = server_version
    assert server == VERSION, f"SDK VERSION {VERSION:#x} != server {server:#x}"


def test_table_hash_matches_server(server_version: tuple[int, int]) -> None:
    _, server = server_version
    assert server == TABLE_HASH, f"SDK table {TABLE_HASH:#018x} != server {server:#018x}"


def test_magic_is_flo() -> None:
    assert MAGIC.to_bytes(4, "little") == b"FLO\0"
