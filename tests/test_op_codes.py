"""The op codes follow the server's list: retired ones are gone, and the
internal and admin ops are known, though no client API sends them."""

import pytest

from flo.types import OpCode


def test_retired_namespace_settings_ops_are_gone() -> None:
    assert "NAMESPACE_CONFIG_SET" not in OpCode.__members__
    assert "NAMESPACE_CONFIG_GET" not in OpCode.__members__
    for code in (0x014, 0x015):
        with pytest.raises(ValueError):
            OpCode(code)


def test_namespace_and_cluster_ops_have_the_servers_codes() -> None:
    assert OpCode(0x016) is OpCode.NAMESPACE_ENSURE
    assert OpCode(0x032) is OpCode.CLUSTER_PROMOTE
    assert OpCode(0x033) is OpCode.CLUSTER_REMOVE
    assert OpCode(0x034) is OpCode.CLUSTER_REPAIR_SKIP
    with pytest.raises(ValueError):
        OpCode(0x035)


def test_no_two_ops_share_a_code() -> None:
    # An IntEnum makes a repeated value an alias, which would hide one op.
    assert len(OpCode.__members__) == len(set(OpCode))
