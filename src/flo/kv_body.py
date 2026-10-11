"""kv.get and kv.set as table ops.

The request is ``[namespace][key][body]``, the body coded by the op's
schema, field by field in order. An optional field is a presence byte then
its value; a field with a default is sent present. A byte string has a
length prefix as wide as its bound needs.
"""

import struct

from .exceptions import BlockTooLongError, IncompleteResponseError, ValidationError
from .types import (
    MAX_KV_VALUE_SIZE,
    MAX_WAIT_MS,
    Condition,
    GetOptions,
    GetResult,
    OpCode,
    PutOptions,
    PutResult,
    WaitFor,
)

# The longest routing key; its length prefix is a u16.
_MAX_ROUTING_KEY = 4096
_U64_MAX = (1 << 64) - 1


def is_table_op(op_code: int) -> bool:
    """Whether the op's request carries a schema-coded body, not a value and options."""
    return op_code in (OpCode.KV_GET, OpCode.KV_PUT)


def _opt_u64(name: str, v: int | None) -> bytes:
    if v is None:
        return b"\x00"
    if not isinstance(v, int) or isinstance(v, bool) or not 0 <= v <= _U64_MAX:
        raise ValidationError(f"{name} must be a u64, got {v!r}")
    return b"\x01" + struct.pack("<Q", v)


def _routing_key(k: str | None) -> bytes:
    if not k:
        return b"\x00"
    b = k.encode("utf-8")
    if len(b) > _MAX_ROUTING_KEY:
        raise ValidationError(f"routing key is {len(b)} bytes, at most {_MAX_ROUTING_KEY}")
    return b"\x01" + struct.pack("<H", len(b)) + b


def encode_get_body(opts: GetOptions, txn: int | None = None) -> bytes:
    """kv.get's request: wait_ms, wait_for, routing_key, txn."""
    wait_ms = opts.wait_ms
    if not isinstance(wait_ms, int) or isinstance(wait_ms, bool) or wait_ms < 0:
        raise ValidationError(f"wait_ms must be a whole number of ms, got {wait_ms!r}")
    if wait_ms > MAX_WAIT_MS:
        raise BlockTooLongError(
            f"a wait (wait_ms) is at most {MAX_WAIT_MS} ms (5 minutes), got {wait_ms}"
        )
    wait_for = WaitFor(opts.wait_for)
    return (
        b"\x01"
        + struct.pack("<Q", wait_ms)
        + b"\x01"
        + bytes([wait_for])
        + _routing_key(opts.routing_key)
        + _opt_u64("txn", txn)
    )


def encode_set_body(value: bytes, opts: PutOptions, txn: int | None = None) -> bytes:
    """kv.set's request: value, ttl_ms, condition, cas_version, routing_key, txn."""
    if len(value) > MAX_KV_VALUE_SIZE:
        raise ValidationError(f"value is {len(value)} bytes, at most {MAX_KV_VALUE_SIZE}")
    condition = Condition(opts.condition)
    ttl = opts.ttl_ms or None
    return (
        struct.pack("<I", len(value))
        + bytes(value)
        + _opt_u64("ttl_ms", ttl)
        + b"\x01"
        + bytes([condition])
        + _opt_u64("cas_version", opts.cas_version)
        + _routing_key(opts.routing_key)
        + _opt_u64("txn", txn)
    )


def decode_get_answer(data: bytes) -> GetResult:
    """kv.get's answer: ``[version:u64][value_len:u32][value]``, whole."""
    if len(data) < 12:
        raise IncompleteResponseError("kv.get answer is cut short")
    version, n = struct.unpack("<QI", data[:12])
    if len(data) != 12 + n:
        raise IncompleteResponseError("kv.get answer's length doesn't match its value")
    return GetResult(value=bytes(data[12:]), version=version)


def decode_set_answer(data: bytes) -> PutResult:
    """kv.set's answer: its version, absent (0) inside a transaction."""
    if data == b"\x00":
        return PutResult(version=0)
    if len(data) == 9 and data[0] == 1:
        return PutResult(version=struct.unpack("<Q", data[1:9])[0])
    raise IncompleteResponseError("kv.set answer is malformed")
