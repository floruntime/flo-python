"""KV ops, action.status and worker.list, as table ops.

The request is ``[namespace][key][body]``, the body coded by the op's
schema, field by field in order. An optional field is a presence byte then
its value; a field with a default is sent present. A byte string or list has
a length prefix as wide as its bound needs. An ok answer's data is the op's
response fields in order, and is decoded whole.
"""

import struct
from collections.abc import Sequence
from typing import TypeVar

from .exceptions import BlockTooLongError, IncompleteResponseError, ValidationError
from .types import (
    MAX_KV_VALUE_SIZE,
    MAX_WAIT_MS,
    ActionRunStatus,
    Condition,
    GetOptions,
    GetResult,
    MGetEntry,
    OpCode,
    PutOptions,
    PutResult,
    RunStatus,
    VersionEntry,
    WaitFor,
    WorkerInfo,
    WorkerStatus,
    WorkerType,
)

# The longest routing key; its length prefix is a u16.
_MAX_ROUTING_KEY = 4096
_MAX_JSON_PATH = 1024
_MAX_JSON_DOCUMENT = 252_819
_MAX_SCAN_CURSOR = 64
MAX_SCAN = 1024
MAX_MGET = 256
# The most bytes of mget key elements (each [len:u16][key]) in one request:
# the frame less the count and the longest routing key.
MAX_MGET_KEYS_BYTES = 253_848
MAX_HISTORY = 65
MAX_WORKER_LIST = 1024
_MAX_WORKER_ID = 4096
# The longest TTL whose nanoseconds fit a u64.
MAX_TTL_MS = 18_446_744_073_709
_U64_MAX = (1 << 64) - 1
_I64_MIN = -(1 << 63)
_I64_MAX = (1 << 63) - 1

_TABLE_OPS = frozenset(
    {
        OpCode.KV_PUT,
        OpCode.KV_GET,
        OpCode.KV_MGET,
        OpCode.KV_DELETE,
        OpCode.KV_SCAN,
        OpCode.KV_HISTORY,
        OpCode.KV_INCR,
        OpCode.KV_JSON_GET,
        OpCode.KV_JSON_SET,
        OpCode.KV_JSON_DEL,
        OpCode.KV_BEGIN_TXN,
        OpCode.KV_COMMIT_TXN,
        OpCode.KV_ROLLBACK_TXN,
        OpCode.KV_EXPIRE,
        OpCode.KV_EXISTS,
        OpCode.ACTION_STATUS,
        OpCode.WORKER_LIST,
    }
)


def is_table_op(op_code: int) -> bool:
    """Whether the op's request carries a schema-coded body, not a value and options."""
    return op_code in _TABLE_OPS


# ── Request fields ────────────────────────────────────────────────────────


def _u64(name: str, v: int) -> bytes:
    if not isinstance(v, int) or isinstance(v, bool) or not 0 <= v <= _U64_MAX:
        raise ValidationError(f"{name} must be a u64, got {v!r}")
    return struct.pack("<Q", v)


def _opt_u64(name: str, v: int | None) -> bytes:
    if v is None:
        return b"\x00"
    return b"\x01" + _u64(name, v)


def _limit(name: str, v: int, most: int) -> bytes:
    """A defaulted u32 limit in 1..most, sent present."""
    if not isinstance(v, int) or isinstance(v, bool) or not 1 <= v <= most:
        raise ValidationError(f"{name} must be 1..{most}, got {v!r}")
    return b"\x01" + struct.pack("<I", v)


def _bytes16(name: str, b: bytes, most: int) -> bytes:
    if len(b) > most:
        raise ValidationError(f"{name} is {len(b)} bytes, at most {most}")
    return struct.pack("<H", len(b)) + b


def _bytes32(name: str, b: bytes, most: int) -> bytes:
    if len(b) > most:
        raise ValidationError(f"{name} is {len(b)} bytes, at most {most}")
    return struct.pack("<I", len(b)) + bytes(b)


def _routing_key(k: str | None) -> bytes:
    if not k:
        return b"\x00"
    return b"\x01" + _bytes16("routing key", k.encode("utf-8"), _MAX_ROUTING_KEY)


def _path(path: str | None) -> bytes:
    return b"\x01" + _bytes16("JSON path", (path or "$").encode("utf-8"), _MAX_JSON_PATH)


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


def encode_delete_body(
    cas_version: int | None, routing_key: str | None, txn: int | None = None
) -> bytes:
    """kv.delete's request: cas_version, routing_key, txn."""
    return _opt_u64("cas_version", cas_version) + _routing_key(routing_key) + _opt_u64("txn", txn)


def encode_scan_body(limit: int, cursor: bytes | None) -> bytes:
    """kv.scan's request: limit, cursor (empty for the first page)."""
    cursor = cursor or b""
    if len(cursor) > _MAX_SCAN_CURSOR:
        raise ValidationError(f"scan cursor is {len(cursor)} bytes, at most {_MAX_SCAN_CURSOR}")
    return _limit("scan limit", limit, MAX_SCAN) + bytes([len(cursor)]) + cursor


def encode_history_body(limit: int, routing_key: str | None) -> bytes:
    """kv.history's request: limit, routing_key."""
    return _limit("history limit", limit, MAX_HISTORY) + _routing_key(routing_key)


def encode_mget_body(keys: Sequence[bytes], routing_key: str | None = None) -> bytes:
    """kv.mget's request: keys, routing_key."""
    if not 1 <= len(keys) <= MAX_MGET:
        raise ValidationError(f"mget takes 1..{MAX_MGET} keys, got {len(keys)}")
    elements = b"".join(_bytes16("mget key", k, _MAX_ROUTING_KEY) for k in keys)
    if len(elements) > MAX_MGET_KEYS_BYTES:
        raise ValidationError(f"mget keys are {len(elements)} bytes, at most {MAX_MGET_KEYS_BYTES}")
    return struct.pack("<H", len(keys)) + elements + _routing_key(routing_key)


def split_mget_keys(keys: Sequence[bytes]) -> list[Sequence[bytes]]:
    """The keys in runs that each fit one request frame; refuses a bad key list
    before any round trip."""
    if not 1 <= len(keys) <= MAX_MGET:
        raise ValidationError(f"mget takes 1..{MAX_MGET} keys, got {len(keys)}")
    runs: list[Sequence[bytes]] = []
    start, size = 0, 0
    for i, k in enumerate(keys):
        if len(k) > _MAX_ROUTING_KEY:
            raise ValidationError(f"mget key is {len(k)} bytes, at most {_MAX_ROUTING_KEY}")
        if size + 2 + len(k) > MAX_MGET_KEYS_BYTES:
            runs.append(keys[start:i])
            start, size = i, 0
        size += 2 + len(k)
    runs.append(keys[start:])
    return runs


def encode_incr_body(delta: int, routing_key: str | None, txn: int | None = None) -> bytes:
    """kv.incr's request: delta, routing_key, txn."""
    if not isinstance(delta, int) or isinstance(delta, bool) or not _I64_MIN <= delta <= _I64_MAX:
        raise ValidationError(f"delta must be an i64, got {delta!r}")
    return b"\x01" + struct.pack("<q", delta) + _routing_key(routing_key) + _opt_u64("txn", txn)


def encode_expire_body(
    ttl_ms: int, cas_version: int | None, routing_key: str | None, txn: int | None = None
) -> bytes:
    """kv.expire's request: ttl_ms (0 clears the TTL), cas_version, routing_key, txn."""
    if not isinstance(ttl_ms, int) or isinstance(ttl_ms, bool) or not 0 <= ttl_ms <= MAX_TTL_MS:
        raise ValidationError(f"ttl_ms must be 0..{MAX_TTL_MS}, got {ttl_ms!r}")
    return (
        struct.pack("<Q", ttl_ms)
        + _opt_u64("cas_version", cas_version)
        + _routing_key(routing_key)
        + _opt_u64("txn", txn)
    )


def encode_exists_body(routing_key: str | None, txn: int | None = None) -> bytes:
    """kv.exists's request: routing_key, txn."""
    return _routing_key(routing_key) + _opt_u64("txn", txn)


def encode_json_get_body(path: str | None, routing_key: str | None) -> bytes:
    """kv.json_get's request: path, routing_key."""
    return _path(path) + _routing_key(routing_key)


def encode_json_set_body(path: str | None, document: bytes, routing_key: str | None) -> bytes:
    """kv.json_set's request: path, document, routing_key."""
    return (
        _path(path)
        + _bytes32("JSON document", document, _MAX_JSON_DOCUMENT)
        + _routing_key(routing_key)
    )


def encode_json_del_body(path: str | None, routing_key: str | None) -> bytes:
    """kv.json_del's request: path, routing_key."""
    return _path(path) + _routing_key(routing_key)


def encode_worker_list_body(limit: int, cursor: bytes | None) -> bytes:
    """worker.list's request: limit, cursor (empty starts)."""
    return _limit("limit", limit, MAX_WORKER_LIST) + _bytes16(
        "cursor", cursor or b"", _MAX_WORKER_ID
    )


def encode_txn_body(txn: int) -> bytes:
    """kv.txn_commit's and kv.txn_rollback's request: txn."""
    return _u64("txn", txn)


# ── Answers ───────────────────────────────────────────────────────────────


class _Answer:
    """Reads an answer's fields in order; ``done`` refuses bytes left over."""

    def __init__(self, op: str, data: bytes):
        self._op = op
        self._data = bytes(data)
        self._at = 0

    def _take(self, n: int) -> bytes:
        if self._at + n > len(self._data):
            raise IncompleteResponseError(f"{self._op} answer is cut short")
        b = self._data[self._at : self._at + n]
        self._at += n
        return b

    def u8(self) -> int:
        return self._take(1)[0]

    def u16(self) -> int:
        return int(struct.unpack("<H", self._take(2))[0])

    def u32(self) -> int:
        return int(struct.unpack("<I", self._take(4))[0])

    def u64(self) -> int:
        return int(struct.unpack("<Q", self._take(8))[0])

    def i64(self) -> int:
        return int(struct.unpack("<q", self._take(8))[0])

    def flag(self) -> bool:
        b = self.u8()
        if b > 1:
            raise IncompleteResponseError(f"{self._op} answer has a bool of {b}")
        return b == 1

    def present(self) -> bool:
        """An optional field's presence byte."""
        return self.flag()

    def bytes8(self) -> bytes:
        return self._take(self.u8())

    def bytes16(self) -> bytes:
        return self._take(self.u16())

    def bytes32(self) -> bytes:
        return self._take(self.u32())

    def done(self) -> None:
        if self._at != len(self._data):
            raise IncompleteResponseError(
                f"{self._op} answer has {len(self._data) - self._at} bytes left over"
            )


def decode_get_answer(data: bytes) -> GetResult:
    """kv.get's answer: version, value, expires_at_ms (absent with no TTL)."""
    a = _Answer("kv.get", data)
    version = a.u64()
    value = a.bytes32()
    expires_at_ms = a.i64() if a.present() else None
    a.done()
    return GetResult(value=value, version=version, expires_at_ms=expires_at_ms)


def decode_set_answer(data: bytes) -> PutResult:
    """kv.set's answer: its version, absent (0) inside a transaction."""
    a = _Answer("kv.set", data)
    version = a.u64() if a.present() else 0
    a.done()
    return PutResult(version=version)


def decode_delete_answer(data: bytes) -> bool | None:
    """kv.delete's answer: whether the key existed, None inside a transaction."""
    a = _Answer("kv.delete", data)
    existed = a.flag() if a.present() else None
    a.done()
    return existed


def decode_scan_answer(data: bytes) -> tuple[list[str], bytes | None]:
    """kv.scan's answer: names, then the next page's cursor (None on the last page)."""
    a = _Answer("kv.scan", data)
    try:
        names = [a.bytes16().decode("utf-8") for _ in range(a.u16())]
    except UnicodeDecodeError as e:
        raise IncompleteResponseError("kv.scan answer has a name that isn't UTF-8") from e
    cursor = a.bytes8() if a.present() else None
    a.done()
    return names, cursor


def decode_history_answer(data: bytes) -> list[VersionEntry]:
    """kv.history's answer: versions, newest first."""
    a = _Answer("kv.history", data)
    versions = [
        VersionEntry(version=a.u64(), at_ms=a.i64(), deleted=a.flag(), value=a.bytes32())
        for _ in range(a.u8())
    ]
    a.done()
    return versions


def decode_mget_answer(data: bytes, keys: Sequence[str]) -> tuple[list[MGetEntry], int | None]:
    """kv.mget's answer: one entry per answered key, then the index of the first
    key not answered (None when all were)."""
    a = _Answer("kv.mget", data)
    count = a.u16()
    if count > len(keys):
        raise IncompleteResponseError(f"kv.mget answered {count} entries for {len(keys)} keys")
    entries = []
    for key in keys[:count]:
        found = a.flag()
        version = a.u64()
        entries.append(MGetEntry(key=key, found=found, version=version, value=a.bytes32()))
    next_index = a.u16() if a.present() else None
    a.done()
    if next_index == len(keys):
        next_index = None  # resumes past the last key: all were answered
    if next_index is None and count != len(keys):
        raise IncompleteResponseError(f"kv.mget answered {count} of {len(keys)} keys")
    if next_index is not None and next_index != count:
        raise IncompleteResponseError(
            f"kv.mget answered {count} entries but resumes at key {next_index}"
        )
    return entries, next_index


def decode_incr_answer(data: bytes) -> tuple[int | None, int | None]:
    """kv.incr's answer: version, value (both None inside a transaction)."""
    a = _Answer("kv.incr", data)
    version = a.u64() if a.present() else None
    value = a.i64() if a.present() else None
    a.done()
    return version, value


def decode_empty_answer(op: str, data: bytes) -> None:
    """An answer with no fields."""
    _Answer(op, data).done()


def decode_exists_answer(data: bytes) -> tuple[bool, int]:
    """kv.exists's answer: exists, version (0 when absent or buffered in a transaction)."""
    a = _Answer("kv.exists", data)
    exists = a.flag()
    version = a.u64()
    a.done()
    return exists, version


def decode_json_get_answer(data: bytes) -> tuple[int, bytes]:
    """kv.json_get's answer: version, document."""
    a = _Answer("kv.json_get", data)
    version = a.u64()
    document = a.bytes32()
    a.done()
    return version, document


def decode_json_set_answer(data: bytes) -> int:
    """kv.json_set's answer: version."""
    a = _Answer("kv.json_set", data)
    version = a.u64()
    a.done()
    return version


def decode_json_del_answer(data: bytes) -> int | None:
    """kv.json_del's answer: version, None when the path was "$" and the key is gone."""
    a = _Answer("kv.json_del", data)
    version = a.u64() if a.present() else None
    a.done()
    return version


def decode_txn_begin_answer(data: bytes) -> tuple[int, int]:
    """kv.txn_begin's answer: txn, pinned."""
    a = _Answer("kv.txn_begin", data)
    txn, pinned = a.u64(), a.u64()
    a.done()
    return txn, pinned


def decode_txn_commit_answer(data: bytes) -> tuple[int, int]:
    """kv.txn_commit's answer: commit_index, ops."""
    a = _Answer("kv.txn_commit", data)
    commit_index, ops = a.u64(), a.u16()
    a.done()
    return commit_index, ops


def decode_action_status_answer(run_id: str, data: bytes) -> ActionRunStatus:
    """action.status's answer: status, created_at, started_at?, completed_at?,
    attempt, output?, error_message?, worker_id?, with u16 lengths."""
    a = _Answer("action.status", data)
    code = a.u8()
    try:
        status = RunStatus(code)
    except ValueError:
        raise IncompleteResponseError(
            f"action.status answer has an unknown status {code}"
        ) from None
    created_at_ms = a.i64()
    started_at_ms = a.i64() if a.present() else None
    completed_at_ms = a.i64() if a.present() else None
    attempt = a.u32()
    output = a.bytes16() if a.present() else None
    error = a.bytes16() if a.present() else None
    worker_id = a.bytes16() if a.present() else None
    a.done()
    return ActionRunStatus(
        run_id=run_id,
        status=status,
        created_at_ms=created_at_ms,
        attempt=attempt,
        started_at_ms=started_at_ms,
        completed_at_ms=completed_at_ms,
        output=output,
        error_message=error.decode("utf-8", errors="replace") if error is not None else None,
        worker_id=worker_id.decode("utf-8", errors="replace") if worker_id is not None else None,
    )


def decode_action_invoke_answer(data: bytes) -> str:
    """action.invoke's answer: [run_id_len:u16][run_id][has_output:u8]; the
    server never sets output."""
    a = _Answer("action.invoke", data)
    run_id = a.bytes16()
    if not run_id:
        raise IncompleteResponseError("action.invoke answer has no run id")
    if a.present():
        raise IncompleteResponseError("action.invoke answer carries an output")
    a.done()
    return run_id.decode("utf-8")


_E = TypeVar("_E", WorkerType, WorkerStatus)


def _enum8(a: _Answer, op: str, kind: type[_E]) -> _E:
    code = a.u8()
    try:
        return kind(code)
    except ValueError:
        raise IncompleteResponseError(
            f"{op} answer has an unknown {kind.__name__} {code}"
        ) from None


def decode_worker_list_answer(data: bytes) -> tuple[list[WorkerInfo], bytes | None]:
    """worker.list's answer: workers in id order, then the next page's cursor
    (None on the last page)."""
    op = "worker.list"
    a = _Answer(op, data)
    count = a.u16()
    if count > MAX_WORKER_LIST:
        raise IncompleteResponseError(f"{op} answer lists {count} workers")
    workers = []
    for _ in range(count):
        worker_id = a.bytes16().decode("utf-8", errors="replace")
        worker_type = _enum8(a, op, WorkerType)
        status = _enum8(a, op, WorkerStatus)
        current_load, max_concurrency = a.u32(), a.u32()
        tasks_completed, tasks_failed = a.u64(), a.u64()
        registered_at_ms, last_heartbeat_ms = a.i64(), a.i64()
        machine_id = a.bytes8().decode("utf-8", errors="replace") if a.present() else None
        workers.append(
            WorkerInfo(
                worker_id=worker_id,
                worker_type=worker_type,
                status=status,
                current_load=current_load,
                max_concurrency=max_concurrency,
                tasks_completed=tasks_completed,
                tasks_failed=tasks_failed,
                registered_at_ms=registered_at_ms,
                last_heartbeat_ms=last_heartbeat_ms,
                machine_id=machine_id,
            )
        )
    cursor = a.bytes16() if a.present() else None
    a.done()
    return workers, cursor
