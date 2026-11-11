"""The ops on the server's op table: KV, the action reports and reads,
worker.list and worker.info, stream group reads, the queue's dequeue,
reports and peek, and processing.remove.

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
    MAX_BLOCK_MS,
    MAX_KV_VALUE_SIZE,
    MAX_WAIT_MS,
    ActionInfo,
    ActionListEntry,
    ActionRunStatus,
    Condition,
    DequeueResult,
    ExistsResult,
    GetOptions,
    GetResult,
    GroupMemberState,
    Message,
    MGetEntry,
    OpCode,
    PeekedMessage,
    PeekResult,
    ProcessKind,
    PutOptions,
    PutResult,
    QueueRefusal,
    QueueRefusalReason,
    QueueReportResult,
    QueueTouchResult,
    RunStatus,
    StreamGroupEntry,
    StreamGroupListResult,
    StreamGroupMember,
    StreamGroupMembersResult,
    TaskAssignment,
    VersionEntry,
    WaitFor,
    WorkerDetails,
    WorkerInfo,
    WorkerProcess,
    WorkerStatus,
    WorkerType,
)

# The longest routing key; its length prefix is a u16.
_MAX_ROUTING_KEY = 4096
_MAX_JSON_PATH = 1024
_MAX_JSON_DOCUMENT = 252_819
# A scan's cursor: the shard it stopped on (u16), then the last key it
# returned; its length prefix is a u16.
_MAX_SCAN_CURSOR = 2 + 4096
MAX_SCAN = 1024
MAX_MGET = 256
# The most bytes of mget key elements (each [len:u16][key]) in one request:
# the frame less the count and the longest routing key.
MAX_MGET_KEYS_BYTES = 253_848
MAX_HISTORY = 65
MAX_WORKER_LIST = 1024
MAX_ACTION_LIST = 1024
MAX_WORKER_PROCESSES = 1024
_MAX_WORKER_ID = 4096
MAX_AWAIT_ACTIONS = 64
MAX_ACTION_NAME = 256
MAX_REQUEST_KEY = 64
MAX_OUTCOME = 256
# A run's result and error each have a u16 length.
MAX_RESULT = 65_535
MAX_ERROR = 65_535
MAX_FAIL_DELAY_MS = 86_400_000
MAX_PEEK = 100
MAX_DEQUEUE = 100
MAX_CONSUMER = 4082
MIN_LEASE_MS = 5_000
MAX_LEASE_MS = 43_200_000
MAX_QUEUE_ERROR = 1024
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
        OpCode.ACTION_AWAIT,
        OpCode.ACTION_COMPLETE,
        OpCode.ACTION_FAIL,
        OpCode.ACTION_TOUCH,
        OpCode.ACTION_INFO,
        OpCode.WORKER_LIST,
        OpCode.WORKER_INFO,
        OpCode.STREAM_GROUP_LIST,
        OpCode.STREAM_GROUP_MEMBERS,
        OpCode.QUEUE_PEEK,
        OpCode.QUEUE_DEQUEUE,
        OpCode.QUEUE_COMPLETE,
        OpCode.QUEUE_FAIL,
        OpCode.QUEUE_TOUCH,
        OpCode.TS_INFO,
        OpCode.PROCESSING_REMOVE,
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
    return _limit("scan limit", limit, MAX_SCAN) + _bytes16(
        "scan cursor", cursor or b"", _MAX_SCAN_CURSOR
    )


def encode_history_body(limit: int, before_version: int | None, routing_key: str | None) -> bytes:
    """kv.history's request: limit, before_version, routing_key."""
    return (
        _limit("history limit", limit, MAX_HISTORY)
        + _opt_u64("before_version", before_version)
        + _routing_key(routing_key)
    )


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


def encode_worker_list_body(limit: int, cursor: bytes | None, action: str | None = None) -> bytes:
    """worker.list's request: limit, cursor (empty starts), action (empty for
    every worker)."""
    return (
        _limit("limit", limit, MAX_WORKER_LIST)
        + _bytes16("cursor", cursor or b"", _MAX_WORKER_ID)
        + _bytes16("action", (action or "").encode("utf-8"), _MAX_WORKER_ID)
    )


def encode_txn_body(txn: int) -> bytes:
    """kv.txn_commit's and kv.txn_rollback's request: txn."""
    return _u64("txn", txn)


def _u32(name: str, v: int) -> bytes:
    if not isinstance(v, int) or isinstance(v, bool) or not 0 <= v <= 0xFFFFFFFF:
        raise ValidationError(f"{name} must be a u32, got {v!r}")
    return struct.pack("<I", v)


def _bytes8(name: str, b: bytes, most: int) -> bytes:
    if len(b) > most:
        raise ValidationError(f"{name} is {len(b)} bytes, at most {most}")
    return bytes([len(b)]) + bytes(b)


def _flag(v: bool) -> bytes:
    return b"\x01" if v else b"\x00"


# The server's wait for an await that names no block_ms.
AWAIT_DEFAULT_BLOCK_MS = 30_000


def encode_await_body(
    actions: Sequence[str], block_ms: int | None, request_key: bytes | None
) -> bytes:
    """action.await's request: actions (the first tried first), block_ms
    (None is the server's default), request_key."""
    if not 1 <= len(actions) <= MAX_AWAIT_ACTIONS:
        raise ValidationError(f"an await names 1..{MAX_AWAIT_ACTIONS} actions, got {len(actions)}")
    if block_ms is not None:
        if not isinstance(block_ms, int) or isinstance(block_ms, bool) or block_ms < 0:
            raise ValidationError(f"block_ms must be a whole number of ms, got {block_ms!r}")
        if block_ms > MAX_BLOCK_MS:
            raise BlockTooLongError(
                f"a wait (block_ms) is at most {MAX_BLOCK_MS} ms (5 minutes), got {block_ms}"
            )
    names = b"".join(_bytes16("action name", a.encode("utf-8"), MAX_ACTION_NAME) for a in actions)
    key = (
        b"\x00"
        if request_key is None
        else b"\x01" + _bytes8("request key", request_key, MAX_REQUEST_KEY)
    )
    wait = b"\x00" if block_ms is None else b"\x01" + struct.pack("<Q", block_ms)
    return bytes([len(actions)]) + names + wait + key


def await_block_ms(body: bytes) -> int:
    """How long an await body asks the server to wait: its block_ms, after
    the actions list."""
    at = 1
    for _ in range(body[0] if body else 0):
        at += 2 + int.from_bytes(body[at : at + 2], "little")
    if len(body) >= at + 9 and body[at] == 1:
        return int.from_bytes(body[at + 1 : at + 9], "little")
    return AWAIT_DEFAULT_BLOCK_MS


def _delivery(worker: str, task: TaskAssignment) -> bytes:
    """A report's delivery fields: action, worker, deliveries, term, index."""
    return (
        _bytes16("action name", task.action.encode("utf-8"), MAX_ACTION_NAME)
        + _bytes16("worker id", worker.encode("utf-8"), _MAX_WORKER_ID)
        + _u32("deliveries", task.deliveries)
        + _u64("term", task.term)
        + _u64("index", task.index)
    )


def encode_complete_body(worker: str, task: TaskAssignment, outcome: str, result: bytes) -> bytes:
    """action.complete's request: the delivery, outcome ("" is success), result."""
    return (
        _delivery(worker, task)
        + b"\x01"
        + _bytes16("outcome", outcome.encode("utf-8"), MAX_OUTCOME)
        + b"\x01"
        + _bytes16("result", result, MAX_RESULT)
    )


def encode_fail_body(
    worker: str,
    task: TaskAssignment,
    error: bytes,
    retry: bool,
    abandoned: bool,
    delay_ms: int,
) -> bytes:
    """action.fail's request: the delivery, retry, abandoned, delay_ms, error."""
    if (
        not isinstance(delay_ms, int)
        or isinstance(delay_ms, bool)
        or not 0 <= delay_ms <= MAX_FAIL_DELAY_MS
    ):
        raise ValidationError(f"delay_ms must be 0..{MAX_FAIL_DELAY_MS}, got {delay_ms!r}")
    return (
        _delivery(worker, task)
        + b"\x01"
        + _flag(retry)
        + b"\x01"
        + _flag(abandoned)
        + b"\x01"
        + struct.pack("<Q", delay_ms)
        + b"\x01"
        + _bytes16("error", error, MAX_ERROR)
    )


def encode_touch_body(worker: str, task: TaskAssignment) -> bytes:
    """action.touch's request: the delivery, and nothing else; the server
    extends by the run's own lease."""
    return _delivery(worker, task)


def encode_group_members_body(group: str) -> bytes:
    """stream.group.members's request: group."""
    return _bytes16("group", group.encode("utf-8"), _MAX_ROUTING_KEY)


def encode_dequeue_body(
    consumer: str, count: int, lease_ms: int | None, block_ms: int | None, ack: bool
) -> bytes:
    """queue.dequeue's request: consumer, count, lease_ms, block_ms, ack."""
    if lease_ms is not None and (
        not isinstance(lease_ms, int)
        or isinstance(lease_ms, bool)
        or not MIN_LEASE_MS <= lease_ms <= MAX_LEASE_MS
    ):
        raise ValidationError(f"lease_ms must be {MIN_LEASE_MS}..{MAX_LEASE_MS}, got {lease_ms!r}")
    wait = 0 if block_ms is None else block_ms
    if not isinstance(wait, int) or isinstance(wait, bool) or wait < 0:
        raise ValidationError(f"block_ms must be a whole number of ms, got {block_ms!r}")
    if wait > MAX_BLOCK_MS:
        raise BlockTooLongError(
            f"a wait (block_ms) is at most {MAX_BLOCK_MS} ms (5 minutes), got {wait}"
        )
    return (
        b"\x01"
        + _bytes16("consumer", consumer.encode("utf-8"), MAX_CONSUMER)
        + _limit("dequeue count", count, MAX_DEQUEUE)
        + _opt_u64("lease_ms", lease_ms)
        + b"\x01"
        + struct.pack("<Q", wait)
        + b"\x01"
        + _flag(ack)
    )


def dequeue_block_ms(body: bytes) -> int:
    """How long a dequeue body asks the server to wait: its block_ms, after
    the consumer, count and lease."""
    at = 3 + int.from_bytes(body[1:3], "little") + 5  # consumer, count
    at += 9 if body[at : at + 1] == b"\x01" else 1  # lease_ms
    if len(body) >= at + 9 and body[at] == 1:
        return int.from_bytes(body[at + 1 : at + 9], "little")
    return 0


def _report(consumer: str, delivery: DequeueResult, messages: Sequence[Message] | None) -> bytes:
    """A queue report's consumer, the dequeue's token, and its messages by seq
    and deliveries: all the dequeue's when ``messages`` is None."""
    picked = delivery.messages if messages is None else messages
    if not 1 <= len(picked) <= MAX_DEQUEUE:
        raise ValidationError(f"a report names 1..{MAX_DEQUEUE} messages, got {len(picked)}")
    return (
        b"\x01"
        + _bytes16("consumer", consumer.encode("utf-8"), MAX_CONSUMER)
        + _u64("term", delivery.term)
        + _u64("index", delivery.index)
        + bytes([len(picked)])
        + b"".join(_u64("seq", m.seq) + _u32("deliveries", m.deliveries) for m in picked)
    )


def encode_queue_complete_body(
    delivery: DequeueResult, messages: Sequence[Message] | None = None
) -> bytes:
    """queue.complete's request (and queue.touch's): the report."""
    return _report(delivery.consumer, delivery, messages)


def encode_queue_fail_body(
    delivery: DequeueResult,
    messages: Sequence[Message] | None,
    delay_ms: int,
    dead: bool,
    error: bytes,
) -> bytes:
    """queue.fail's request: the report, delay_ms, dead, error."""
    if (
        not isinstance(delay_ms, int)
        or isinstance(delay_ms, bool)
        or not 0 <= delay_ms <= MAX_FAIL_DELAY_MS
    ):
        raise ValidationError(f"delay_ms must be 0..{MAX_FAIL_DELAY_MS}, got {delay_ms!r}")
    return (
        _report(delivery.consumer, delivery, messages)
        + b"\x01"
        + struct.pack("<Q", delay_ms)
        + b"\x01"
        + _flag(dead)
        + b"\x01"
        + _bytes16("error", error, MAX_QUEUE_ERROR)
    )


def encode_peek_body(limit: int) -> bytes:
    """queue.peek's request: limit, 1..100."""
    return _limit("peek limit", limit, MAX_PEEK)


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
    cursor = a.bytes16() if a.present() else None
    a.done()
    return names, cursor


def decode_history_answer(data: bytes) -> tuple[list[VersionEntry], bool]:
    """kv.history's answer: versions, newest first, then whether older kept
    versions follow."""
    a = _Answer("kv.history", data)
    versions = [
        VersionEntry(version=a.u64(), at_ms=a.i64(), deleted=a.flag(), value=a.bytes32())
        for _ in range(a.u8())
    ]
    more = a.flag()
    a.done()
    return versions, more


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


def decode_exists_answer(data: bytes) -> ExistsResult:
    """kv.exists's answer: exists, version (0 when absent or buffered in a transaction)."""
    a = _Answer("kv.exists", data)
    exists = a.flag()
    version = a.u64()
    a.done()
    return ExistsResult(exists=exists, version=version)


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


def _text(a: _Answer, op: str, raw: bytes) -> str:
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as e:
        raise IncompleteResponseError(f"{op} answer has a field that isn't UTF-8") from e


def decode_action_status_answer(run_id: str, data: bytes) -> ActionRunStatus:
    """action.status's answer: status, created_at, started_at?, completed_at?,
    attempt, output?, error_message?, worker_id?, outcome?, with u16 lengths."""
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
    outcome = a.bytes16() if a.present() else None
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
        outcome=outcome.decode("utf-8", errors="replace") if outcome is not None else None,
    )


def decode_await_answer(data: bytes) -> TaskAssignment | None:
    """action.await's answer: the task, None if no run was ready in time."""
    op = "action.await"
    a = _Answer(op, data)
    if not a.present():
        a.done()
        return None
    run = _text(a, op, a.bytes16())
    namespace = _text(a, op, a.bytes8())
    deliveries, term, index = a.u32(), a.u64(), a.u64()
    action = _text(a, op, a.bytes16())
    created_at_ms, attempt, lease_ms = a.i64(), a.u32(), a.u64()
    caller_run = _text(a, op, a.bytes16()) if a.present() else None
    caller_workflow = _text(a, op, a.bytes16()) if a.present() else None
    task_input = a.bytes32()
    a.done()
    return TaskAssignment(
        run=run,
        namespace=namespace,
        deliveries=deliveries,
        term=term,
        index=index,
        action=action,
        created_at_ms=created_at_ms,
        attempt=attempt,
        lease_ms=lease_ms,
        input=task_input,
        caller_run=caller_run,
        caller_workflow=caller_workflow,
    )


def decode_touch_answer(data: bytes) -> int:
    """action.touch's answer: expires_at_ms, when the lease runs out without
    another touch."""
    a = _Answer("action.touch", data)
    expires_at_ms = a.i64()
    a.done()
    return expires_at_ms


def decode_action_info_answer(data: bytes) -> ActionInfo:
    """action.info's answer: the definition, ready counts, then the optional
    run counts and latency."""
    op = "action.info"
    a = _Answer(op, data)
    owner = _text(a, op, a.bytes16())
    version, timeout_ms, max_attempts = a.u32(), a.u64(), a.u32()
    created_at_ms, ready = a.i64(), a.u64()
    ready_capped, too_many_runs = a.flag(), a.flag()
    rest = [a.u64() if a.present() else None for _ in range(9)]
    a.done()
    return ActionInfo(
        owner=owner,
        version=version,
        timeout_ms=timeout_ms,
        max_attempts=max_attempts,
        created_at_ms=created_at_ms,
        ready=ready,
        ready_capped=ready_capped,
        too_many_runs=too_many_runs,
        running=rest[0],
        completed=rest[1],
        failed=rest[2],
        timed_out=rest[3],
        latency_count=rest[4],
        latency_avg_ms=rest[5],
        latency_p50_ms=rest[6],
        latency_p95_ms=rest[7],
        latency_max_ms=rest[8],
    )


def decode_group_list_answer(data: bytes) -> StreamGroupListResult:
    """stream.group.list's answer: groups, then whether more didn't fit."""
    op = "stream.group.list"
    a = _Answer(op, data)
    groups = [
        StreamGroupEntry(
            name=_text(a, op, a.bytes16()),
            members=a.u32(),
            pending=a.u64(),
            created_at_ms=a.i64(),
            ack_timeout_ms=a.u32(),
            max_deliver=a.u8(),
            last_delivered_ms=a.u64(),
            last_delivered_seq=a.u64(),
        )
        for _ in range(a.u16())
    ]
    more = a.flag()
    a.done()
    return StreamGroupListResult(groups=groups, more=more)


def decode_group_members_answer(data: bytes) -> StreamGroupMembersResult:
    """stream.group.members's answer: members, then whether more didn't fit."""
    op = "stream.group.members"
    a = _Answer(op, data)
    members = []
    for _ in range(a.u16()):
        member_id = _text(a, op, a.bytes16())
        code = a.u8()
        try:
            state = GroupMemberState(code)
        except ValueError:
            raise IncompleteResponseError(f"{op} answer has an unknown state {code}") from None
        pending, joined_at_ms = a.u32(), a.i64()
        last_active_ms = a.i64() if a.present() else None
        members.append(
            StreamGroupMember(
                id=member_id,
                state=state,
                pending=pending,
                joined_at_ms=joined_at_ms,
                last_active_ms=last_active_ms,
            )
        )
    more = a.flag()
    a.done()
    return StreamGroupMembersResult(members=members, more=more)


def decode_peek_answer(data: bytes) -> PeekResult:
    """queue.peek's answer: the next ready messages, then whether more may follow."""
    a = _Answer("queue.peek", data)
    messages = [
        PeekedMessage(
            seq=a.u64(),
            priority=a.u32(),
            attempts=a.u32(),
            enqueued_at_ms=a.i64(),
            size=a.u32(),
            payload=a.bytes16(),
        )
        for _ in range(a.u8())
    ]
    more = a.flag()
    a.done()
    return PeekResult(messages=messages, more=more)


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


def decode_action_list_answer(data: bytes) -> tuple[list[ActionListEntry], bytes | None]:
    """action.list's answer: [count:u32]([name_len:u16][name][value_len:u32][value])*
    [has_more:u8][cursor_len:u16][cursor], each value
    [version:u32][owner_len:u16][owner][timeout_ms:u64][max_attempts:u32]
    [created_at_ms:i64]. Returns the actions and the next page's cursor,
    None on the last page."""
    op = "action.list"
    a = _Answer(op, data)
    count = a.u32()
    if count > MAX_ACTION_LIST:
        raise IncompleteResponseError(f"{op} answer lists {count} actions")
    actions = []
    for _ in range(count):
        name = _text(a, op, a.bytes16())
        v = _Answer(op, a.bytes32())
        actions.append(
            ActionListEntry(
                name=name,
                version=v.u32(),
                owner=_text(v, op, v.bytes16()),
                timeout_ms=v.u64(),
                max_attempts=v.u32(),
                created_at_ms=v.i64(),
            )
        )
        v.done()
    has_more = a.flag()
    cursor = a.bytes16()
    a.done()
    if has_more and not cursor:
        raise IncompleteResponseError(f"{op} answer has more pages but no cursor")
    if not has_more and cursor:
        raise IncompleteResponseError(f"{op} answer has a cursor on its last page")
    return actions, cursor if has_more else None


_E = TypeVar("_E", WorkerType, WorkerStatus, ProcessKind)


def _enum8(a: _Answer, op: str, kind: type[_E]) -> _E:
    code = a.u8()
    try:
        return kind(code)
    except ValueError:
        raise IncompleteResponseError(
            f"{op} answer has an unknown {kind.__name__} {code}"
        ) from None


def _worker_row(a: _Answer, op: str) -> WorkerInfo:
    """A worker's row, as worker.list and worker.info both answer it."""
    worker_id = a.bytes16().decode("utf-8", errors="replace")
    worker_type = _enum8(a, op, WorkerType)
    status = _enum8(a, op, WorkerStatus)
    current_load, max_concurrency = a.u32(), a.u32()
    tasks_completed, tasks_failed = a.u64(), a.u64()
    registered_at_ms, last_heartbeat_ms = a.i64(), a.i64()
    machine_id = a.bytes8().decode("utf-8", errors="replace") if a.present() else None
    return WorkerInfo(
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


def decode_worker_list_answer(data: bytes) -> tuple[list[WorkerInfo], bytes | None]:
    """worker.list's answer: workers in id order, then the next page's cursor
    (None on the last page)."""
    op = "worker.list"
    a = _Answer(op, data)
    count = a.u16()
    if count > MAX_WORKER_LIST:
        raise IncompleteResponseError(f"{op} answer lists {count} workers")
    workers = [_worker_row(a, op) for _ in range(count)]
    cursor = a.bytes16() if a.present() else None
    a.done()
    return workers, cursor


def decode_worker_info_answer(data: bytes) -> WorkerDetails:
    """worker.info's answer: the worker's row, its metadata, its processes."""
    op = "worker.info"
    a = _Answer(op, data)
    worker = _worker_row(a, op)
    metadata = a.bytes16() if a.present() else None
    count = a.u16()
    if count > MAX_WORKER_PROCESSES:
        raise IncompleteResponseError(f"{op} answer lists {count} processes")
    processes = []
    for _ in range(count):
        name = a.bytes16().decode("utf-8", errors="replace")
        kind = _enum8(a, op, ProcessKind)
        processes.append(
            WorkerProcess(
                name=name,
                kind=kind,
                run_count=a.u64(),
                fail_count=a.u64(),
                last_run_at_ms=a.i64(),
            )
        )
    a.done()
    return WorkerDetails(worker=worker, metadata=metadata, processes=processes)


def decode_dequeue_answer(data: bytes, consumer: str) -> DequeueResult:
    """queue.dequeue's answer: messages, term, index, expires_at_ms."""
    a = _Answer("queue.dequeue", data)
    messages = []
    for _ in range(a.u8()):
        seq, priority, deliveries, enqueued_at_ms = a.u64(), a.u32(), a.u32(), a.i64()
        messages.append(
            Message(
                seq=seq,
                payload=a.bytes32(),
                priority=priority,
                deliveries=deliveries,
                enqueued_at_ms=enqueued_at_ms,
            )
        )
    term, index, expires_at_ms = a.u64(), a.u64(), a.i64()
    a.done()
    return DequeueResult(
        messages=messages, term=term, index=index, expires_at_ms=expires_at_ms, consumer=consumer
    )


def _refusals(a: _Answer, op: str) -> list[QueueRefusal]:
    refused = []
    for _ in range(a.u8()):
        seq = a.u64()
        code = a.u8()
        try:
            why = QueueRefusalReason(code)
        except ValueError:
            raise IncompleteResponseError(f"{op} answer has an unknown refusal {code}") from None
        refused.append(QueueRefusal(seq=seq, why=why, reason=_text(a, op, a.bytes8())))
    return refused


def decode_queue_report_answer(op: str, data: bytes) -> QueueReportResult:
    """queue.complete's and queue.fail's answer: applied, refused."""
    a = _Answer(op, data)
    applied = a.u32()
    refused = _refusals(a, op)
    a.done()
    return QueueReportResult(applied=applied, refused=refused)


def decode_queue_touch_answer(data: bytes) -> QueueTouchResult:
    """queue.touch's answer: expires_at_ms, applied, refused."""
    op = "queue.touch"
    a = _Answer(op, data)
    expires_at_ms, applied = a.i64(), a.u32()
    refused = _refusals(a, op)
    a.done()
    return QueueTouchResult(expires_at_ms=expires_at_ms, applied=applied, refused=refused)
