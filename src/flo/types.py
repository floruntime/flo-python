"""Flo SDK Types

Core types, constants, and data classes for the Flo client SDK.
"""

import struct
from dataclasses import dataclass
from enum import IntEnum
from typing import Any, Generic, Optional, TypeVar

# =============================================================================
# Protocol Constants
# =============================================================================

MAGIC: int = 0x004F4C46  # "FLO\0" in little-endian
VERSION: int = 0x03
# The hash of the op table this SDK was built for, written into every request
# header. A server built from another table refuses the request, and its
# answers carry its own hash, which this SDK refuses. CI's live tests run
# against the flo commit in .github/flo-ref.env: change both in one commit.
TABLE_HASH: int = 0xC688167B51FCC846
# Response header flag: a shard that can't apply its next entry (halted at
# it, or blocked reading it from disk) answered a read from its state just
# before that entry. The read succeeded.
RESPONSE_STALE: int = 0x01


class Reason(IntEnum):
    """Why the server refused a request. Mirrors the server's list; the table
    hash covers it, so a server can't send one this SDK doesn't know."""

    UNKNOWN_OP = 1
    MALFORMED = 2
    FIELD_TOO_LONG = 3
    OUT_OF_RANGE = 4
    LEADER_UNKNOWN = 5
    SHARD_BUSY = 6
    TABLE_MISMATCH = 7
    UNKNOWN_FLAG = 8
    INTERNAL = 9
    COMMITTED_NOT_APPLIED = 10
    ANSWER_LOST = 11
    # A refusal from an op family the server hasn't classified yet.
    UNCLASSIFIED = 12
    # What the request names doesn't exist.
    NOT_FOUND = 13
    # The key is in a prefix the server keeps for itself.
    RESERVED_KEY = 14
    # The write's cas_version isn't the key's version.
    CAS_MISMATCH = 15
    # The write's condition didn't hold.
    CONDITION_FAILED = 16
    # No open transaction has this id here.
    NO_SUCH_TRANSACTION = 17
    # The key doesn't route with the key the transaction was begun with: give
    # every key in a transaction the same {tag}.
    KEYS_NOT_COLOCATED = 18
    # The transaction holds as many ops or bytes as it may.
    TXN_TOO_LARGE = 19
    # The shard holds as many waiting reads as it may.
    TOO_MANY_WAITERS = 20
    # Two fields given together that don't go together.
    CONFLICTING_FIELDS = 21
    # The shard stopped taking writes until the node restarts.
    WRITES_STOPPED = 22
    # Too many writes wait for commit.
    TOO_MANY_PROPOSALS = 23
    # The key's value isn't what the op works on (a counter, a JSON document).
    WRONG_TYPE = 24
    # The shard holds as many open transactions as it may.
    TOO_MANY_TRANSACTIONS = 25
    # An action complete or fail for a run that has moved on (another worker
    # holds it, or it finished). It was dropped; resending it is refused too.
    STALE_REPORT = 26
    # An await from a worker that missed its heartbeats: a heartbeat (or, once
    # it's removed, registering again) lets it take work again.
    WORKER_UNHEALTHY = 27
    # An await from a draining worker: it takes no new work.
    WORKER_DRAINING = 28
    # The shard halted at an entry every replica fails the same way; it takes
    # no writes until an operator repairs it.
    SHARD_HALTED = 29
    # The op is served only on a node's admin socket.
    ADMIN_ONLY = 30
    # A processing job that still runs can't be removed: stop or cancel it first.
    JOB_RUNNING = 31
    # The node's memory budget is spent (status LIMIT).
    MEMORY_BUDGET = 32
    # The node holds as many actions as it registers (status LIMIT).
    TOO_MANY_ACTIONS = 33
    # The node holds as many workflow definitions as it keeps (status LIMIT).
    TOO_MANY_WORKFLOW_DEFINITIONS = 34
    # The node holds as many workflow runs as it keeps (status LIMIT).
    TOO_MANY_WORKFLOW_RUNS = 35


class Ran(IntEnum):
    """Whether a refused request took effect: retry logic reads this, not the
    message."""

    NO = 0
    # It took effect; don't resend it.
    YES = 1
    # It may still take effect; check before resending.
    UNKNOWN = 2


HEADER_SIZE: int = 32
# The most an answer frame holds, header included; a server never sends more.
MAX_ANSWER_BYTES: int = 256 * 1024

# Size limits (for client-side validation)
MAX_NAMESPACE_SIZE: int = 255
MAX_KEY_SIZE: int = 64 * 1024  # 64 KB
MAX_VALUE_SIZE: int = 16 * 1024 * 1024  # 16 MB practical limit
# Longest blocking wait (block_ms) the server accepts; checked client-side so
# it fails before the round trip.
MAX_BLOCK_MS: int = 300_000  # 5 minutes
# Longest a KV get may wait (wait_ms).
MAX_WAIT_MS: int = 300_000
# Longest value a KV put takes.
MAX_KV_VALUE_SIZE: int = 257_916
# A worker always long-polls: block_ms=0 ("don't wait") would turn its poll
# loop into a busy spin against the server, so a worker reads 0 (or unset) as this.
_DEFAULT_WORKER_BLOCK_MS: int = 30_000


# =============================================================================
# Enums
# =============================================================================


class OpCode(IntEnum):
    """Operation codes for Flo protocol requests.

    Three-layer layout: Infra(0x000-0x0FF), Data(0x100-0x2FF), Compute(0x300-0x3FF)
    """

    # ── System (0x000 – 0x00F) ──
    PING = 0x000

    # ── Namespace (0x010 – 0x02F) ──
    NAMESPACE_CREATE = 0x010
    NAMESPACE_DELETE = 0x011
    NAMESPACE_LIST = 0x012
    NAMESPACE_INFO = 0x013
    # Sent by one shard to another, never by a client.
    NAMESPACE_ENSURE = 0x016

    # ── Cluster (0x030 – 0x04F) ──
    CLUSTER_STATUS = 0x030
    CLUSTER_MEMBERS = 0x031
    CLUSTER_PROMOTE = 0x032
    CLUSTER_REMOVE = 0x033
    # Served only on a node's admin socket.
    CLUSTER_REPAIR_SKIP = 0x034

    # ── KV (0x100 – 0x12F) ──
    KV_PUT = 0x100
    KV_GET = 0x101
    KV_MGET = 0x102
    KV_DELETE = 0x103
    KV_SCAN = 0x104
    KV_HISTORY = 0x105
    # KV Extended (atomic counters, JSON ops)
    KV_INCR = 0x10B
    KV_JSON_GET = 0x10C
    KV_JSON_SET = 0x10D
    KV_JSON_DEL = 0x10E
    # KV Per-Shard Transactions
    KV_BEGIN_TXN = 0x110
    KV_COMMIT_TXN = 0x111
    KV_ROLLBACK_TXN = 0x112
    # KV Extended (TTL, exists)
    KV_EXPIRE = 0x113
    KV_EXISTS = 0x115

    # ── Streams (0x130 – 0x14F) ──
    STREAM_APPEND = 0x130
    STREAM_READ = 0x131
    STREAM_TRIM = 0x132
    STREAM_INFO = 0x133
    STREAM_LIST = 0x13B
    STREAM_CREATE = 0x13D
    STREAM_ALTER = 0x13F
    STREAM_DELETE = 0x140

    # ── Stream Consumer Groups (0x150 – 0x16F) ──
    STREAM_GROUP_CREATE = 0x150
    STREAM_GROUP_READ = 0x153
    STREAM_GROUP_ACK = 0x154
    STREAM_GROUP_PENDING = 0x156
    STREAM_GROUP_NACK = 0x159
    STREAM_GROUP_TOUCH = 0x15A
    STREAM_GROUP_INFO = 0x15B
    STREAM_GROUP_DELETE = 0x15C
    STREAM_GROUP_LIST = 0x15D
    STREAM_GROUP_MEMBERS = 0x15E

    # ── Queues (0x170 – 0x19F) ──
    QUEUE_ENQUEUE = 0x170
    QUEUE_DLQ_LIST = 0x176
    QUEUE_DLQ_DELETE = 0x177
    QUEUE_DLQ_REQUEUE = 0x178
    QUEUE_STATS = 0x17B
    QUEUE_PURGE = 0x17F
    QUEUE_PEEK = 0x180
    QUEUE_DEQUEUE = 0x181
    QUEUE_COMPLETE = 0x182
    QUEUE_FAIL = 0x183
    QUEUE_TOUCH = 0x184
    QUEUE_LIST = 0x198

    # ── Time-Series (0x1A0 – 0x1BF) ──
    TS_WRITE = 0x1A0
    TS_READ = 0x1A1
    TS_QUERY = 0x1A2
    TS_FLOQL = 0x1A3
    TS_LIST = 0x1A4
    TS_DELETE = 0x1A5
    TS_RETENTION = 0x1A6
    TS_INFO = 0x1A7

    # ── Actions (0x300 – 0x31F) ──
    ACTION_REGISTER = 0x300
    ACTION_INVOKE = 0x301
    ACTION_STATUS = 0x302
    ACTION_LIST = 0x303
    ACTION_LIST_RUNS = 0x304
    ACTION_DELETE = 0x305
    ACTION_AWAIT = 0x30A
    ACTION_COMPLETE = 0x30B
    ACTION_FAIL = 0x30C
    ACTION_TOUCH = 0x30D
    ACTION_INFO = 0x30F

    # ── Workers (0x320 – 0x33F) ──
    WORKER_REGISTER = 0x320
    WORKER_HEARTBEAT = 0x321
    WORKER_DEREGISTER = 0x322
    WORKER_LIST = 0x323
    WORKER_INFO = 0x324
    WORKER_DRAIN = 0x325

    # ── Workflows (0x340 – 0x35F) ──
    WORKFLOW_CREATE = 0x340
    WORKFLOW_START = 0x341
    WORKFLOW_SIGNAL = 0x342
    WORKFLOW_CANCEL = 0x343
    WORKFLOW_STATUS = 0x344
    WORKFLOW_HISTORY = 0x345
    WORKFLOW_LIST_RUNS = 0x346
    WORKFLOW_GET_DEFINITION = 0x347
    WORKFLOW_DISABLE = 0x348
    WORKFLOW_ENABLE = 0x349
    WORKFLOW_LIST_DEFINITIONS = 0x34A

    # ── Processing (0x360 – 0x37F) ──
    PROCESSING_SUBMIT = 0x360
    PROCESSING_STOP = 0x361
    PROCESSING_CANCEL = 0x362
    PROCESSING_STATUS = 0x363
    PROCESSING_LIST = 0x364
    PROCESSING_REMOVE = 0x365


class StatusCode(IntEnum):
    """Status codes for Flo protocol responses."""

    OK = 0
    ERROR_GENERIC = 1
    NOT_FOUND = 2
    BAD_REQUEST = 3
    CROSS_CORE_TRANSACTION = 4
    NO_ACTIVE_TRANSACTION = 5
    GROUP_LOCKED = 6
    UNAUTHORIZED = 7
    CONFLICT = 8
    INTERNAL_ERROR = 9
    OVERLOADED = 10
    RATE_LIMITED = 11
    UNAVAILABLE = 12
    # The write committed but hasn't applied: its shard halted at or before
    # it. Resending it would write it twice, or halt again.
    OUTCOME_PENDING = 13
    # The namespace the request names doesn't exist; NOT_FOUND means a
    # missing key.
    NO_NAMESPACE = 14
    # The node is at one of its limits; the reason names which. It
    # never ran, and retrying won't clear it.
    LIMIT = 15

    @classmethod
    def _missing_(cls, value: object) -> "StatusCode | None":
        # A status this SDK doesn't know still has a body on the wire; failing
        # here would abandon it unread, so it becomes a pseudo-member instead.
        if not isinstance(value, int) or not 0 <= value <= 255:
            return None
        member = int.__new__(cls, value)
        member._name_ = f"UNKNOWN_{value}"
        member._value_ = value
        return member

    def __reduce_ex__(self, proto: object) -> tuple[type["StatusCode"], tuple[int]]:
        # Rebuilt from its value, not its name: a pseudo-member such as
        # UNKNOWN_200 has no name to look up, so pickle and copy would fail.
        return self.__class__, (int(self),)

    def message(self) -> str:
        """Get human-readable error message."""
        messages = {
            StatusCode.OK: "OK",
            StatusCode.ERROR_GENERIC: "Generic error",
            StatusCode.NOT_FOUND: "Not found",
            StatusCode.BAD_REQUEST: "Bad request",
            StatusCode.CROSS_CORE_TRANSACTION: "Cross-core transaction not supported",
            StatusCode.NO_ACTIVE_TRANSACTION: "No active transaction",
            StatusCode.GROUP_LOCKED: "Consumer group is locked",
            StatusCode.UNAUTHORIZED: "Unauthorized",
            StatusCode.CONFLICT: "Conflict",
            StatusCode.INTERNAL_ERROR: "Internal server error",
            StatusCode.OVERLOADED: "Server overloaded",
            StatusCode.RATE_LIMITED: "Request rate limit exceeded",
            StatusCode.UNAVAILABLE: (
                "Unavailable: no leader or the shard isn't taking writes; retry"
            ),
            StatusCode.OUTCOME_PENDING: (
                "Committed, not applied: its shard halted; don't resend it unchanged"
            ),
            StatusCode.NO_NAMESPACE: "Namespace not found",
            StatusCode.LIMIT: "At a limit: delete something, or raise the limit",
        }
        return messages.get(self, f"Unknown status {int(self)}")


class OptionTag(IntEnum):
    """Option tags for TLV-encoded operation parameters.

    Organized by feature area, matching proto.zig definitions.
    """

    # General Options (0x01 - 0x0F)
    LIMIT = 0x05  # u32: Maximum number of results for list operations

    # Queue Options (0x10 - 0x1F)
    PRIORITY = 0x10  # u8: Message priority (0-255, lower is taken first)
    DELAY_MS = 0x11  # u64: How long before an enqueued message is ready (at most a day)
    COUNT = 0x15  # u32: Most records a stream read returns
    BLOCK_MS = 0x17  # u32: Block timeout - wait for data (0=don't wait, max 300000)

    # Stream Options (0x20 - 0x2F) - StreamID-native ONLY
    # All stream positioning uses StreamID (timestamp_ms + sequence)
    # 0x20 reserved
    STREAM_START = 0x21  # [16]u8: Read records after this StreamID (exclusive)
    STREAM_END = 0x22  # [16]u8: End StreamID for reads (inclusive)
    STREAM_TAIL = 0x23  # void: Flag indicating tail read (start from end)
    PARTITION = 0x24  # u32: Explicit partition index
    PARTITION_KEY = 0x25  # string: Key for partition routing
    MAX_AGE_SECONDS = 0x26  # u64: Maximum age in seconds for retention
    DRY_RUN = 0x28  # void: Flag to preview what would be deleted
    FORCE = 0x2C  # void: Flag to delete a stream that still has records

    # Consumer Group Options (0x30 - 0x3F)
    ACK_TIMEOUT_MS = 0x30  # u32: A group's lease, 5 s to 12 h (default 30 s)
    MAX_DELIVER = 0x31  # u8: Nacks or expiries before a record is dead, 1..255 (default 5)
    DEAD = 0x32  # void: A nack makes its records dead now
    ERROR_TEXT = 0x33  # string: Why a nack's records failed

    # Time-Series Options (0x60 - 0x6F)
    TS_FROM_MS = 0x60  # i64: Start of time range (inclusive, unix ms)
    TS_TO_MS = 0x61  # i64: End of time range (inclusive, 0 = now)
    TS_WINDOW_MS = 0x62  # i64: Aggregation window size (ms)
    TS_AGGREGATION = 0x63  # string: Aggregation function name (avg, sum, count, min, max)
    TS_FIELD = 0x64  # string: Field name filter (empty = "value")
    TS_TAGS = 0x65  # string: Comma-separated tag filters "key=val,key2=val2"
    TS_TIMESTAMP = 0x67  # i64: Explicit timestamp for write (0 = server-assigned)
    TS_RAW_TTL = 0x68  # string: Raw data TTL (e.g., "7d")
    TS_DOWNSAMPLE = 0x69  # string: Downsample rule (e.g., "1m:avg:30d")


# =============================================================================
# Result Types
# =============================================================================


@dataclass
class ScanResult:
    """A page of a KV scan: the keys' names, and the cursor for the next page."""

    names: list[str]
    cursor: bytes | None  # None on the last page
    # Read from the state before an entry its shard can't apply yet (halted
    # at it, or blocked reading it from disk).
    stale: bool = False

    @property
    def has_more(self) -> bool:
        return self.cursor is not None


@dataclass
class VersionEntry:
    """One version of a key from KV history."""

    version: int
    at_ms: int  # When it was written, unix ms
    deleted: bool  # This version is a delete; value is empty
    value: bytes


@dataclass
class HistoryResult:
    """A page of a key's versions, newest first. ``more`` says older kept
    versions follow: ask again with ``before_version`` set to the last
    version here."""

    versions: list[VersionEntry]
    more: bool
    # Read from the state before an entry its shard can't apply yet (halted
    # at it, or blocked reading it from disk).
    stale: bool = False


@dataclass
class ExistsResult:
    """Whether a key exists, and its version (0 when absent, or for a write
    buffered in a transaction)."""

    exists: bool
    version: int
    # Read from the state before an entry its shard can't apply yet (halted
    # at it, or blocked reading it from disk).
    stale: bool = False

    def __bool__(self) -> bool:
        # So `if await kv.exists(key):` still reads whether the key exists.
        return self.exists


@dataclass
class IncrResult:
    """A KV incr's new counter value and the key's new version."""

    value: int
    version: int


@dataclass
class PutResult:
    """Result of a successful KV put.

    The ``version`` field is the new version assigned by the server, suitable
    for CAS on the next write via :class:`PutOptions.cas_version`.
    """

    version: int


@dataclass
class KVBeginResult:
    """Result of a successful KV transaction begin.

    ``txn_id`` is the server-assigned transaction handle. ``pinned_hash`` is
    the partition hash this transaction is bound to — every key written or
    read inside the transaction must hash to the same partition.
    """

    txn_id: int
    pinned_hash: int


@dataclass
class KVCommitResult:
    """Result of a successful KV transaction commit.

    ``commit_index`` is the Raft log index of the committed batch and
    ``op_count`` is the number of buffered operations applied atomically.
    """

    commit_index: int
    op_count: int


@dataclass
class GetResult:
    """Result of a KV get that found a key.

    ``kv.get`` returns ``None`` when the key is missing; check for ``None``
    before dereferencing.
    """

    value: bytes
    version: int
    # When the key's TTL runs out, unix ms; None when it has none.
    expires_at_ms: int | None = None
    # Read from the state before an entry its shard can't apply yet (halted
    # at it, or blocked reading it from disk).
    stale: bool = False


T = TypeVar("T")


@dataclass
class GetJsonResult(Generic[T]):
    """Result of a :meth:`KV.json_get` that found the key and path.

    Unlike :class:`GetResult`, ``value`` is the already-parsed JSON (the
    ``KV_JSON_GET`` wire body is always a JSON document, so it is decoded for
    you). ``version`` is the document's current version. ``kv.json_get``
    returns ``None`` when the key or path is missing.

    The value type defaults to ``Any``; the SDK does not validate the shape at
    runtime.
    """

    value: Any
    version: int
    # Read from the state before an entry its shard can't apply yet (halted
    # at it, or blocked reading it from disk).
    stale: bool = False


@dataclass
class MGetEntry:
    """One entry in a :meth:`KV.mget` response.

    ``found`` is ``False`` when the key did not exist; in that case ``value``
    is ``b''`` and ``version`` is ``0``.
    """

    key: str
    value: bytes
    version: int
    found: bool
    # Read from the state before an entry its shard can't apply yet (halted
    # at it, or blocked reading it from disk).
    stale: bool = False


@dataclass
class Message:
    """A dequeued message. ``deliveries`` counts its deliveries, this one
    included: above 1, an earlier lease ran out or was failed."""

    seq: int
    payload: bytes
    priority: int = 0
    deliveries: int = 1
    enqueued_at_ms: int = 0


@dataclass
class DequeueResult:
    """A dequeue's messages, leased to ``consumer`` until ``expires_at_ms``
    unless completed, failed or touched. ``term`` and ``index`` name the
    delivery: a complete, fail or touch names them back (pass this result).
    ``expires_at_ms`` is 0 when nothing was taken, or with ack."""

    messages: list[Message]
    term: int = 0
    index: int = 0
    expires_at_ms: int = 0
    consumer: str = ""


class QueueRefusalReason(IntEnum):
    """Why a queue complete, fail or touch didn't apply to a message, or a
    group ack, nack or touch to a record."""

    # For a complete, already done; otherwise the message is gone.
    GONE = 0
    DELIVERED_AGAIN = 1
    OTHER_HOLDER = 2
    EXPIRED = 3
    RELEASED = 4
    NOT_HELD = 5


@dataclass
class QueueRefusal:
    """A message a report didn't apply to, why, and the server's words for it."""

    seq: int
    why: QueueRefusalReason
    reason: str


@dataclass
class QueueReportResult:
    """What a complete or fail applied to: ``applied`` messages, and the
    ``refused`` rest. The report itself succeeded either way."""

    applied: int
    refused: list[QueueRefusal]

    @property
    def lost(self) -> list[QueueRefusal]:
        """For a complete, the refusals that aren't ``GONE`` (which means
        done): messages whose lease this consumer no longer holds, which go
        or have gone to another delivery."""
        return [r for r in self.refused if r.why != QueueRefusalReason.GONE]


@dataclass
class QueueTouchResult:
    """A touch's new expiry (when the lease runs out without another touch,
    Unix ms), and what it applied to."""

    expires_at_ms: int
    applied: int
    refused: list[QueueRefusal]


@dataclass
class PeekedMessage:
    """A ready message as a peek shows it, without taking it. ``payload`` is
    at most its first 4 KiB; ``size`` is its whole length."""

    seq: int
    priority: int
    attempts: int
    enqueued_at_ms: int
    size: int
    payload: bytes


@dataclass
class PeekResult:
    """A queue's next ready messages, in the order a dequeue takes them.
    ``more`` says more may follow."""

    messages: list[PeekedMessage]
    more: bool


@dataclass
class DeadLetter:
    """A message in a queue's dead letter queue. ``payload`` is at most its
    first 4 KiB; ``size`` is its whole length."""

    seq: int
    size: int
    payload: bytes
    enqueued_at_ms: int
    attempts: int
    priority: int
    moved_at_ms: int


@dataclass
class DlqPage:
    """A page of dead letters, oldest first. ``total`` counts them all; with
    ``has_more``, ask again with ``after=last_seq``."""

    messages: list[DeadLetter]
    total: int
    has_more: bool
    last_seq: int


# =============================================================================
# Stream Types
# =============================================================================


@dataclass
class StreamID:
    """Unique position in a stream (timestamp_ms + sequence).

    The StreamID format is: [timestamp_ms: u64][sequence: u64] = 16 bytes total.
    """

    timestamp_ms: int = 0
    sequence: int = 0

    def to_bytes(self) -> bytes:
        """Serialize the StreamID to 16 bytes (big-endian for lexicographic sorting)."""
        return struct.pack(">QQ", self.timestamp_ms, self.sequence)

    @classmethod
    def from_bytes(cls, data: bytes) -> "StreamID":
        """Parse a StreamID from 16 bytes (big-endian)."""
        if len(data) < 16:
            raise ValueError(f"Invalid StreamID: expected 16 bytes, got {len(data)}")
        ts, seq = struct.unpack(">QQ", data[:16])
        return cls(timestamp_ms=ts, sequence=seq)


class StorageTier(IntEnum):
    """Where a stream record's payload was read from. A tier this SDK doesn't
    know decodes to a pseudo-member ``UNKNOWN_<n>``, so a newer server's
    records still read."""

    HOT = 0  # the hot ring of recent appends
    WARM = 1  # the warm store of payload copies
    DISK = 2  # the durable log: a sealed segment, or the writer's buffer

    @classmethod
    def _missing_(cls, value: object) -> "StorageTier | None":
        if not isinstance(value, int) or not 0 <= value <= 255:
            return None
        member = int.__new__(cls, value)
        member._name_ = f"UNKNOWN_{value}"
        member._value_ = value
        return member

    def __reduce_ex__(self, proto: object) -> tuple[type["StorageTier"], tuple[int]]:
        return self.__class__, (int(self),)


@dataclass
class StreamRecord:
    """A record in a stream."""

    id: StreamID = None  # type: ignore[assignment]
    tier: StorageTier = StorageTier.HOT
    stream: str = ""
    payload: bytes = b""
    headers: dict[str, str] | None = None


@dataclass
class StreamAppendResult:
    """Result of appending to a stream."""

    id: StreamID = None  # type: ignore[assignment]


@dataclass
class StreamReadResult:
    """Result of reading from a stream."""

    records: list[StreamRecord]


@dataclass
class PendingEntry:
    """A consumer group record leased to ``consumer`` now: read and not yet
    acked, nacked or expired. ``delivery_count`` counts its deliveries, this
    one included."""

    id: StreamID = None  # type: ignore[assignment]
    consumer: str = ""
    delivery_count: int = 0


@dataclass
class GroupRecord(StreamRecord):
    """A record a group read leased. ``deliveries`` counts its deliveries,
    this one included: above 1, an earlier lease ran out or was nacked."""

    deliveries: int = 1


@dataclass
class GroupReadResult:
    """A group read's records, leased to ``consumer`` until ``expires_at_ms``
    (Unix ms) unless acked, nacked or touched. ``term`` and ``index`` name the
    delivery: an ack, nack or touch names them back (pass this result).
    ``expires_at_ms`` is 0 when nothing was read."""

    records: list[GroupRecord]
    term: int = 0
    index: int = 0
    expires_at_ms: int = 0
    consumer: str = ""


@dataclass
class GroupRefusal:
    """A record an ack, nack or touch didn't apply to, and why. ``GONE``
    means no such record in the group: acked already, trimmed, or the group
    is gone."""

    id: StreamID
    why: QueueRefusalReason


@dataclass
class GroupReportResult:
    """What an ack or nack applied to: ``applied`` records, and the
    ``refused`` rest. The report itself succeeded either way."""

    applied: int
    refused: list[GroupRefusal]

    @property
    def lost(self) -> list[GroupRefusal]:
        """For an ack, the refusals that aren't ``GONE`` (which means done):
        records this consumer no longer holds, which go or have gone to
        another delivery."""
        return [r for r in self.refused if r.why != QueueRefusalReason.GONE]


@dataclass
class GroupTouchResult:
    """A touch's new expiry (when the lease runs out without another touch,
    Unix ms), and what it applied to."""

    expires_at_ms: int
    applied: int
    refused: list[GroupRefusal]


@dataclass
class StreamGroupInfo:
    """A consumer group's counts. ``pending`` is ``ready`` plus ``leased``;
    ``holders`` the consumers holding records now. Records at or below a
    trim that cleanup hasn't taken yet are ``trimmed_pending`` alone."""

    pending: int
    holders: int
    created_at_ns: int
    ready: int
    leased: int
    dead: int
    trimmed_pending: int


@dataclass
class StreamInfo:
    """Stream metadata."""

    count: int
    bytes_size: int
    first_id: StreamID = None  # type: ignore[assignment]
    last_id: StreamID = None  # type: ignore[assignment]
    partition_count: int = 1


@dataclass
class StreamGroupEntry:
    """A consumer group, as a group list shows it: ``members`` the consumers
    holding records now, ``pending`` its records ready and leased, and the
    last stream record it read (``last_delivered_ms``/``_seq``)."""

    name: str
    members: int
    pending: int
    created_at_ms: int
    ack_timeout_ms: int
    max_deliver: int
    last_delivered_ms: int
    last_delivered_seq: int


@dataclass
class StreamGroupListResult:
    """A stream's consumer groups. ``more`` says it has more than fit."""

    groups: list[StreamGroupEntry]
    more: bool


class GroupMemberState(IntEnum):
    """A consumer group member's state: a member is a consumer holding
    records, always ACTIVE."""

    ACTIVE = 0
    LEAVING = 1


@dataclass
class StreamGroupMember:
    """A consumer holding a group's records: ``pending`` of them, held since
    ``joined_at_ms`` (its oldest lease), last given one at
    ``last_active_ms``."""

    id: str
    state: GroupMemberState
    pending: int
    joined_at_ms: int
    last_active_ms: int | None


@dataclass
class StreamGroupMembersResult:
    """A consumer group's members. ``more`` says it has more than fit."""

    members: list[StreamGroupMember]
    more: bool


# =============================================================================
# Option Types (for operation parameters)
# =============================================================================


class WaitFor(IntEnum):
    """What a KV get with wait_ms waits for."""

    # The key to exist; one that exists answers at once.
    EXISTS = 0
    # The key's next version.
    CHANGE = 1


class Condition(IntEnum):
    """When a KV put may write."""

    ALWAYS = 0
    IF_ABSENT = 1
    IF_PRESENT = 2


@dataclass
class GetOptions:
    """Options for KV get operations."""

    namespace: str | None = None
    # Wait up to this long for what wait_for names (0 = don't wait, max 300000).
    wait_ms: int = 0
    # The key to exist (the default), or its next version.
    wait_for: WaitFor = WaitFor.EXISTS


@dataclass
class PutOptions:
    """Options for KV put operations."""

    namespace: str | None = None
    ttl_ms: int | None = None  # Time-to-live in milliseconds (0 = no expiration)
    # When the write may happen.
    condition: Condition = Condition.ALWAYS
    # The key's version the write requires; 0 means it doesn't exist.
    cas_version: int | None = None


@dataclass
class DeleteOptions:
    """Options for KV delete operations."""

    namespace: str | None = None
    # Only delete when the key's version is this.
    cas_version: int | None = None


@dataclass
class ScanOptions:
    """Options for KV scan operations."""

    namespace: str | None = None
    # The cursor from the previous page; None for the first.
    cursor: bytes | None = None
    limit: int = 1024  # 1..1024 names per page


@dataclass
class HistoryOptions:
    """Options for KV history operations."""

    namespace: str | None = None
    limit: int = 10  # 1..65 versions, newest first
    # Only versions older than this one (the last version of the page
    # before); None starts from the newest.
    before_version: int | None = None


@dataclass
class KVIncrOptions:
    """Options for KV incr operations."""

    namespace: str | None = None
    delta: int = 1


@dataclass
class KVExpireOptions:
    """Options for KV expire operations."""

    namespace: str | None = None
    # Only succeed when the key's version is this.
    cas_version: int | None = None


@dataclass
class KVExistsOptions:
    """Options for KV exists operations."""

    namespace: str | None = None


@dataclass
class KVJsonOptions:
    """Options for KV JSON.* operations."""

    namespace: str | None = None


@dataclass
class KVMGetOptions:
    """Options for KV mget operations."""

    namespace: str | None = None


@dataclass
class EnqueueOptions:
    """Options for queue enqueue operations."""

    namespace: str | None = None
    priority: int = 0  # 0-255, lower is taken first
    delay_ms: int = 0  # How long before it's ready, at most a day


@dataclass
class DequeueOptions:
    """Options for queue dequeue operations.

    The messages are leased to ``consumer`` for ``lease_ms`` (5000 to
    43200000; None is the server's 30 s) and come back once it runs out,
    unless completed, failed or touched first. With ``ack`` they're completed
    as they're taken, a one-shot consume: if the leader fails in between,
    they come back once their lease runs out.
    """

    namespace: str | None = None
    block_ms: int | None = None  # Wait for messages (0 = don't wait, max 300000)
    consumer: str = ""
    lease_ms: int | None = None
    ack: bool = False


@dataclass
class PurgeOptions:
    """Options for a queue purge."""

    namespace: str | None = None


@dataclass
class QueueReportOptions:
    """Options for a queue complete or touch."""

    namespace: str | None = None


@dataclass
class QueueFailOptions:
    """Options for a queue fail: ready again after ``delay_ms`` (at most a
    day), or dead now with ``dead``, whatever attempts are left. ``error``
    (at most 1024 bytes) says why."""

    namespace: str | None = None
    delay_ms: int = 0
    dead: bool = False
    error: str = ""


@dataclass
class DlqListOptions:
    """Options for DLQ list operations: at most ``limit`` (1..100) dead
    letters after the one whose seq is ``after`` (None starts at the oldest)."""

    namespace: str | None = None
    limit: int = 100
    after: int | None = None


@dataclass
class PeekOptions:
    """Options for queue peek operations."""

    namespace: str | None = None


# =============================================================================
# Stream Option Types
# =============================================================================


@dataclass
class StreamAppendOptions:
    """Options for stream append operations."""

    namespace: str | None = None
    headers: dict[str, str] | None = None


@dataclass
class StreamReadOptions:
    """Options for stream read operations.

    Uses StreamID-native positioning (timestamp_ms + sequence).
    """

    namespace: str | None = None
    start: Optional["StreamID"] = None  # Read records after this StreamID (exclusive)
    end: Optional["StreamID"] = None  # End StreamID for reads (inclusive)
    tail: bool = False  # Start from end of stream (mutually exclusive with start)
    partition: int | None = None  # Explicit partition index
    count: int | None = None  # Maximum number of records to return
    block_ms: int | None = None  # Blocking timeout (0 = don't wait, max 300000)


@dataclass
class StreamTrimOptions:
    """Options for stream trim operations."""

    namespace: str | None = None
    before: Optional["StreamID"] = None  # Remove records up to and including this id
    max_len: int | None = None  # Keep only the newest N records (> 0)
    max_age_seconds: int | None = None  # Remove records older than this (> 0)
    dry_run: bool = False  # Report what would be removed, removing nothing


@dataclass
class StreamTrimResult:
    """What a trim removed, or with dry_run would remove."""

    removed: int  # Records removed
    first_seq: int  # Sequence of the first record left; meaningful only when records remain


@dataclass
class StreamDeleteOptions:
    """Options for stream delete operations."""

    namespace: str | None = None
    force: bool = False  # Delete even if the stream still has records


@dataclass
class StreamInfoOptions:
    """Options for stream info operations."""

    namespace: str | None = None


@dataclass
class StreamGroupCreateOptions:
    """A consumer group's settings, fixed when it is created. None takes the
    server's default: a 30 s lease and 5 attempts."""

    namespace: str | None = None
    # The lease: a record not acked or touched this long is read again.
    # 5000..43200000 (5 s to 12 h).
    ack_timeout_ms: int | None = None
    # A record nacked or expired this many times is dead. 1..255.
    max_deliver: int | None = None


@dataclass
class StreamGroupReadOptions:
    """Options for reading from a consumer group."""

    namespace: str | None = None
    count: int | None = None  # 1..100 records; None reads up to 100
    block_ms: int | None = None  # Block waiting for records (0 = don't wait, max 300000)


@dataclass
class StreamGroupAckOptions:
    """Options for an ack or touch of a group read's records."""

    namespace: str | None = None


@dataclass
class StreamGroupNackOptions:
    """Options for a nack: the records are read again after ``delay_ms``
    (at most a day), or are dead once their attempts are spent, or at once
    with ``dead``. ``error`` (at most 255 bytes: it travels as an option,
    whose length is one byte) says why."""

    namespace: str | None = None
    delay_ms: int = 0
    dead: bool = False
    error: str = ""


# =============================================================================
# Action Types
# =============================================================================


class ActionType(IntEnum):
    """Type of action."""

    USER = 0  # External worker-based action


class RunStatus(IntEnum):
    """An action run's status."""

    PENDING = 0
    RUNNING = 1
    COMPLETED = 2
    FAILED = 3
    CANCELLED = 4
    TIMED_OUT = 5


@dataclass
class ActionRunStatus:
    """Status of an action run. Times are ms since the epoch; ``attempt``
    counts from 1 once a worker has taken the run. ``output`` is a completed
    run's result, and ``error_message`` the last failure's message."""

    run_id: str
    status: RunStatus
    created_at_ms: int
    attempt: int
    started_at_ms: int | None = None
    completed_at_ms: int | None = None
    output: bytes | None = None
    error_message: str | None = None
    # The worker holding the run, or that last held it; None until one takes
    # it. Only the leader learns of a take at once.
    worker_id: str | None = None
    # The outcome a completed run's worker named; None for success.
    outcome: str | None = None


@dataclass
class ActionInvokeResult:
    """Result of invoking an action."""

    run_id: str


@dataclass
class ActionListEntry:
    """A registered action, as an action list shows it. ``timeout_ms`` is its
    lease; ``version`` counts its registrations."""

    name: str
    version: int
    owner: str
    timeout_ms: int
    max_attempts: int
    created_at_ms: int


@dataclass
class ActionListResult:
    """A page of registered actions. ``cursor`` gives the next page; None on
    the last."""

    actions: list[ActionListEntry]
    cursor: bytes | None = None

    @property
    def names(self) -> list[str]:
        return [a.name for a in self.actions]

    @property
    def has_more(self) -> bool:
        return self.cursor is not None


@dataclass
class ActionInfo:
    """An action's definition, its runs by status and how long completed runs
    took. ``ready`` is counted up to 100,000 (``ready_capped`` past that).
    With ``too_many_runs`` (its shard holds more than 10,000 of the
    namespace's runs) the other counts and the latency are None; the latency
    is None too until a run completes."""

    owner: str
    version: int
    timeout_ms: int
    max_attempts: int
    created_at_ms: int
    ready: int
    ready_capped: bool
    too_many_runs: bool
    running: int | None = None
    completed: int | None = None
    failed: int | None = None
    timed_out: int | None = None
    latency_count: int | None = None
    latency_avg_ms: int | None = None
    latency_p50_ms: int | None = None
    latency_p95_ms: int | None = None
    latency_max_ms: int | None = None


# =============================================================================
# Action Option Types
# =============================================================================


@dataclass
class ActionRegisterOptions:
    """Options for registering an action. Registering an action replaces
    the settings it had: the last registration wins."""

    namespace: str | None = None
    # The lease: how long a worker holds a run without completing, failing
    # or touching it before it's taken back. The server takes 5000 to
    # 43200000 ms; 0, the default here, takes the server's, 300000.
    timeout_ms: int = 0
    # Attempts in all, counting the first.
    max_attempts: int = 3
    owner: str | None = None

    def __post_init__(self) -> None:
        # Both go on the wire as u32.
        for name in ("timeout_ms", "max_attempts"):
            value = getattr(self, name)
            if not 0 <= value < 2**32:
                raise ValueError(f"{name} must be in [0, 2**32), not {value}")


@dataclass
class ActionInvokeOptions:
    """Options for invoking an action."""

    namespace: str | None = None
    # Required worker labels: only workers registered with every key/value
    # here receive the run.
    labels: dict[str, Any] | None = None


@dataclass
class ActionStatusOptions:
    """Options for getting action status."""

    namespace: str | None = None


@dataclass
class ActionListOptions:
    """Options for listing actions: at most ``limit`` (1..1024; 0 is the
    server's default) after ``cursor``, the last page's cursor (None starts)."""

    namespace: str | None = None
    limit: int = 100
    prefix: str | None = None
    cursor: bytes | None = None


@dataclass
class ActionDeleteOptions:
    """Options for deleting an action."""

    namespace: str | None = None


# =============================================================================
# Worker Types
# =============================================================================


@dataclass(frozen=True)
class TaskAssignment:
    """A task: one delivery of a run to one worker.

    ``run``, ``deliveries``, ``term`` and ``index`` name the delivery, and a
    complete, fail or touch names it back, with the action; a report naming
    any other delivery of the run is refused as stale. ``attempt`` is the
    run's failures so far, plus one; ``lease_ms`` how long the worker holds it
    without a touch.
    """

    run: str
    namespace: str
    deliveries: int
    term: int
    index: int
    action: str
    created_at_ms: int
    attempt: int
    lease_ms: int
    input: bytes
    caller_run: str | None = None
    caller_workflow: str | None = None

    @property
    def task_id(self) -> str:
        """The task's id as the CLI prints it: ``run:deliveries:term:index:action``."""
        return f"{self.run}:{self.deliveries}:{self.term}:{self.index}:{self.action}"


@dataclass
class WorkerAwaitResult:
    """Result of awaiting a task."""

    task: TaskAssignment | None = None  # None if no task available


class WorkerType(IntEnum):
    """What a worker runs."""

    ACTION = 0
    STREAM = 1


class WorkerStatus(IntEnum):
    """A registered worker's status."""

    ACTIVE = 0
    IDLE = 1
    DRAINING = 2
    UNHEALTHY = 3


@dataclass
class WorkerInfo:
    """A registered worker, as worker.list reports it. Times are ms since the epoch."""

    worker_id: str
    worker_type: WorkerType
    status: WorkerStatus
    current_load: int
    max_concurrency: int
    tasks_completed: int
    tasks_failed: int
    registered_at_ms: int
    last_heartbeat_ms: int
    machine_id: str | None = None


class ProcessKind(IntEnum):
    """What a process on a worker runs."""

    ACTION = 0
    STREAM_CONSUMER = 1


@dataclass
class WorkerProcess:
    """One process a worker registered, with its counts. Times are ms since the epoch."""

    name: str
    kind: ProcessKind
    run_count: int
    fail_count: int
    last_run_at_ms: int


@dataclass
class WorkerDetails:
    """A worker's record, as worker.info answers it: its row (as worker.list
    shows it), the metadata it registered with, and its processes."""

    worker: WorkerInfo
    metadata: bytes | None  # As registered; decode it if you know it is text
    processes: list[WorkerProcess]


@dataclass
class WorkerListResult:
    """A page of workers, in id order. ``cursor`` gives the next page; None on the last."""

    workers: list[WorkerInfo]
    cursor: bytes | None = None


# =============================================================================
# Worker Option Types
# =============================================================================


@dataclass
class WorkerRegisterOptions:
    """Options for registering a worker."""

    namespace: str | None = None
    worker_type: WorkerType = WorkerType.ACTION
    concurrency: int = 10
    machine_id: str | None = None
    metadata: str | None = None


@dataclass
class WorkerAwaitOptions:
    """Options for awaiting a task."""

    namespace: str | None = None
    block_ms: int | None = None  # Block waiting for task (None = 30000, 0 = don't wait, max 300000)
    # Up to 64 bytes: an await sent again with the same key gets the task the
    # first one got, while its lease runs, so a lost answer loses no task.
    request_key: bytes | None = None


@dataclass
class WorkerCompleteOptions:
    """Options for completing a task."""

    # The outcome a workflow step follows; "" is success.
    outcome: str = ""


@dataclass
class WorkerFailOptions:
    """Options for failing a task."""

    # Run it again while attempts are left; False fails the run now.
    retry: bool = True
    # Hand it back untried: no failure is counted.
    abandoned: bool = False
    # How long before it is ready again, at most a day.
    delay_ms: int = 0


@dataclass
class WorkerListOptions:
    """Options for listing workers: at most ``limit`` (1..1024) after ``cursor``,
    the last page's cursor (None starts)."""

    namespace: str | None = None
    limit: int = 100
    cursor: bytes | None = None
    # Only workers that run this action; None for every worker.
    action: str | None = None


# =============================================================================
# Workflow Types
# =============================================================================


@dataclass
class WorkflowCreateOptions:
    """Options for creating a workflow."""

    namespace: str | None = None


@dataclass
class WorkflowGetDefinitionOptions:
    """Options for getting a workflow definition."""

    namespace: str | None = None
    version: str | None = None


@dataclass
class WorkflowStartOptions:
    """Options for starting a workflow run."""

    namespace: str | None = None
    idempotency_key: str | None = None
    run_id: str | None = None
    version: str | None = None


@dataclass
class WorkflowStatusOptions:
    """Options for getting workflow status."""

    namespace: str | None = None


@dataclass
class WorkflowSignalOptions:
    """Options for sending a signal to a workflow."""

    namespace: str | None = None


@dataclass
class WorkflowCancelOptions:
    """Options for cancelling a workflow."""

    namespace: str | None = None


@dataclass
class WorkflowHistoryOptions:
    """Options for getting workflow history."""

    namespace: str | None = None
    limit: int = 100


@dataclass
class WorkflowListRunsOptions:
    """Options for listing workflow runs.

    ``workflow_name`` None lists every workflow's runs. ``status`` and
    ``search`` (a case-insensitive substring of the run's id, workflow, step,
    input or tags) narrow the list; None for any. ``cursor`` is the last
    page's, None to start. ``limit`` 0 asks for the server's default (100);
    the server sends at most 1024.
    """

    namespace: str | None = None
    workflow_name: str | None = None
    status: str | None = None
    cursor: bytes | None = None
    search: str | None = None
    limit: int = 100


@dataclass
class WorkflowListDefinitionsOptions:
    """Options for listing workflow definitions."""

    namespace: str | None = None
    limit: int = 100
    cursor: bytes | None = None


@dataclass
class WorkflowDisableOptions:
    """Options for disabling a workflow."""

    namespace: str | None = None


@dataclass
class WorkflowEnableOptions:
    """Options for enabling a workflow."""

    namespace: str | None = None


@dataclass
class WorkflowSyncOptions:
    """Options for syncing workflows."""

    namespace: str | None = None


@dataclass
class WorkflowSyncResult:
    """Result of a workflow sync operation."""

    name: str
    version: str
    description: str
    action: str  # "created", "updated", "unchanged"


@dataclass
class WorkflowDefinitionEntry:
    """A workflow definition as a definition list names it."""

    name: str
    version: str
    created_at: int
    enabled: bool


@dataclass
class WorkflowRunEntry:
    """A workflow run as a run list names it. ``created_at`` is when the run
    started, in milliseconds since the epoch."""

    run_id: str
    workflow: str
    status: str
    created_at: int


@dataclass
class WorkflowListRunsResult:
    """A page of workflow runs, newest first. ``cursor`` gives the next page;
    None on the last. A page may hold fewer than the limit and still have
    more."""

    runs: list[WorkflowRunEntry]
    cursor: bytes | None = None

    @property
    def has_more(self) -> bool:
        return self.cursor is not None


@dataclass
class WorkflowListDefinitionsResult:
    """A page of workflow definitions. ``cursor`` gives the next page; None on
    the last. A page may hold fewer than the limit and still have more."""

    definitions: list[WorkflowDefinitionEntry]
    cursor: bytes | None = None

    @property
    def has_more(self) -> bool:
        return self.cursor is not None


# =============================================================================
# Processing Types
# =============================================================================


@dataclass
class ProcessingSubmitOptions:
    """Options for submitting a processing job."""

    namespace: str | None = None


@dataclass
class ProcessingStatusOptions:
    """Options for getting processing job status."""

    namespace: str | None = None


@dataclass
class ProcessingListOptions:
    """Options for listing processing jobs: at most ``limit`` (1..1024; 0 is
    the server's default) after ``cursor``, the last page's cursor (None
    starts)."""

    namespace: str | None = None
    limit: int = 100
    cursor: bytes | None = None


@dataclass
class ProcessingStopOptions:
    """Options for stopping a processing job."""

    namespace: str | None = None


@dataclass
class ProcessingCancelOptions:
    """Options for cancelling a processing job."""

    namespace: str | None = None


@dataclass
class ProcessingRemoveOptions:
    """Options for removing a processing job."""

    namespace: str | None = None


@dataclass
class ProcessingSyncOptions:
    """Options for declarative processing sync."""

    namespace: str | None = None


@dataclass
class ProcessingRuntime:
    """A job's pipelines on the node that answered, summed. None of it is
    replicated: it starts again with the node or a new leader."""

    running: bool
    records_in: int
    records_out: int
    watermark_ms: int
    tick_avg_us: int
    tick_max_us: int
    uptime_ms: int  # Since the earliest of them started


@dataclass
class ProcessingStatusResult:
    """Status of a processing job."""

    job_id: str
    name: str
    status: str
    batch_size: int
    records_processed: int
    created_at: int
    # Why a failed job failed, or why a running one is held on this node;
    # None otherwise.
    failure_reason: str | None
    runtime: ProcessingRuntime
    yaml: bytes  # The submitted definition


@dataclass
class ProcessingListEntry:
    """A single entry in a processing job list."""

    name: str
    job_id: str
    status: str
    created_at: int
    batch_size: int
    records_processed: int


@dataclass
class ProcessingListResult:
    """A page of processing jobs. ``cursor`` gives the next page; None on the
    last."""

    jobs: list[ProcessingListEntry]
    cursor: bytes | None = None

    @property
    def has_more(self) -> bool:
        return self.cursor is not None


@dataclass
class ProcessingSyncResult:
    """Result of a processing sync operation."""

    name: str
    job_id: str
