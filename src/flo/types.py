"""Flo SDK Types

Core types, constants, and data classes for the Flo client SDK.
"""

import struct
from dataclasses import dataclass
from enum import IntEnum
from typing import Optional

# =============================================================================
# Protocol Constants
# =============================================================================

MAGIC: int = 0x004F4C46  # "FLO\0" in little-endian
VERSION: int = 0x01
HEADER_SIZE: int = 32

# Size limits (for client-side validation)
MAX_NAMESPACE_SIZE: int = 255
MAX_KEY_SIZE: int = 64 * 1024  # 64 KB
MAX_VALUE_SIZE: int = 16 * 1024 * 1024  # 16 MB practical limit


# =============================================================================
# Enums
# =============================================================================


class OpCode(IntEnum):
    """Operation codes for Flo protocol requests.

    Three-layer layout: Infra(0x000-0x0FF), Data(0x100-0x2FF), Compute(0x300-0x3FF)
    """

    # ── System (0x000 – 0x00F) ──
    PING = 0x000
    PONG = 0x001
    ERROR_RESPONSE = 0x002
    AUTH = 0x003
    SET_DURABILITY = 0x004
    OK = 0x005

    # ── Namespace (0x010 – 0x02F) ──
    NAMESPACE_CREATE = 0x010
    NAMESPACE_DELETE = 0x011
    NAMESPACE_LIST = 0x012
    NAMESPACE_INFO = 0x013
    NAMESPACE_CONFIG_SET = 0x014
    NAMESPACE_CONFIG_GET = 0x015
    NAMESPACE_CREATE_RESPONSE = 0x020
    NAMESPACE_DELETE_RESPONSE = 0x021
    NAMESPACE_LIST_RESPONSE = 0x022
    NAMESPACE_INFO_RESPONSE = 0x023
    NAMESPACE_CONFIG_SET_RESPONSE = 0x024
    NAMESPACE_CONFIG_GET_RESPONSE = 0x025

    # ── Cluster (0x030 – 0x04F) ──
    CLUSTER_STATUS = 0x030
    CLUSTER_MEMBERS = 0x031
    CLUSTER_JOIN = 0x032
    CLUSTER_LEAVE = 0x033
    CLUSTER_TRANSFER_LEADER = 0x034
    CLUSTER_ADD_NODE = 0x035
    CLUSTER_REMOVE_NODE = 0x036
    CLUSTER_STATUS_RESPONSE = 0x040
    CLUSTER_MEMBERS_RESPONSE = 0x041
    CLUSTER_JOIN_RESPONSE = 0x042

    # ── KV (0x100 – 0x12F) ──
    KV_PUT = 0x100
    KV_GET = 0x101
    KV_MGET = 0x102
    KV_DELETE = 0x103
    KV_SCAN = 0x104
    KV_HISTORY = 0x105
    KV_GET_RESPONSE = 0x106
    KV_MGET_RESPONSE = 0x107
    KV_PUT_RESPONSE = 0x108
    KV_SCAN_RESPONSE = 0x109
    KV_HISTORY_RESPONSE = 0x10A
    # KV Extended (atomic counters, JSON ops)
    KV_INCR = 0x10B
    KV_JSON_GET = 0x10C
    KV_JSON_SET = 0x10D
    KV_JSON_DEL = 0x10E
    # KV Per-Shard Transactions
    KV_BEGIN_TXN = 0x110
    KV_COMMIT_TXN = 0x111
    KV_ROLLBACK_TXN = 0x112
    # KV Extended (TTL lifecycle, exists)
    KV_TOUCH = 0x113
    KV_PERSIST = 0x114
    KV_EXISTS = 0x115
    KV_INCR_RESPONSE = 0x116
    KV_JSON_RESPONSE = 0x117
    KV_EXISTS_RESPONSE = 0x118
    KV_TXN_RESPONSE = 0x119

    # ── Streams (0x130 – 0x14F) ──
    STREAM_APPEND = 0x130
    STREAM_READ = 0x131
    STREAM_TRIM = 0x132
    STREAM_INFO = 0x133
    STREAM_APPEND_RESPONSE = 0x134
    STREAM_READ_RESPONSE = 0x135
    STREAM_EVENT = 0x136
    STREAM_SUBSCRIBE = 0x137
    STREAM_UNSUBSCRIBE = 0x138
    STREAM_SUBSCRIBED = 0x139
    STREAM_UNSUBSCRIBED = 0x13A
    STREAM_LIST = 0x13B
    STREAM_LIST_RESPONSE = 0x13C
    STREAM_CREATE = 0x13D
    STREAM_CREATE_RESPONSE = 0x13E
    STREAM_ALTER = 0x13F

    # ── Stream Consumer Groups (0x150 – 0x16F) ──
    STREAM_GROUP_CREATE = 0x150
    STREAM_GROUP_JOIN = 0x151
    STREAM_GROUP_LEAVE = 0x152
    STREAM_GROUP_READ = 0x153
    STREAM_GROUP_ACK = 0x154
    STREAM_GROUP_CLAIM = 0x155
    STREAM_GROUP_PENDING = 0x156
    STREAM_GROUP_CONFIGURE_SWEEPER = 0x157
    STREAM_GROUP_READ_RESPONSE = 0x158
    STREAM_GROUP_NACK = 0x159
    STREAM_GROUP_TOUCH = 0x15A
    STREAM_GROUP_INFO = 0x15B
    STREAM_GROUP_DELETE = 0x15C

    # ── Queues (0x170 – 0x19F) ──
    QUEUE_ENQUEUE = 0x170
    QUEUE_DEQUEUE = 0x171
    QUEUE_COMPLETE = 0x172
    QUEUE_EXTEND_LEASE = 0x173
    QUEUE_FAIL = 0x174
    QUEUE_FAIL_AUTO = 0x175
    QUEUE_DLQ_LIST = 0x176
    QUEUE_DLQ_DELETE = 0x177
    QUEUE_DLQ_REQUEUE = 0x178
    QUEUE_DLQ_STATS = 0x179
    QUEUE_PROMOTE_DUE = 0x17A
    QUEUE_STATS = 0x17B
    QUEUE_PEEK = 0x17C
    QUEUE_TOUCH = 0x17D
    QUEUE_BATCH_ENQUEUE = 0x17E
    QUEUE_PURGE = 0x17F
    QUEUE_ENQUEUE_RESPONSE = 0x190
    QUEUE_DEQUEUE_RESPONSE = 0x191
    QUEUE_DLQ_LIST_RESPONSE = 0x192
    QUEUE_STATS_RESPONSE = 0x193
    QUEUE_PEEK_RESPONSE = 0x194
    QUEUE_TOUCH_RESPONSE = 0x195
    QUEUE_BATCH_ENQUEUE_RESPONSE = 0x196
    QUEUE_PURGE_RESPONSE = 0x197
    QUEUE_LIST = 0x198
    QUEUE_LIST_RESPONSE = 0x199

    # ── Time-Series (0x1A0 – 0x1BF) ──
    TS_WRITE = 0x1A0
    TS_READ = 0x1A1
    TS_QUERY = 0x1A2
    TS_FLOQL = 0x1A3
    TS_LIST = 0x1A4
    TS_DELETE = 0x1A5
    TS_RETENTION = 0x1A6
    TS_WRITE_RESPONSE = 0x1A7
    TS_READ_RESPONSE = 0x1A8
    TS_QUERY_RESPONSE = 0x1A9
    TS_FLOQL_RESPONSE = 0x1AA
    TS_LIST_RESPONSE = 0x1AB
    TS_DELETE_RESPONSE = 0x1AC
    TS_RETENTION_RESPONSE = 0x1AD

    # ── Actions (0x300 – 0x31F) ──
    ACTION_REGISTER = 0x300
    ACTION_INVOKE = 0x301
    ACTION_STATUS = 0x302
    ACTION_LIST = 0x303
    ACTION_LIST_RUNS = 0x304
    ACTION_DELETE = 0x305
    ACTION_AWAIT = 0x306
    ACTION_COMPLETE = 0x307
    ACTION_FAIL = 0x308
    ACTION_TOUCH = 0x309
    ACTION_REGISTER_RESPONSE = 0x310
    ACTION_INVOKE_RESPONSE = 0x311
    ACTION_STATUS_RESPONSE = 0x312
    ACTION_LIST_RESPONSE = 0x313
    ACTION_LIST_RUNS_RESPONSE = 0x314
    ACTION_TASK_ASSIGNMENT = 0x315

    # ── Workers (0x320 – 0x33F) ──
    WORKER_REGISTER = 0x320
    WORKER_HEARTBEAT = 0x321
    WORKER_DEREGISTER = 0x322
    WORKER_LIST = 0x323
    WORKER_INFO = 0x324
    WORKER_DRAIN = 0x325
    WORKER_REGISTER_RESPONSE = 0x330
    WORKER_LIST_RESPONSE = 0x331
    WORKER_INFO_RESPONSE = 0x332
    WORKER_DRAIN_RESPONSE = 0x333

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
    WORKFLOW_CREATE_RESPONSE = 0x350
    WORKFLOW_START_RESPONSE = 0x351
    WORKFLOW_STATUS_RESPONSE = 0x352
    WORKFLOW_HISTORY_RESPONSE = 0x353
    WORKFLOW_LIST_RUNS_RESPONSE = 0x354
    WORKFLOW_GET_DEFINITION_RESPONSE = 0x355
    WORKFLOW_DISABLE_RESPONSE = 0x356
    WORKFLOW_ENABLE_RESPONSE = 0x357
    WORKFLOW_LIST_DEFINITIONS_RESPONSE = 0x358

    # ── Processing (0x360 – 0x37F) ──
    PROCESSING_SUBMIT = 0x360
    PROCESSING_STOP = 0x361
    PROCESSING_CANCEL = 0x362
    PROCESSING_STATUS = 0x363
    PROCESSING_LIST = 0x364
    PROCESSING_SAVEPOINT = 0x365
    PROCESSING_RESTORE = 0x366
    PROCESSING_RESCALE = 0x367
    PROCESSING_SUBMIT_RESPONSE = 0x370
    PROCESSING_STOP_RESPONSE = 0x371
    PROCESSING_CANCEL_RESPONSE = 0x372
    PROCESSING_STATUS_RESPONSE = 0x373
    PROCESSING_LIST_RESPONSE = 0x374
    PROCESSING_SAVEPOINT_RESPONSE = 0x375
    PROCESSING_RESTORE_RESPONSE = 0x376
    PROCESSING_RESCALE_RESPONSE = 0x377


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
        }
        return messages.get(self, "Unknown error")


class OptionTag(IntEnum):
    """Option tags for TLV-encoded operation parameters.

    Organized by feature area, matching proto.zig definitions.
    """

    # KV Options (0x01 - 0x0F)
    TTL_SECONDS = 0x01  # u64: Time-to-live in seconds (0 = no expiration)
    CAS_VERSION = 0x02  # u64: Expected version for compare-and-swap
    IF_NOT_EXISTS = 0x03  # void: Only set if key doesn't exist (NX)
    IF_EXISTS = 0x04  # void: Only set if key exists (XX)
    LIMIT = 0x05  # u32: Maximum number of results for scan/list operations
    KEYS_ONLY = 0x06  # u8: Skip values in scan response (0/1)
    CURSOR = 0x07  # bytes: Pagination cursor (ShardWalker format)
    ROUTING_KEY = 0x08  # string: Explicit routing key for shard co-location
    TXN_ID = 0x09  # u64: Transaction ID for per-shard transactions

    # Queue Options (0x10 - 0x1F)
    PRIORITY = 0x10  # u8: Message priority (0-255, higher = more urgent)
    DELAY_MS = 0x11  # u64: Delay before message becomes visible
    VISIBILITY_TIMEOUT_MS = 0x12  # u32: How long message is invisible after dequeue
    DEDUP_KEY = 0x13  # string: Deduplication key
    MAX_RETRIES = 0x14  # u8: Maximum retry attempts before DLQ
    COUNT = 0x15  # u32: Number of messages to dequeue
    SEND_TO_DLQ = 0x16  # u8: Whether to send failed messages to DLQ (0/1)
    BLOCK_MS = 0x17  # u32: Block timeout - wait until exists (0=forever)
    WAIT_MS = 0x18  # u32: Watch timeout - wait for NEXT version change (0=forever)

    # Stream Options (0x20 - 0x2F) - StreamID-native ONLY
    # All stream positioning uses StreamID (timestamp_ms + sequence)
    # 0x20 reserved
    STREAM_START = 0x21  # [16]u8: Start StreamID for reads (inclusive)
    STREAM_END = 0x22  # [16]u8: End StreamID for reads (inclusive)
    STREAM_TAIL = 0x23  # void: Flag indicating tail read (start from end)
    PARTITION = 0x24  # u32: Explicit partition index
    PARTITION_KEY = 0x25  # string: Key for partition routing
    MAX_AGE_SECONDS = 0x26  # u64: Maximum age in seconds for retention
    MAX_BYTES = 0x27  # u64: Maximum size in bytes for retention
    DRY_RUN = 0x28  # void: Flag to preview what would be deleted
    RETENTION_COUNT = 0x29  # u64: Retention policy - max event count
    RETENTION_AGE = 0x2A  # u64: Retention policy - max age in seconds
    RETENTION_BYTES = 0x2B  # u64: Retention policy - max bytes

    # Consumer Group Options (0x30 - 0x3F)
    ACK_TIMEOUT_MS = 0x30  # u32: Time before unacked message auto-redelivers
    MAX_DELIVER = 0x31  # u8: Max delivery attempts before DLQ (default: 10)
    SUBSCRIPTION_MODE = 0x32  # u8: 0=shared, 1=exclusive, 2=key_shared
    REDELIVERY_DELAY_MS = 0x33  # u32: Delay before NACK'd message becomes visible
    CONSUMER_TIMEOUT_MS = 0x34  # u32: Remove consumer from group if no activity
    NO_ACK = 0x35  # void: Auto-ack on delivery (at-most-once)
    IDLE_TIMEOUT_MS = 0x36  # u64: Min idle time for claiming stuck messages
    MAX_ACK_PENDING = 0x37  # u32: Max unacked messages per consumer
    EXTEND_ACK_MS = 0x38  # u32: Amount of time to extend ack deadline
    MAX_STANDBYS = 0x39  # u16: Max standby consumers in exclusive mode
    NUM_SLOTS = 0x3A  # u16: Number of hash slots for key_shared mode

    # Worker/Action Options (0x40 - 0x4F)
    WORKER_ID = 0x40  # string: Worker identifier
    EXTEND_MS = 0x41  # u32: Lease extension time in milliseconds
    MAX_TASKS = 0x42  # u32: Maximum tasks to return in batch
    RETRY = 0x43  # u8: Whether to retry on failure (0/1)

    # Workflow Options (0x50 - 0x5F)
    TIMEOUT_MS = 0x50  # u64: Workflow/activity timeout
    RETRY_POLICY = 0x51  # bytes: Serialized retry policy
    CORRELATION_ID = 0x52  # string: Correlation ID for tracing
    SUBSCRIPTION_ID = 0x53  # u64: Subscription ID for stream subscriptions

    # Time-Series Options (0x60 - 0x6F)
    TS_FROM_MS = 0x60  # i64: Start of time range (inclusive, unix ms)
    TS_TO_MS = 0x61  # i64: End of time range (inclusive, 0 = now)
    TS_WINDOW_MS = 0x62  # i64: Aggregation window size (ms)
    TS_AGGREGATION = 0x63  # string: Aggregation function name (avg, sum, count, min, max)
    TS_FIELD = 0x64  # string: Field name filter (empty = "value")
    TS_TAGS = 0x65  # string: Comma-separated tag filters "key=val,key2=val2"
    TS_PRECISION = 0x66  # u8: Timestamp precision (0=ns, 1=us, 2=ms, 3=s)
    TS_TIMESTAMP = 0x67  # i64: Explicit timestamp for write (0 = server-assigned)
    TS_RAW_TTL = 0x68  # string: Raw data TTL (e.g., "7d")
    TS_DOWNSAMPLE = 0x69  # string: Downsample rule (e.g., "1m:avg:30d")
    TS_BATCH = 0x6A  # void: Flag indicating batch/line-protocol mode


# =============================================================================
# Result Types
# =============================================================================


@dataclass
class KVEntry:
    """KV entry from scan results."""

    key: bytes
    value: bytes | None  # None if keys_only=True


@dataclass
class ScanResult:
    """Result of a KV scan operation."""

    entries: list[KVEntry]
    cursor: bytes | None  # None if no more pages
    has_more: bool


@dataclass
class VersionEntry:
    """KV version entry from history."""

    version: int
    timestamp: int
    value: bytes


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


@dataclass
class Message:
    """Queue message."""

    seq: int
    payload: bytes


@dataclass
class DequeueResult:
    """Result of a queue dequeue operation."""

    messages: list[Message]


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
    """Storage tier of a stream record."""

    HOT = 0
    PENDING = 1
    WARM = 2
    COLD = 3


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
class StreamInfo:
    """Stream metadata."""

    count: int
    bytes_size: int
    first_id: StreamID = None  # type: ignore[assignment]
    last_id: StreamID = None  # type: ignore[assignment]
    partition_count: int = 1


# =============================================================================
# Option Types (for operation parameters)
# =============================================================================


@dataclass
class GetOptions:
    """Options for KV get operations."""

    namespace: str | None = None
    block_ms: int | None = None  # Block until value available (0 = infinite)


@dataclass
class PutOptions:
    """Options for KV put operations."""

    namespace: str | None = None
    ttl_seconds: int | None = None
    cas_version: int | None = None
    if_not_exists: bool = False
    if_exists: bool = False


@dataclass
class DeleteOptions:
    """Options for KV delete operations."""

    namespace: str | None = None
    if_match: int | None = None  # CAS: only delete when current version equals if_match


@dataclass
class ScanOptions:
    """Options for KV scan operations."""

    namespace: str | None = None
    cursor: bytes | None = None
    limit: int | None = None
    keys_only: bool = False


@dataclass
class HistoryOptions:
    """Options for KV history operations."""

    namespace: str | None = None
    limit: int | None = None


@dataclass
class KVIncrOptions:
    """Options for KV incr operations."""

    namespace: str | None = None
    delta: int | None = None  # default +1 when None


@dataclass
class KVTouchOptions:
    """Options for KV touch / persist operations."""

    namespace: str | None = None
    if_match: int | None = None  # CAS: only succeed when current version equals if_match


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
    priority: int = 0
    delay_ms: int | None = None
    dedup_key: str | None = None


@dataclass
class DequeueOptions:
    """Options for queue dequeue operations."""

    namespace: str | None = None
    visibility_timeout_ms: int | None = None
    block_ms: int | None = None


@dataclass
class AckOptions:
    """Options for queue ack operations."""

    namespace: str | None = None


@dataclass
class NackOptions:
    """Options for queue nack operations."""

    namespace: str | None = None
    to_dlq: bool = False


@dataclass
class DlqListOptions:
    """Options for DLQ list operations."""

    namespace: str | None = None
    limit: int = 100


@dataclass
class DlqRequeueOptions:
    """Options for DLQ requeue operations."""

    namespace: str | None = None


@dataclass
class PeekOptions:
    """Options for queue peek operations."""

    namespace: str | None = None


@dataclass
class TouchOptions:
    """Options for queue touch (lease renewal) operations."""

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
    start: Optional["StreamID"] = None  # Start StreamID for reads (inclusive)
    end: Optional["StreamID"] = None  # End StreamID for reads (inclusive)
    tail: bool = False  # Start from end of stream (mutually exclusive with start)
    partition: int | None = None  # Explicit partition index
    count: int | None = None  # Maximum number of records to return
    block_ms: int | None = None  # Blocking timeout (0 = infinite)


@dataclass
class StreamTrimOptions:
    """Options for stream trim operations."""

    namespace: str | None = None
    max_len: int | None = None  # Retention policy - max event count
    max_age_seconds: int | None = None  # Retention policy - max age in seconds
    max_bytes: int | None = None  # Retention policy - max bytes
    dry_run: bool = False  # Preview what would be deleted


@dataclass
class StreamInfoOptions:
    """Options for stream info operations."""

    namespace: str | None = None


@dataclass
class StreamGroupJoinOptions:
    """Options for joining a consumer group."""

    namespace: str | None = None


@dataclass
class StreamGroupReadOptions:
    """Options for reading from a consumer group."""

    namespace: str | None = None
    count: int | None = None  # Max records to read
    block_ms: int | None = None  # Block waiting for records


@dataclass
class StreamGroupAckOptions:
    """Options for acknowledging records in a consumer group."""

    namespace: str | None = None
    consumer: str = ""  # Consumer ID (required for correct ack matching)


@dataclass
class StreamGroupNackOptions:
    """Options for negatively acknowledging records in a consumer group."""

    namespace: str | None = None
    consumer: str = ""  # Consumer ID (required for correct nack matching)
    redelivery_delay_ms: int | None = None  # Delay before message becomes visible again


# =============================================================================
# Action Types
# =============================================================================


class ActionType(IntEnum):
    """Type of action."""

    USER = 0  # External worker-based action


@dataclass
class ActionInfo:
    """Information about a registered action."""

    name: str
    action_type: ActionType
    timeout_ms: int
    max_retries: int
    description: str | None = None


@dataclass
class ActionRunStatus:
    """Status of an action run."""

    run_id: str
    status: str  # "pending", "running", "completed", "failed"
    result: bytes | None = None
    error: str | None = None


@dataclass
class ActionInvokeResult:
    """Result of invoking an action."""

    run_id: str


@dataclass
class ActionListResult:
    """Result of listing actions."""

    actions: list[ActionInfo]
    cursor: bytes | None = None


# =============================================================================
# Action Option Types
# =============================================================================


@dataclass
class ActionRegisterOptions:
    """Options for registering an action."""

    namespace: str | None = None
    timeout_ms: int = 30000
    max_retries: int = 3
    description: str | None = None


@dataclass
class ActionInvokeOptions:
    """Options for invoking an action."""

    namespace: str | None = None
    priority: int = 10
    idempotency_key: str | None = None


@dataclass
class ActionStatusOptions:
    """Options for getting action status."""

    namespace: str | None = None


@dataclass
class ActionListOptions:
    """Options for listing actions."""

    namespace: str | None = None
    limit: int = 100
    prefix: str | None = None


@dataclass
class ActionDeleteOptions:
    """Options for deleting an action."""

    namespace: str | None = None


# =============================================================================
# Worker Types
# =============================================================================


@dataclass
class TaskAssignment:
    """A task assigned to a worker."""

    task_id: str
    task_type: str
    payload: bytes
    created_at: int
    attempt: int
    caller_run_id: str = ""
    caller_workflow_name: str = ""


# Alias for backwards compatibility
WorkerTask = TaskAssignment


@dataclass
class WorkerAwaitResult:
    """Result of awaiting a task."""

    task: TaskAssignment | None = None  # None if no task available


@dataclass
class WorkerInfo:
    """Information about a registered worker."""

    worker_id: str
    task_types: list[str]


@dataclass
class WorkerListResult:
    """Result of listing workers."""

    workers: list[WorkerInfo]


# =============================================================================
# Worker Option Types
# =============================================================================


@dataclass
class WorkerRegisterOptions:
    """Options for registering a worker."""

    namespace: str | None = None
    concurrency: int = 10
    machine_id: str | None = None
    metadata: str | None = None


@dataclass
class WorkerAwaitOptions:
    """Options for awaiting a task."""

    namespace: str | None = None
    block_ms: int | None = None  # Block waiting for task (0 = infinite)
    timeout_ms: int | None = None


@dataclass
class WorkerTouchOptions:
    """Options for extending task lease."""

    namespace: str | None = None
    extend_ms: int = 30000


@dataclass
class WorkerCompleteOptions:
    """Options for completing a task."""

    namespace: str | None = None
    outcome: str = "success"


@dataclass
class WorkerFailOptions:
    """Options for failing a task."""

    namespace: str | None = None
    retry: bool = True  # Whether to retry the task


@dataclass
class WorkerListOptions:
    """Options for listing workers."""

    namespace: str | None = None
    limit: int = 100


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
    """Options for listing workflow runs."""

    namespace: str | None = None
    workflow_name: str | None = None
    status_filter: str | None = None
    cursor: bytes | None = None
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
    """Options for listing processing jobs."""

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
class ProcessingSavepointOptions:
    """Options for triggering a savepoint."""

    namespace: str | None = None


@dataclass
class ProcessingRestoreOptions:
    """Options for restoring from a savepoint."""

    namespace: str | None = None


@dataclass
class ProcessingRescaleOptions:
    """Options for rescaling a processing job."""

    namespace: str | None = None


@dataclass
class ProcessingSyncOptions:
    """Options for declarative processing sync."""

    namespace: str | None = None


@dataclass
class ProcessingStatusResult:
    """Status of a processing job."""

    job_id: str
    name: str
    status: str
    parallelism: int
    batch_size: int
    records_processed: int
    created_at: int


@dataclass
class ProcessingListEntry:
    """A single entry in a processing job list."""

    name: str
    job_id: str
    status: str
    parallelism: int
    created_at: int


@dataclass
class ProcessingSyncResult:
    """Result of a processing sync operation."""

    name: str
    job_id: str
