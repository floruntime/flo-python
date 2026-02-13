"""Flo SDK Types

Core types, constants, and data classes for the Flo client SDK.
"""

from dataclasses import dataclass
from enum import IntEnum
from typing import Optional
import struct


# =============================================================================
# Protocol Constants
# =============================================================================

MAGIC: int = 0x004F4C46  # "FLO\0" in little-endian
VERSION: int = 0x01
HEADER_SIZE: int = 24

# Size limits (for client-side validation)
MAX_NAMESPACE_SIZE: int = 255
MAX_KEY_SIZE: int = 64 * 1024  # 64 KB
MAX_VALUE_SIZE: int = 16 * 1024 * 1024  # 16 MB practical limit


# =============================================================================
# Enums
# =============================================================================


class OpCode(IntEnum):
    """Operation codes for Flo protocol requests."""

    # System Operations (0x00 - 0x0F)
    PING = 0x00
    PONG = 0x01
    ERROR_RESPONSE = 0x02
    AUTH = 0x03
    SET_DURABILITY = 0x04
    OK = 0x05

    # Streams (0x10 - 0x1F)
    STREAM_APPEND = 0x10
    STREAM_READ = 0x11
    STREAM_TRIM = 0x12
    STREAM_INFO = 0x13
    STREAM_APPEND_RESPONSE = 0x14
    STREAM_READ_RESPONSE = 0x15
    STREAM_EVENT = 0x16
    STREAM_SUBSCRIBE = 0x17
    STREAM_UNSUBSCRIBE = 0x18
    STREAM_SUBSCRIBED = 0x19
    STREAM_UNSUBSCRIBED = 0x1A
    STREAM_LIST = 0x1B
    STREAM_LIST_RESPONSE = 0x1C
    STREAM_CREATE = 0x1D
    STREAM_CREATE_RESPONSE = 0x1E
    STREAM_ALTER = 0x1F

    # Stream Consumer Groups (0x20 - 0x2F)
    STREAM_GROUP_CREATE = 0x20
    STREAM_GROUP_JOIN = 0x21
    STREAM_GROUP_LEAVE = 0x22
    STREAM_GROUP_READ = 0x23
    STREAM_GROUP_ACK = 0x24
    STREAM_GROUP_CLAIM = 0x25
    STREAM_GROUP_PENDING = 0x26
    STREAM_GROUP_CONFIGURE_SWEEPER = 0x27
    STREAM_GROUP_READ_RESPONSE = 0x28
    STREAM_GROUP_NACK = 0x29
    STREAM_GROUP_TOUCH = 0x2A
    STREAM_GROUP_INFO = 0x2B
    STREAM_GROUP_DELETE = 0x2C

    # KV Operations (0x30 - 0x3F)
    KV_PUT = 0x30
    KV_GET = 0x31
    KV_DELETE = 0x32
    KV_SCAN = 0x33
    KV_HISTORY = 0x34
    KV_GET_RESPONSE = 0x35
    KV_PUT_RESPONSE = 0x36
    KV_SCAN_RESPONSE = 0x37
    KV_HISTORY_RESPONSE = 0x38

    # Transactions (0x39 - 0x3B)
    KV_BEGIN_TXN = 0x39
    KV_COMMIT_TXN = 0x3A
    KV_ROLLBACK_TXN = 0x3B

    # Snapshots (0x3C - 0x3F)
    KV_SNAPSHOT_CREATE = 0x3C
    KV_SNAPSHOT_GET = 0x3D
    KV_SNAPSHOT_RELEASE = 0x3E
    KV_SNAPSHOT_CREATE_RESPONSE = 0x3F

    # Queues (0x40 - 0x5F)
    QUEUE_ENQUEUE = 0x40
    QUEUE_DEQUEUE = 0x41
    QUEUE_COMPLETE = 0x42
    QUEUE_EXTEND_LEASE = 0x43
    QUEUE_FAIL = 0x44
    QUEUE_FAIL_AUTO = 0x45
    QUEUE_DLQ_LIST = 0x46
    QUEUE_DLQ_DELETE = 0x47
    QUEUE_DLQ_REQUEUE = 0x48
    QUEUE_DLQ_STATS = 0x49
    QUEUE_PROMOTE_DUE = 0x4A
    QUEUE_STATS = 0x4B
    QUEUE_PEEK = 0x4C
    QUEUE_TOUCH = 0x4D
    QUEUE_BATCH_ENQUEUE = 0x4E
    QUEUE_PURGE = 0x4F

    # Queue responses (0x50 - 0x5F)
    QUEUE_ENQUEUE_RESPONSE = 0x50
    QUEUE_DEQUEUE_RESPONSE = 0x51
    QUEUE_DLQ_LIST_RESPONSE = 0x52
    QUEUE_STATS_RESPONSE = 0x53
    QUEUE_PEEK_RESPONSE = 0x54
    QUEUE_TOUCH_RESPONSE = 0x55
    QUEUE_BATCH_ENQUEUE_RESPONSE = 0x56
    QUEUE_PURGE_RESPONSE = 0x57

    # Actions (0x60 - 0x68)
    ACTION_REGISTER = 0x60
    ACTION_INVOKE = 0x61
    ACTION_STATUS = 0x62
    ACTION_LIST = 0x63
    ACTION_DELETE = 0x64
    ACTION_REGISTER_RESPONSE = 0x65
    ACTION_INVOKE_RESPONSE = 0x66
    ACTION_STATUS_RESPONSE = 0x67
    ACTION_LIST_RESPONSE = 0x68

    # Workers (0x69 - 0x7F)
    WORKER_REGISTER = 0x69
    WORKER_TOUCH = 0x6A
    WORKER_AWAIT = 0x6B
    WORKER_COMPLETE = 0x6C
    WORKER_FAIL = 0x6D
    WORKER_LIST = 0x6E
    WORKER_REGISTER_RESPONSE = 0x70
    WORKER_TASK_ASSIGNMENT = 0x71
    WORKER_LIST_RESPONSE = 0x72

    # Workflows (0x80 - 0x91)
    WORKFLOW_CREATE = 0x80
    WORKFLOW_START = 0x81
    WORKFLOW_SIGNAL = 0x82
    WORKFLOW_CANCEL = 0x83
    WORKFLOW_STATUS = 0x84
    WORKFLOW_HISTORY = 0x85
    WORKFLOW_LIST_RUNS = 0x86
    WORKFLOW_GET_DEFINITION = 0x87
    WORKFLOW_CREATE_RESPONSE = 0x88
    WORKFLOW_START_RESPONSE = 0x89
    WORKFLOW_STATUS_RESPONSE = 0x8A
    WORKFLOW_HISTORY_RESPONSE = 0x8B
    WORKFLOW_LIST_RUNS_RESPONSE = 0x8C
    WORKFLOW_GET_DEFINITION_RESPONSE = 0x8D
    WORKFLOW_DISABLE = 0x8E
    WORKFLOW_ENABLE = 0x8F
    WORKFLOW_DISABLE_RESPONSE = 0x90
    WORKFLOW_ENABLE_RESPONSE = 0x91

    # Cluster Management (0xA0 - 0xAF)
    CLUSTER_STATUS = 0xA0
    CLUSTER_MEMBERS = 0xA1
    CLUSTER_JOIN = 0xA2
    CLUSTER_LEAVE = 0xA3
    CLUSTER_TRANSFER_LEADER = 0xA4
    CLUSTER_ADD_NODE = 0xA5
    CLUSTER_REMOVE_NODE = 0xA6
    CLUSTER_STATUS_RESPONSE = 0xA8
    CLUSTER_MEMBERS_RESPONSE = 0xA9
    CLUSTER_JOIN_RESPONSE = 0xAA

    # Namespace Management (0xB0 - 0xBF)
    NAMESPACE_CREATE = 0xB0
    NAMESPACE_DELETE = 0xB1
    NAMESPACE_LIST = 0xB2
    NAMESPACE_INFO = 0xB3
    NAMESPACE_CREATE_RESPONSE = 0xB4
    NAMESPACE_DELETE_RESPONSE = 0xB5
    NAMESPACE_LIST_RESPONSE = 0xB6
    NAMESPACE_INFO_RESPONSE = 0xB7

    # Processing / Stream Processing (0xC0 - 0xD1)
    PROCESSING_SUBMIT = 0xC0
    PROCESSING_STOP = 0xC1
    PROCESSING_CANCEL = 0xC2
    PROCESSING_STATUS = 0xC3
    PROCESSING_LIST = 0xC4
    PROCESSING_SAVEPOINT = 0xC6
    PROCESSING_RESTORE = 0xC7
    PROCESSING_RESCALE = 0xC8
    PROCESSING_SUBMIT_RESPONSE = 0xC9
    PROCESSING_STOP_RESPONSE = 0xCA
    PROCESSING_CANCEL_RESPONSE = 0xCB
    PROCESSING_STATUS_RESPONSE = 0xCC
    PROCESSING_LIST_RESPONSE = 0xCD
    PROCESSING_SAVEPOINT_RESPONSE = 0xCF
    PROCESSING_RESTORE_RESPONSE = 0xD0
    PROCESSING_RESCALE_RESPONSE = 0xD1


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
    TTL_SECONDS = 0x01   # u64: Time-to-live in seconds (0 = no expiration)
    CAS_VERSION = 0x02   # u64: Expected version for compare-and-swap
    IF_NOT_EXISTS = 0x03 # void: Only set if key doesn't exist (NX)
    IF_EXISTS = 0x04     # void: Only set if key exists (XX)
    LIMIT = 0x05         # u32: Maximum number of results for scan/list operations
    KEYS_ONLY = 0x06     # u8: Skip values in scan response (0/1)
    CURSOR = 0x07        # bytes: Pagination cursor (ShardWalker format)

    # Queue Options (0x10 - 0x1F)
    PRIORITY = 0x10            # u8: Message priority (0-255, higher = more urgent)
    DELAY_MS = 0x11            # u64: Delay before message becomes visible
    VISIBILITY_TIMEOUT_MS = 0x12  # u32: How long message is invisible after dequeue
    DEDUP_KEY = 0x13           # string: Deduplication key
    MAX_RETRIES = 0x14         # u8: Maximum retry attempts before DLQ
    COUNT = 0x15               # u32: Number of messages to dequeue
    SEND_TO_DLQ = 0x16         # u8: Whether to send failed messages to DLQ (0/1)
    BLOCK_MS = 0x17            # u32: Block timeout - wait until exists (0=forever)
    WAIT_MS = 0x18             # u32: Watch timeout - wait for NEXT version change (0=forever)

    # Stream Options (0x20 - 0x2F) - StreamID-native ONLY
    # All stream positioning uses StreamID (timestamp_ms + sequence) - no legacy offset/timestamp modes
    # 0x20 reserved
    STREAM_START = 0x21      # [16]u8: Start StreamID for reads (inclusive)
    STREAM_END = 0x22        # [16]u8: End StreamID for reads (inclusive)
    STREAM_TAIL = 0x23       # void: Flag indicating tail read (start from end)
    PARTITION = 0x24         # u32: Explicit partition index
    PARTITION_KEY = 0x25     # string: Key for partition routing
    MAX_AGE_SECONDS = 0x26   # u64: Maximum age in seconds for retention
    MAX_BYTES = 0x27         # u64: Maximum size in bytes for retention
    DRY_RUN = 0x28           # void: Flag to preview what would be deleted
    RETENTION_COUNT = 0x29   # u64: Retention policy - max event count
    RETENTION_AGE = 0x2A     # u64: Retention policy - max age in seconds
    RETENTION_BYTES = 0x2B   # u64: Retention policy - max bytes

    # Consumer Group Options (0x30 - 0x3F)
    ACK_TIMEOUT_MS = 0x30      # u32: Time before unacked message auto-redelivers
    MAX_DELIVER = 0x31         # u8: Max delivery attempts before DLQ (default: 10)
    SUBSCRIPTION_MODE = 0x32   # u8: 0=shared, 1=exclusive, 2=key_shared
    REDELIVERY_DELAY_MS = 0x33 # u32: Delay before NACK'd message becomes visible
    CONSUMER_TIMEOUT_MS = 0x34 # u32: Remove consumer from group if no activity
    NO_ACK = 0x35              # void: Auto-ack on delivery (at-most-once)
    IDLE_TIMEOUT_MS = 0x36     # u64: Min idle time for claiming stuck messages
    MAX_ACK_PENDING = 0x37     # u32: Max unacked messages per consumer
    EXTEND_ACK_MS = 0x38       # u32: Amount of time to extend ack deadline
    MAX_STANDBYS = 0x39        # u16: Max standby consumers in exclusive mode
    NUM_SLOTS = 0x3A           # u16: Number of hash slots for key_shared mode

    # Worker/Action Options (0x40 - 0x4F)
    WORKER_ID = 0x40  # string: Worker identifier
    EXTEND_MS = 0x41  # u32: Lease extension time in milliseconds
    MAX_TASKS = 0x42  # u32: Maximum tasks to return in batch
    RETRY = 0x43      # u8: Whether to retry on failure (0/1)

    # Workflow Options (0x50 - 0x5F)
    TIMEOUT_MS = 0x50      # u64: Workflow/activity timeout
    RETRY_POLICY = 0x51    # bytes: Serialized retry policy
    CORRELATION_ID = 0x52  # string: Correlation ID for tracing
    SUBSCRIPTION_ID = 0x53 # u64: Subscription ID for stream subscriptions


# =============================================================================
# Result Types
# =============================================================================


@dataclass
class KVEntry:
    """KV entry from scan results."""

    key: bytes
    value: Optional[bytes]  # None if keys_only=True


@dataclass
class ScanResult:
    """Result of a KV scan operation."""

    entries: list[KVEntry]
    cursor: Optional[bytes]  # None if no more pages
    has_more: bool


@dataclass
class VersionEntry:
    """KV version entry from history."""

    version: int
    timestamp: int
    value: bytes


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
        """Serialize the StreamID to 16 bytes (little-endian)."""
        return struct.pack("<QQ", self.timestamp_ms, self.sequence)

    @classmethod
    def from_bytes(cls, data: bytes) -> "StreamID":
        """Parse a StreamID from 16 bytes (little-endian)."""
        if len(data) < 16:
            raise ValueError(f"Invalid StreamID: expected 16 bytes, got {len(data)}")
        ts, seq = struct.unpack("<QQ", data[:16])
        return cls(timestamp_ms=ts, sequence=seq)

    @classmethod
    def from_sequence(cls, seq: int) -> "StreamID":
        """Create a StreamID with just a sequence number.

        Used for backwards compatibility with offset-based reads.
        """
        return cls(timestamp_ms=0, sequence=seq)


class StorageTier(IntEnum):
    """Storage tier of a stream record."""

    HOT = 0
    PENDING = 1
    WARM = 2
    COLD = 3


@dataclass
class StreamRecord:
    """A record in a stream."""

    sequence: int
    timestamp_ms: int
    tier: StorageTier = StorageTier.HOT
    payload: bytes = b""
    headers: Optional[dict[str, str]] = None


@dataclass
class StreamAppendResult:
    """Result of appending to a stream."""

    sequence: int
    timestamp_ms: int


@dataclass
class StreamReadResult:
    """Result of reading from a stream."""

    records: list[StreamRecord]


@dataclass
class StreamInfo:
    """Stream metadata."""

    first_seq: int
    last_seq: int
    count: int
    bytes_size: int


# =============================================================================
# Option Types (for operation parameters)
# =============================================================================


@dataclass
class GetOptions:
    """Options for KV get operations."""

    namespace: Optional[str] = None
    block_ms: Optional[int] = None  # Block until value available (0 = infinite)


@dataclass
class PutOptions:
    """Options for KV put operations."""

    namespace: Optional[str] = None
    ttl_seconds: Optional[int] = None
    cas_version: Optional[int] = None
    if_not_exists: bool = False
    if_exists: bool = False


@dataclass
class DeleteOptions:
    """Options for KV delete operations."""

    namespace: Optional[str] = None


@dataclass
class ScanOptions:
    """Options for KV scan operations."""

    namespace: Optional[str] = None
    cursor: Optional[bytes] = None
    limit: Optional[int] = None
    keys_only: bool = False


@dataclass
class HistoryOptions:
    """Options for KV history operations."""

    namespace: Optional[str] = None
    limit: Optional[int] = None


@dataclass
class EnqueueOptions:
    """Options for queue enqueue operations."""

    namespace: Optional[str] = None
    priority: int = 0
    delay_ms: Optional[int] = None
    dedup_key: Optional[str] = None


@dataclass
class DequeueOptions:
    """Options for queue dequeue operations."""

    namespace: Optional[str] = None
    visibility_timeout_ms: Optional[int] = None
    block_ms: Optional[int] = None


@dataclass
class AckOptions:
    """Options for queue ack operations."""

    namespace: Optional[str] = None


@dataclass
class NackOptions:
    """Options for queue nack operations."""

    namespace: Optional[str] = None
    to_dlq: bool = False


@dataclass
class DlqListOptions:
    """Options for DLQ list operations."""

    namespace: Optional[str] = None
    limit: int = 100


@dataclass
class DlqRequeueOptions:
    """Options for DLQ requeue operations."""

    namespace: Optional[str] = None


@dataclass
class PeekOptions:
    """Options for queue peek operations."""

    namespace: Optional[str] = None


@dataclass
class TouchOptions:
    """Options for queue touch (lease renewal) operations."""

    namespace: Optional[str] = None


# =============================================================================
# Stream Option Types
# =============================================================================


@dataclass
class StreamAppendOptions:
    """Options for stream append operations."""

    namespace: Optional[str] = None
    headers: Optional[dict[str, str]] = None


@dataclass
class StreamReadOptions:
    """Options for stream read operations.

    Uses StreamID-native positioning (timestamp_ms + sequence).
    """

    namespace: Optional[str] = None
    start: Optional["StreamID"] = None     # Start StreamID for reads (inclusive)
    end: Optional["StreamID"] = None       # End StreamID for reads (inclusive)
    tail: bool = False                     # Start from end of stream (mutually exclusive with start)
    partition: Optional[int] = None        # Explicit partition index
    count: Optional[int] = None            # Maximum number of records to return
    block_ms: Optional[int] = None         # Blocking timeout (0 = infinite)


@dataclass
class StreamTrimOptions:
    """Options for stream trim operations."""

    namespace: Optional[str] = None
    max_len: Optional[int] = None          # Retention policy - max event count
    max_age_seconds: Optional[int] = None  # Retention policy - max age in seconds
    max_bytes: Optional[int] = None        # Retention policy - max bytes
    dry_run: bool = False                  # Preview what would be deleted


@dataclass
class StreamInfoOptions:
    """Options for stream info operations."""

    namespace: Optional[str] = None


@dataclass
class StreamGroupJoinOptions:
    """Options for joining a consumer group."""

    namespace: Optional[str] = None


@dataclass
class StreamGroupReadOptions:
    """Options for reading from a consumer group."""

    namespace: Optional[str] = None
    count: Optional[int] = None  # Max records to read
    block_ms: Optional[int] = None  # Block waiting for records


@dataclass
class StreamGroupAckOptions:
    """Options for acknowledging records in a consumer group."""

    namespace: Optional[str] = None


# =============================================================================
# Action Types
# =============================================================================


class ActionType(IntEnum):
    """Type of action."""

    USER = 0  # External worker-based action
    WASM = 1  # WebAssembly action


@dataclass
class ActionInfo:
    """Information about a registered action."""

    name: str
    action_type: ActionType
    timeout_ms: int
    max_retries: int
    description: Optional[str] = None


@dataclass
class ActionRunStatus:
    """Status of an action run."""

    run_id: str
    status: str  # "pending", "running", "completed", "failed"
    result: Optional[bytes] = None
    error: Optional[str] = None


@dataclass
class ActionInvokeResult:
    """Result of invoking an action."""

    run_id: str


@dataclass
class ActionListResult:
    """Result of listing actions."""

    actions: list[ActionInfo]
    cursor: Optional[bytes] = None


# =============================================================================
# Action Option Types
# =============================================================================


@dataclass
class ActionRegisterOptions:
    """Options for registering an action."""

    namespace: Optional[str] = None
    timeout_ms: int = 30000
    max_retries: int = 3
    description: Optional[str] = None


@dataclass
class ActionInvokeOptions:
    """Options for invoking an action."""

    namespace: Optional[str] = None
    priority: int = 10
    idempotency_key: Optional[str] = None


@dataclass
class ActionStatusOptions:
    """Options for getting action status."""

    namespace: Optional[str] = None


@dataclass
class ActionListOptions:
    """Options for listing actions."""

    namespace: Optional[str] = None
    limit: int = 100
    prefix: Optional[str] = None


@dataclass
class ActionDeleteOptions:
    """Options for deleting an action."""

    namespace: Optional[str] = None


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


# Alias for backwards compatibility
WorkerTask = TaskAssignment


@dataclass
class WorkerAwaitResult:
    """Result of awaiting a task."""

    task: Optional[TaskAssignment] = None  # None if no task available


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

    namespace: Optional[str] = None


@dataclass
class WorkerAwaitOptions:
    """Options for awaiting a task."""

    namespace: Optional[str] = None
    block_ms: Optional[int] = None  # Block waiting for task (0 = infinite)
    timeout_ms: Optional[int] = None


@dataclass
class WorkerTouchOptions:
    """Options for extending task lease."""

    namespace: Optional[str] = None
    extend_ms: int = 30000


@dataclass
class WorkerCompleteOptions:
    """Options for completing a task."""

    namespace: Optional[str] = None


@dataclass
class WorkerFailOptions:
    """Options for failing a task."""

    namespace: Optional[str] = None
    retry: bool = True  # Whether to retry the task


@dataclass
class WorkerListOptions:
    """Options for listing workers."""

    namespace: Optional[str] = None
    limit: int = 100
