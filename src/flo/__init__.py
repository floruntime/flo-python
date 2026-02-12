"""Flo Python SDK

A Python client for the Flo distributed systems platform.

Example:
    import asyncio
    from flo import FloClient

    async def main():
        async with FloClient("localhost:9000") as client:
            # KV operations
            await client.kv.put("key", b"value")
            value = await client.kv.get("key")
            print(f"Got: {value}")

            # Queue operations
            seq = await client.queue.enqueue("tasks", b'{"task": "process"}')
            result = await client.queue.dequeue("tasks", 10)
            for msg in result.messages:
                print(f"Message: {msg.payload}")
                await client.queue.ack("tasks", [msg.seq])

    asyncio.run(main())
"""

from .client import FloClient
from .worker import ActionContext, Worker, WorkerConfig
from .exceptions import (
    BadRequestError,
    ConflictError,
    ConnectionFailedError,
    FloError,
    GenericServerError,
    IncompleteResponseError,
    InternalServerError,
    InvalidChecksumError,
    InvalidEndpointError,
    InvalidMagicError,
    KeyTooLargeError,
    NamespaceTooLargeError,
    NotConnectedError,
    NotFoundError,
    OverloadedError,
    PayloadTooLargeError,
    ProtocolError,
    RateLimitedError,
    ServerError,
    UnauthorizedError,
    UnexpectedEofError,
    UnsupportedVersionError,
    ValidationError,
    ValueTooLargeError,
)
from .types import (
    # KV types
    AckOptions,
    DeleteOptions,
    DequeueOptions,
    DequeueResult,
    DlqListOptions,
    DlqRequeueOptions,
    EnqueueOptions,
    GetOptions,
    HistoryOptions,
    KVEntry,
    Message,
    NackOptions,
    OpCode,
    OptionTag,
    PeekOptions,
    PutOptions,
    ScanOptions,
    ScanResult,
    StatusCode,
    TouchOptions,
    VersionEntry,
    # Stream types
    StorageTier,
    StreamAppendOptions,
    StreamAppendResult,
    StreamGroupAckOptions,
    StreamGroupJoinOptions,
    StreamGroupReadOptions,
    StreamID,
    StreamInfo,
    StreamInfoOptions,
    StreamReadOptions,
    StreamReadResult,
    StreamRecord,
    StreamTrimOptions,
    # Action types
    ActionDeleteOptions,
    ActionInfo,
    ActionInvokeOptions,
    ActionInvokeResult,
    ActionListOptions,
    ActionListResult,
    ActionRegisterOptions,
    ActionRunStatus,
    ActionStatusOptions,
    ActionType,
    # Worker types
    TaskAssignment,
    WorkerAwaitOptions,
    WorkerAwaitResult,
    WorkerCompleteOptions,
    WorkerFailOptions,
    WorkerInfo,
    WorkerListOptions,
    WorkerListResult,
    WorkerRegisterOptions,
    WorkerTask,
    WorkerTouchOptions,
)

__version__ = "0.1.0"

__all__ = [
    # Client
    "FloClient",
    # High-level Worker API
    "Worker",
    "WorkerConfig",
    "ActionContext",
    # Exceptions
    "FloError",
    "NotConnectedError",
    "ConnectionFailedError",
    "InvalidEndpointError",
    "UnexpectedEofError",
    "ProtocolError",
    "InvalidMagicError",
    "UnsupportedVersionError",
    "InvalidChecksumError",
    "PayloadTooLargeError",
    "IncompleteResponseError",
    "ValidationError",
    "NamespaceTooLargeError",
    "KeyTooLargeError",
    "ValueTooLargeError",
    "ServerError",
    "NotFoundError",
    "BadRequestError",
    "ConflictError",
    "UnauthorizedError",
    "OverloadedError",
    "RateLimitedError",
    "InternalServerError",
    "GenericServerError",
    # Types
    "OpCode",
    "StatusCode",
    "OptionTag",
    # Result types
    "KVEntry",
    "ScanResult",
    "VersionEntry",
    "Message",
    "DequeueResult",
    # Options
    "GetOptions",
    "PutOptions",
    "DeleteOptions",
    "ScanOptions",
    "HistoryOptions",
    "EnqueueOptions",
    "DequeueOptions",
    "AckOptions",
    "NackOptions",
    "DlqListOptions",
    "DlqRequeueOptions",
    "PeekOptions",
    "TouchOptions",
    # Stream types
    "StreamID",
    "StorageTier",
    "StreamRecord",
    "StreamAppendResult",
    "StreamReadResult",
    "StreamInfo",
    # Stream options
    "StreamAppendOptions",
    "StreamReadOptions",
    "StreamTrimOptions",
    "StreamInfoOptions",
    "StreamGroupJoinOptions",
    "StreamGroupReadOptions",
    "StreamGroupAckOptions",
    # Action types
    "ActionType",
    "ActionInfo",
    "ActionRunStatus",
    "ActionInvokeResult",
    "ActionListResult",
    # Action options
    "ActionRegisterOptions",
    "ActionInvokeOptions",
    "ActionStatusOptions",
    "ActionListOptions",
    "ActionDeleteOptions",
    # Worker types
    "TaskAssignment",
    "WorkerTask",
    "WorkerAwaitResult",
    "WorkerInfo",
    "WorkerListResult",
    # Worker options
    "WorkerRegisterOptions",
    "WorkerAwaitOptions",
    "WorkerTouchOptions",
    "WorkerCompleteOptions",
    "WorkerFailOptions",
    "WorkerListOptions",
]
