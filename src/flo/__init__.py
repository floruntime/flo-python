"""Flo Python SDK

A Python client for the Flo distributed systems platform.

All primitives are accessed as attributes on a connected FloClient:

Example:
    import asyncio
    from flo import FloClient

    async def main():
        async with FloClient("localhost:9000", namespace="myapp") as client:
            # KV operations
            await client.kv.put("key", b"value")
            value = await client.kv.get("key")

            # Queue operations
            await client.queue.enqueue("tasks", b'{"task": "process"}')

            # Stream operations
            await client.stream.append("events", b'{"event": "click"}')

            # Action operations
            await client.action.invoke("process", b'{}')

            # Worker (created from client)
            worker = client.new_action_worker(concurrency=5)
            worker.register_action("my-action", handler)
            await worker.start()

    asyncio.run(main())
"""

from .client import FloClient
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
    NonRetryableError,
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
    is_connection_error,
)
from .processing import ProcessingOperations
from .types import (
    AckOptions,
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
    ProcessingCancelOptions,
    ProcessingListEntry,
    ProcessingListOptions,
    ProcessingRescaleOptions,
    ProcessingRestoreOptions,
    ProcessingSavepointOptions,
    ProcessingStatusOptions,
    ProcessingStatusResult,
    ProcessingStopOptions,
    ProcessingSubmitOptions,
    ProcessingSyncOptions,
    ProcessingSyncResult,
    PutOptions,
    ScanOptions,
    ScanResult,
    StatusCode,
    StorageTier,
    StreamAppendOptions,
    StreamAppendResult,
    StreamGroupAckOptions,
    StreamGroupJoinOptions,
    StreamGroupNackOptions,
    StreamGroupReadOptions,
    StreamID,
    StreamInfo,
    StreamInfoOptions,
    StreamReadOptions,
    StreamReadResult,
    StreamRecord,
    StreamTrimOptions,
    TaskAssignment,
    TouchOptions,
    VersionEntry,
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
    WorkflowCancelOptions,
    WorkflowCreateOptions,
    WorkflowDisableOptions,
    WorkflowEnableOptions,
    WorkflowGetDefinitionOptions,
    WorkflowHistoryOptions,
    WorkflowListDefinitionsOptions,
    WorkflowListRunsOptions,
    WorkflowSignalOptions,
    WorkflowStartOptions,
    WorkflowStatusOptions,
    WorkflowSyncOptions,
    WorkflowSyncResult,
)
from .worker import (
    ActionContext,
    ActionResult,
    ActionWorker,
    ActionWorkerOptions,
    StreamContext,
    StreamRecordHandler,
    StreamWorker,
    StreamWorkerOptions,
)
from .workflows import WorkflowOperations

__version__ = "0.1.0"

__all__ = [
    # Client
    "FloClient",
    # High-level Worker API
    "ActionWorker",
    "ActionWorkerOptions",
    "ActionContext",
    "ActionResult",
    "StreamWorker",
    "StreamWorkerOptions",
    "StreamContext",
    "StreamRecordHandler",
    "WorkflowOperations",
    "ProcessingOperations",
    # Exceptions
    "FloError",
    "NonRetryableError",
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
    "is_connection_error",
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
    "StreamGroupNackOptions",
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
    # Workflow types
    "WorkflowOperations",
    "WorkflowCreateOptions",
    "WorkflowGetDefinitionOptions",
    "WorkflowStartOptions",
    "WorkflowStatusOptions",
    "WorkflowSignalOptions",
    "WorkflowCancelOptions",
    "WorkflowHistoryOptions",
    "WorkflowListRunsOptions",
    "WorkflowListDefinitionsOptions",
    "WorkflowDisableOptions",
    "WorkflowEnableOptions",
    "WorkflowSyncOptions",
    "WorkflowSyncResult",
    # Processing types
    "ProcessingOperations",
    "ProcessingSubmitOptions",
    "ProcessingStatusOptions",
    "ProcessingListOptions",
    "ProcessingStopOptions",
    "ProcessingCancelOptions",
    "ProcessingSavepointOptions",
    "ProcessingRestoreOptions",
    "ProcessingRescaleOptions",
    "ProcessingSyncOptions",
    "ProcessingStatusResult",
    "ProcessingListEntry",
    "ProcessingSyncResult",
]
