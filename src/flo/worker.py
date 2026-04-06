"""Flo High-Level Worker API

Provides ActionWorker for executing actions and StreamWorker for
processing stream records via consumer groups.

Example::

    from flo import FloClient, ActionContext

    async def process_order(ctx: ActionContext) -> bytes:
        order = ctx.json()
        # Process the order...
        return ctx.to_bytes({"status": "completed"})

    async def main():
        async with FloClient("localhost:3000", namespace="myapp") as client:
            worker = client.new_action_worker(concurrency=5)
            worker.register_action("process-order", process_order)
            async with worker:
                await worker.start()
"""

import asyncio
import contextlib
import json
import logging
import secrets
import socket
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from .client import FloClient
from .exceptions import NonRetryableError, is_connection_error
from .types import (
    ActionType,
    StreamGroupAckOptions,
    StreamGroupNackOptions,
    StreamGroupReadOptions,
    StreamID,
    StreamRecord,
    TaskAssignment,
    WorkerAwaitOptions,
    WorkerTouchOptions,
)

logger = logging.getLogger("flo.worker")


class ActionResult:
    """Represents the result of an action with a named outcome.

    Use ``ActionContext.result()`` to create instances.

    Attributes:
        outcome: Named outcome (e.g. "approved", "rejected").
        data: Result data as bytes.
    """

    __slots__ = ("outcome", "data")

    def __init__(self, outcome: str, data: bytes):
        self.outcome = outcome
        self.data = data


# Type alias for action handlers — can return bytes, dict, or ActionResult
ActionHandler = Callable[["ActionContext"], Awaitable[bytes | dict[str, Any] | ActionResult]]


@dataclass
class ActionWorkerOptions:
    """Configuration for a Flo action worker.

    Endpoint and namespace are inherited from the parent FloClient.
    """

    worker_id: str = ""
    concurrency: int = 10
    action_timeout: float = 300.0  # 5 minutes
    block_ms: int = 30000


@dataclass
class ActionContext:
    """Context passed to action handlers.

    Provides access to task information and helper methods for
    parsing input and formatting output.
    """

    task_id: str
    action_name: str
    payload: bytes
    attempt: int
    created_at: int
    namespace: str
    _worker: "ActionWorker" = field(repr=False)
    _cancel_event: asyncio.Event = field(default_factory=asyncio.Event, repr=False)

    def input(self) -> bytes:
        """Get the raw input bytes."""
        return self.payload

    def json(self) -> Any:
        """Parse input as JSON and return the result."""
        if not self.payload:
            raise ValueError("No input data")
        return json.loads(self.payload.decode("utf-8"))

    def into(self, cls: type) -> Any:
        """Parse input as JSON and instantiate the given class.

        Args:
            cls: A class that accepts keyword arguments matching the JSON structure.

        Returns:
            An instance of the class populated with the JSON data.
        """
        data = self.json()
        if isinstance(data, dict):
            return cls(**data)
        return cls(data)

    @staticmethod
    def to_bytes(value: Any) -> bytes:
        """Serialize a value to JSON bytes."""
        return json.dumps(value).encode("utf-8")

    async def touch(self, extend_ms: int = 30000) -> None:
        """Extend the lease on this task.

        Use this for long-running tasks to prevent timeout.

        Args:
            extend_ms: How long to extend the lease in milliseconds.
        """
        await self._worker._touch_task(self.action_name, self.task_id, extend_ms)

    @property
    def cancelled(self) -> bool:
        """Check if the task has been cancelled."""
        return self._cancel_event.is_set()

    async def check_cancelled(self) -> None:
        """Check if cancelled and raise asyncio.CancelledError if so."""
        if self._cancel_event.is_set():
            raise asyncio.CancelledError("Task was cancelled")

    def result(self, outcome: str, value: Any = None) -> ActionResult:
        """Create an ActionResult with a named outcome.

        Args:
            outcome: Named outcome (e.g. "approved", "rejected").
            value: Result value — dict/list is JSON-encoded, bytes passed through.

        Returns:
            ActionResult to return from the handler.
        """
        if isinstance(value, bytes):
            data = value
        elif value is not None:
            data = json.dumps(value).encode("utf-8")
        else:
            data = b""
        return ActionResult(outcome=outcome, data=data)


class ActionWorker:
    """High-level Flo worker for executing actions.

    Created from a FloClient via ``client.new_action_worker()``.

    Example:
        async with FloClient("localhost:3000", namespace="myapp") as client:
            worker = client.new_action_worker(concurrency=5)

            @worker.action("process-order")
            async def process_order(ctx: ActionContext) -> bytes:
                order = ctx.json()
                return ctx.to_bytes({"status": "completed"})

            await worker.start()
    """

    def __init__(
        self,
        parent_client: "FloClient",
        *,
        worker_id: str | None = None,
        concurrency: int = 10,
        action_timeout: float = 300.0,
        block_ms: int = 30000,
    ):
        """Initialize a Flo worker from a connected client.

        Args:
            parent_client: The parent FloClient whose endpoint and namespace are used.
            worker_id: Unique worker identifier (auto-generated if not provided).
            concurrency: Maximum number of concurrent actions.
            action_timeout: Timeout for action handlers in seconds.
            block_ms: Timeout for blocking dequeue in milliseconds.
        """
        self._parent_client = parent_client
        self.config = ActionWorkerOptions(
            worker_id=worker_id or self._generate_worker_id(),
            concurrency=concurrency,
            action_timeout=action_timeout,
            block_ms=block_ms,
        )

        self._client: FloClient | None = None
        self._result_client: FloClient | None = None
        self._handlers: dict[str, ActionHandler] = {}
        self._running = False
        self._stop_event = asyncio.Event()
        self._tasks: set[asyncio.Task[None]] = set()
        self._semaphore: asyncio.Semaphore | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None

    @staticmethod
    def _generate_worker_id() -> str:
        """Generate a unique worker ID."""
        try:
            hostname = socket.gethostname()
        except Exception:
            hostname = "unknown"
        return f"{hostname}-{secrets.token_hex(4)}"

    def action(self, name: str) -> Callable[[ActionHandler], ActionHandler]:
        """Decorator to register an action handler.

        Args:
            name: The action name to register.

        Returns:
            Decorator function.

        Example:
            @worker.action("process-order")
            async def process_order(ctx: ActionContext) -> bytes:
                return ctx.to_bytes({"status": "ok"})
        """

        def decorator(handler: ActionHandler) -> ActionHandler:
            self.register_action(name, handler)
            return handler

        return decorator

    def register_action(self, name: str, handler: ActionHandler) -> None:
        """Register an action handler.

        Args:
            name: The action name.
            handler: Async function that handles the action.

        Raises:
            ValueError: If action is already registered.
        """
        if name in self._handlers:
            raise ValueError(f"Action '{name}' is already registered")
        self._handlers[name] = handler
        logger.info(f"Registered action: {name}")

    async def start(self) -> None:
        """Start the worker and begin processing actions.

        This method blocks until stop() is called or an error occurs.

        Raises:
            ValueError: If no handlers are registered.
            ConnectionError: If connection to server fails.
        """
        if not self._handlers:
            raise ValueError("No action handlers registered")

        logger.info(
            f"Starting Flo worker (id={self.config.worker_id}, "
            f"namespace={self._parent_client.namespace}, concurrency={self.config.concurrency})"
        )

        # Create a dedicated connection using the parent client's endpoint and namespace.
        # Timeout must accommodate block_ms + action_timeout so blocking reads
        # (ACTION_AWAIT with block_ms) don't get killed by socket-level timeout.
        worker_timeout_ms = max(
            self.config.block_ms + 5000,
            int(self.config.action_timeout * 1000),
        )
        self._client = FloClient(
            self._parent_client._endpoint,
            namespace=self._parent_client.namespace,
            debug=self._parent_client._debug,
            timeout_ms=worker_timeout_ms,
        )
        await self._client.connect()

        # Create a second connection for sending Complete/Fail results.
        # The polling connection holds its lock during blocking Await calls
        # (up to block_ms), so a separate connection prevents contention.
        self._result_client = FloClient(
            self._parent_client._endpoint,
            namespace=self._parent_client.namespace,
            debug=self._parent_client._debug,
            timeout_ms=worker_timeout_ms,
        )
        await self._result_client.connect()

        try:
            # Register actions with the server
            action_names = list(self._handlers.keys())
            for action_name in action_names:
                await self._client.action.register(action_name, ActionType.USER)
                logger.debug(f"Registered action with server: {action_name}")

            # Register worker
            from .types import WorkerRegisterOptions

            await self._client.worker.register(
                self.config.worker_id,
                action_names,
                WorkerRegisterOptions(
                    concurrency=self.config.concurrency,
                    machine_id=socket.gethostname(),
                ),
            )
            logger.info(f"Worker registered with {len(action_names)} actions")

            # Initialize concurrency control
            self._semaphore = asyncio.Semaphore(self.config.concurrency)
            self._running = True
            self._stop_event.clear()

            # Start heartbeat loop
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

            # Main polling loop
            await self._poll_loop(action_names)

        finally:
            # Cancel heartbeat
            if self._heartbeat_task is not None:
                self._heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._heartbeat_task
                self._heartbeat_task = None

            # Wait for running tasks
            if self._tasks:
                logger.info(f"Waiting for {len(self._tasks)} tasks to complete...")
                await asyncio.gather(*self._tasks, return_exceptions=True)

            if self._result_client:
                await self._result_client.close()
                self._result_client = None
            await self._client.close()
            self._client = None
            self._running = False
            logger.info("Worker stopped")

    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats to keep the worker registration alive."""
        assert self._client is not None
        while self._running and not self._stop_event.is_set():
            try:
                await asyncio.sleep(30)
                if not self._running:
                    break
                current_load = len(self._tasks)
                await self._client.worker.heartbeat(
                    self.config.worker_id,
                    current_load=current_load,
                )
                logger.debug(f"Heartbeat sent (load={current_load})")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Heartbeat failed: {e}")

    async def _poll_loop(self, action_names: list[str]) -> None:
        """Main polling loop for tasks."""
        assert self._client is not None
        assert self._semaphore is not None
        while self._running and not self._stop_event.is_set():
            try:
                # Wait for semaphore slot
                await self._semaphore.acquire()

                # Check if we should stop
                if self._stop_event.is_set():
                    self._semaphore.release()
                    break

                # Await task from server
                result = await self._client.worker.await_task(
                    self.config.worker_id,
                    action_names,
                    WorkerAwaitOptions(block_ms=self.config.block_ms),
                )

                if result.task is None:
                    # No task available, release semaphore and continue
                    self._semaphore.release()
                    continue

                # Execute task in background
                task = asyncio.create_task(self._execute_task(result.task))
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._semaphore.release()
                if is_connection_error(e):
                    logger.warning("Connection lost, reconnecting...")
                    try:
                        await self._client.reconnect()
                        # Also reconnect result client
                        if self._result_client is not None:
                            try:
                                await self._result_client.reconnect()
                            except Exception as rc_err:
                                logger.warning(f"Failed to reconnect result client: {rc_err}")
                        # Re-register worker after reconnect
                        try:
                            from .types import WorkerRegisterOptions

                            await self._client.worker.register(
                                self.config.worker_id,
                                action_names,
                                WorkerRegisterOptions(
                                    concurrency=self.config.concurrency,
                                    machine_id=socket.gethostname(),
                                ),
                            )
                        except Exception as reg_err:
                            logger.warning(f"Failed to re-register worker: {reg_err}")
                        logger.info("Reconnected, resuming work")
                    except Exception as recon_err:
                        logger.error(f"Reconnect failed: {recon_err}, retrying...")
                        await asyncio.sleep(1)
                else:
                    logger.error(f"Await error: {e}, retrying...")
                    await asyncio.sleep(1)

    async def _send_with_retry(self, op: str, fn: Callable[[], Awaitable[None]]) -> None:
        """Attempt to send a result (Complete/Fail), reconnecting on connection error."""
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                await fn()
                return
            except Exception as e:
                if not is_connection_error(e) or not self._running:
                    raise
                logger.warning(
                    f"Connection lost while sending {op} result "
                    f"(attempt {attempt}/{max_attempts}), reconnecting..."
                )
                if self._result_client is not None:
                    await self._result_client.reconnect()
        raise RuntimeError(f"Failed to send {op} result after {max_attempts} attempts")

    async def _execute_task(self, task: TaskAssignment) -> None:
        """Execute a task with error handling."""
        assert self._client is not None
        assert self._semaphore is not None
        rc = self._result_client or self._client
        try:
            logger.info(
                f"Executing action: {task.task_type} (task={task.task_id}, attempt={task.attempt})"
            )

            # Get handler
            handler = self._handlers.get(task.task_type)
            if handler is None:
                logger.error(f"No handler registered for action: {task.task_type}")
                await self._send_with_retry(
                    "fail",
                    lambda: rc.worker.fail(
                        self.config.worker_id,
                        task.task_type,
                        task.task_id,
                        f"No handler for: {task.task_type}",
                    ),
                )
                return

            # Create action context
            ctx = ActionContext(
                task_id=task.task_id,
                action_name=task.task_type,
                payload=task.payload,
                attempt=task.attempt,
                created_at=task.created_at,
                namespace=self._parent_client.namespace,
                _worker=self,
            )

            # Execute with timeout
            try:
                result = await asyncio.wait_for(
                    handler(ctx),
                    timeout=self.config.action_timeout,
                )

                # 3-way dispatch based on result type
                from .types import WorkerCompleteOptions

                if isinstance(result, ActionResult):
                    # Named outcome
                    await self._send_with_retry(
                        "complete",
                        lambda: rc.worker.complete(
                            self.config.worker_id,
                            task.task_type,
                            task.task_id,
                            result.data,
                            WorkerCompleteOptions(outcome=result.outcome),
                        ),
                    )
                elif isinstance(result, dict):
                    # Plain dict → JSON serialize
                    result_bytes = json.dumps(result).encode("utf-8")
                    await self._send_with_retry(
                        "complete",
                        lambda: rc.worker.complete(
                            self.config.worker_id,
                            task.task_type,
                            task.task_id,
                            result_bytes,
                        ),
                    )
                else:
                    # bytes or other → pass through
                    await self._send_with_retry(
                        "complete",
                        lambda: rc.worker.complete(
                            self.config.worker_id,
                            task.task_type,
                            task.task_id,
                            result if isinstance(result, bytes) else b"",
                        ),
                    )
                logger.info(f"Action completed: {task.task_type}")

            except asyncio.TimeoutError:
                logger.error(f"Action timed out: {task.task_type}")
                await self._send_with_retry(
                    "fail",
                    lambda: rc.worker.fail(
                        self.config.worker_id,
                        task.task_type,
                        task.task_id,
                        "Action timed out",
                    ),
                )

            except asyncio.CancelledError:
                logger.warning(f"Action cancelled: {task.task_type}")
                await self._send_with_retry(
                    "fail",
                    lambda: rc.worker.fail(
                        self.config.worker_id,
                        task.task_type,
                        task.task_id,
                        "Action cancelled",
                    ),
                )

            except Exception as exc:
                logger.error(f"Action failed: {task.task_type} - {exc}")
                from .types import WorkerFailOptions

                retry = not isinstance(exc, NonRetryableError)
                err_msg = str(exc)
                await self._send_with_retry(
                    "fail",
                    lambda: rc.worker.fail(
                        self.config.worker_id,
                        task.task_type,
                        task.task_id,
                        err_msg,
                        WorkerFailOptions(retry=retry),
                    ),
                )

        except Exception as e:
            logger.error(f"Failed to report task result: {e}")

        finally:
            if self._semaphore is not None:
                self._semaphore.release()

    async def _touch_task(self, action_name: str, task_id: str, extend_ms: int) -> None:
        """Extend lease on a task (internal method)."""
        if self._client is None:
            raise RuntimeError("Worker not connected")
        await self._client.worker.touch(
            self.config.worker_id,
            action_name,
            task_id,
            WorkerTouchOptions(extend_ms=extend_ms),
        )

    def stop(self) -> None:
        """Signal the worker to stop.

        This sets a flag that will cause the polling loop to exit
        after the current iteration completes. Also interrupts in-flight
        connections to unblock any blocking Await call immediately.
        """
        logger.info("Stopping worker...")
        self._running = False
        self._stop_event.set()
        # Interrupt connections to unblock any blocking Await
        if self._client:
            self._client.interrupt()
        if self._result_client:
            self._result_client.interrupt()

    async def close(self) -> None:
        """Stop and close the worker."""
        self.stop()
        if self._result_client:
            await self._result_client.close()
        if self._client:
            await self._client.close()

    async def __aenter__(self) -> "ActionWorker":
        """Async context manager entry."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Async context manager exit — closes the worker."""
        await self.close()


# =============================================================================
# Stream Worker
# =============================================================================

# Type alias for stream record handlers
# Return normally to auto-ack, raise to auto-nack.
StreamRecordHandler = Callable[["StreamContext"], Awaitable[None]]


@dataclass
class StreamWorkerOptions:
    """Configuration for a Flo stream worker.

    Endpoint and namespace are inherited from the parent FloClient.
    """

    stream: str
    group: str = ""
    consumer: str = ""
    worker_id: str = ""
    concurrency: int = 10
    batch_size: int = 10
    block_ms: int = 30000
    message_timeout: float = 300.0  # 5 minutes


@dataclass
class StreamContext:
    """Context passed to stream record handlers.

    Provides access to record data and helper methods.
    """

    record: StreamRecord
    namespace: str
    stream: str
    group: str
    consumer: str

    @property
    def stream_id(self) -> StreamID:
        """Get the record's StreamID."""
        return self.record.id

    @property
    def payload(self) -> bytes:
        """Get the raw record payload."""
        return self.record.payload

    def json(self) -> Any:
        """Parse record payload as JSON."""
        if not self.record.payload:
            raise ValueError("No payload data")
        return json.loads(self.record.payload.decode("utf-8"))

    def into(self, cls: type) -> Any:
        """Parse payload as JSON and instantiate the given class."""
        data = self.json()
        if isinstance(data, dict):
            return cls(**data)
        return cls(data)

    @property
    def headers(self) -> dict[str, str]:
        """Get record headers."""
        return self.record.headers if self.record.headers else {}


class StreamWorker:
    """High-level Flo worker for processing stream records via consumer groups.

    Polls a consumer group with ``group_read()``, dispatches records to the
    handler, and auto-acks on success or auto-nacks on error.

    Created from a FloClient via ``client.new_stream_worker()``.

    Example:
        async with FloClient("localhost:3000", namespace="myapp") as client:
            async def process_event(ctx: StreamContext) -> None:
                event = ctx.json()
                print(f"Got event: {event}")
                # Return normally → auto-ack
                # Raise → auto-nack

            worker = client.new_stream_worker(
                stream="events",
                group="processors",
                consumer="worker-1",
                handler=process_event,
                concurrency=5,
            )
            await worker.start()
    """

    def __init__(
        self,
        parent_client: "FloClient",
        handler: StreamRecordHandler,
        *,
        stream: str,
        group: str,
        consumer: str | None = None,
        worker_id: str | None = None,
        concurrency: int = 10,
        batch_size: int = 10,
        block_ms: int = 30000,
        message_timeout: float = 300.0,
    ):
        self._parent_client = parent_client
        self._handler = handler
        self.config = StreamWorkerOptions(
            stream=stream,
            group=group,
            consumer=consumer or self._generate_consumer_id(),
            worker_id=worker_id or self._generate_consumer_id(),
            concurrency=concurrency,
            batch_size=batch_size,
            block_ms=block_ms,
            message_timeout=message_timeout,
        )

        self._client: FloClient | None = None
        self._running = False
        self._stop_event = asyncio.Event()
        self._tasks: set[asyncio.Task[None]] = set()
        self._semaphore: asyncio.Semaphore | None = None

    @staticmethod
    def _generate_consumer_id() -> str:
        """Generate a unique consumer ID."""
        try:
            hostname = socket.gethostname()
        except Exception:
            hostname = "unknown"
        return f"{hostname}-{secrets.token_hex(4)}"

    async def start(self) -> None:
        """Start the stream worker and begin processing records.

        Joins the consumer group, then polls for records. Blocks until
        ``stop()`` is called.
        """
        logger.info(
            f"Starting stream worker (stream={self.config.stream}, "
            f"group={self.config.group}, consumer={self.config.consumer}, "
            f"concurrency={self.config.concurrency})"
        )

        # Timeout must accommodate block_ms + message_timeout so blocking reads
        # (group_read with block_ms) don't get killed by socket-level timeout.
        worker_timeout_ms = max(
            self.config.block_ms + 5000,
            int(self.config.message_timeout * 1000),
        )
        self._client = FloClient(
            self._parent_client._endpoint,
            namespace=self._parent_client.namespace,
            debug=self._parent_client._debug,
            timeout_ms=worker_timeout_ms,
        )
        await self._client.connect()

        try:
            # Join consumer group
            await self._client.stream.group_join(
                self.config.stream,
                self.config.group,
                self.config.consumer,
            )
            logger.info(f"Joined consumer group {self.config.group} on stream {self.config.stream}")

            self._semaphore = asyncio.Semaphore(self.config.concurrency)
            self._running = True
            self._stop_event.clear()

            await self._poll_loop()

        finally:
            # Wait for in-flight tasks
            if self._tasks:
                logger.info(f"Waiting for {len(self._tasks)} tasks to complete...")
                await asyncio.gather(*self._tasks, return_exceptions=True)

            # Leave consumer group
            if self._client:
                try:
                    await self._client.stream.group_leave(
                        self.config.stream,
                        self.config.group,
                        self.config.consumer,
                    )
                    logger.info(f"Left consumer group {self.config.group}")
                except Exception as e:
                    logger.warning(f"Failed to leave consumer group: {e}")

                await self._client.close()
                self._client = None

            self._running = False
            logger.info("Stream worker stopped")

    async def _poll_loop(self) -> None:
        """Main polling loop for stream records."""
        assert self._client is not None
        assert self._semaphore is not None

        while self._running and not self._stop_event.is_set():
            try:
                # Wait for a concurrency slot
                await self._semaphore.acquire()

                if self._stop_event.is_set():
                    self._semaphore.release()
                    break

                result = await self._client.stream.group_read(
                    self.config.stream,
                    self.config.group,
                    self.config.consumer,
                    StreamGroupReadOptions(
                        count=self.config.batch_size,
                        block_ms=self.config.block_ms,
                    ),
                )

                if not result.records:
                    self._semaphore.release()
                    continue

                # Release semaphore before dispatching — each task will
                # acquire its own slot via _process_record.
                self._semaphore.release()

                for record in result.records:
                    await self._semaphore.acquire()
                    if self._stop_event.is_set():
                        self._semaphore.release()
                        return
                    task = asyncio.create_task(self._process_record(record))
                    self._tasks.add(task)
                    task.add_done_callback(self._tasks.discard)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._semaphore.release()
                if is_connection_error(e):
                    logger.warning("Stream worker lost connection, reconnecting...")
                    try:
                        await self._handle_reconnect()
                        logger.info("Stream worker reconnected, resuming")
                    except Exception as recon_err:
                        logger.error(f"Stream worker reconnect failed: {recon_err}, retrying...")
                        await asyncio.sleep(1)
                else:
                    logger.error(f"Stream read error: {e}, retrying...")
                    await asyncio.sleep(1)

    async def _handle_reconnect(self) -> None:
        """Reconnect and re-join the consumer group."""
        assert self._client is not None
        await self._client.reconnect()
        await self._client.stream.group_join(
            self.config.stream,
            self.config.group,
            self.config.consumer,
        )

    async def _ack_with_retry(
        self, record_ids: list[StreamID], options: StreamGroupAckOptions
    ) -> None:
        """Ack with retry on connection error."""
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                assert self._client is not None
                await self._client.stream.group_ack(
                    self.config.stream,
                    self.config.group,
                    record_ids,
                    options,
                )
                return
            except Exception as e:
                if not is_connection_error(e) or not self._running:
                    raise
                logger.warning(
                    "Connection lost while acking "
                    f"(attempt {attempt}/{max_attempts}), reconnecting..."
                )
                await self._handle_reconnect()

    async def _process_record(self, record: StreamRecord) -> None:
        """Process a single record: call handler, then ack or nack."""
        assert self._client is not None
        assert self._semaphore is not None
        try:
            ctx = StreamContext(
                record=record,
                namespace=self._parent_client.namespace,
                stream=self.config.stream,
                group=self.config.group,
                consumer=self.config.consumer,
            )

            try:
                await asyncio.wait_for(
                    self._handler(ctx),
                    timeout=self.config.message_timeout,
                )

                # Success — ack with retry
                await self._ack_with_retry(
                    [record.id],
                    StreamGroupAckOptions(consumer=self.config.consumer),
                )

            except Exception as e:
                logger.error(
                    f"Record processing failed (stream={self.config.stream}, id={record.id}): {e}"
                )
                try:
                    await self._client.stream.group_nack(
                        self.config.stream,
                        self.config.group,
                        [record.id],
                        StreamGroupNackOptions(consumer=self.config.consumer),
                    )
                except Exception as nack_err:
                    logger.error(f"Failed to nack record: {nack_err}")

        except Exception as e:
            logger.error(f"Failed to process record: {e}")

        finally:
            self._semaphore.release()

    def stop(self) -> None:
        """Signal the stream worker to stop."""
        logger.info("Stopping stream worker...")
        self._running = False
        self._stop_event.set()
        if self._client:
            self._client.interrupt()

    async def close(self) -> None:
        """Stop and close the stream worker."""
        self.stop()
        if self._client:
            await self._client.close()

    async def __aenter__(self) -> "StreamWorker":
        """Async context manager entry."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Async context manager exit — closes the stream worker."""
        await self.close()
