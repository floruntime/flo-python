"""Flo High-Level Worker API

Provides an easy-to-use Worker class for executing actions.

Example:
    from flo import Worker, ActionContext

    async def process_order(ctx: ActionContext) -> bytes:
        order = ctx.json()
        # Process the order...
        return ctx.to_bytes({"status": "completed"})

    async def main():
        worker = Worker(
            endpoint="localhost:3000",
            namespace="myapp",
        )
        worker.action("process-order")(process_order)

        await worker.start()
"""

import asyncio
import json
import logging
import secrets
import socket
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from .client import FloClient
from .types import ActionType, TaskAssignment, WorkerAwaitOptions, WorkerTouchOptions

logger = logging.getLogger("flo.worker")


# Type alias for action handlers
ActionHandler = Callable[["ActionContext"], Awaitable[bytes]]


@dataclass
class WorkerConfig:
    """Configuration for a Flo worker."""

    endpoint: str
    namespace: str = "default"
    worker_id: str = ""
    concurrency: int = 10
    action_timeout: float = 300.0  # 5 minutes
    block_ms: int = 30000
    debug: bool = False


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
    _worker: "Worker" = field(repr=False)
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
        await self._worker._touch_task(self.task_id, extend_ms)

    @property
    def cancelled(self) -> bool:
        """Check if the task has been cancelled."""
        return self._cancel_event.is_set()

    async def check_cancelled(self) -> None:
        """Check if cancelled and raise asyncio.CancelledError if so."""
        if self._cancel_event.is_set():
            raise asyncio.CancelledError("Task was cancelled")


class Worker:
    """High-level Flo worker for executing actions.

    Example:
        worker = Worker(endpoint="localhost:3000", namespace="myapp")

        @worker.action("process-order")
        async def process_order(ctx: ActionContext) -> bytes:
            order = ctx.json()
            # Process the order...
            return ctx.to_bytes({"status": "completed"})

        await worker.start()
    """

    def __init__(
        self,
        endpoint: str,
        *,
        namespace: str = "default",
        worker_id: str | None = None,
        concurrency: int = 10,
        action_timeout: float = 300.0,
        block_ms: int = 30000,
        debug: bool = False,
    ):
        """Initialize a Flo worker.

        Args:
            endpoint: Server endpoint in "host:port" format.
            namespace: Namespace for operations.
            worker_id: Unique worker identifier (auto-generated if not provided).
            concurrency: Maximum number of concurrent actions.
            action_timeout: Timeout for action handlers in seconds.
            block_ms: Timeout for blocking dequeue in milliseconds.
            debug: Enable debug logging.
        """
        self.config = WorkerConfig(
            endpoint=endpoint,
            namespace=namespace,
            worker_id=worker_id or self._generate_worker_id(),
            concurrency=concurrency,
            action_timeout=action_timeout,
            block_ms=block_ms,
            debug=debug,
        )

        self._client: FloClient | None = None
        self._handlers: dict[str, ActionHandler] = {}
        self._running = False
        self._stop_event = asyncio.Event()
        self._tasks: set[asyncio.Task[None]] = set()
        self._semaphore: asyncio.Semaphore | None = None

        if debug:
            logging.basicConfig(level=logging.DEBUG)

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
            f"namespace={self.config.namespace}, concurrency={self.config.concurrency})"
        )

        # Connect to server
        self._client = FloClient(
            self.config.endpoint,
            namespace=self.config.namespace,
            debug=self.config.debug,
        )
        await self._client.connect()

        try:
            # Register actions with the server
            action_names = list(self._handlers.keys())
            for action_name in action_names:
                await self._client.action.register(action_name, ActionType.USER)
                logger.debug(f"Registered action with server: {action_name}")

            # Register worker
            await self._client.worker.register(
                self.config.worker_id,
                action_names,
            )
            logger.info(f"Worker registered with {len(action_names)} actions")

            # Initialize concurrency control
            self._semaphore = asyncio.Semaphore(self.config.concurrency)
            self._running = True
            self._stop_event.clear()

            # Main polling loop
            await self._poll_loop(action_names)

        finally:
            # Wait for running tasks
            if self._tasks:
                logger.info(f"Waiting for {len(self._tasks)} tasks to complete...")
                await asyncio.gather(*self._tasks, return_exceptions=True)

            await self._client.close()
            self._client = None
            self._running = False
            logger.info("Worker stopped")

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
                logger.error(f"Await error: {e}, retrying...")
                await asyncio.sleep(1)

    async def _execute_task(self, task: TaskAssignment) -> None:
        """Execute a task with error handling."""
        assert self._client is not None
        assert self._semaphore is not None
        try:
            logger.info(
                f"Executing action: {task.task_type} (task={task.task_id}, attempt={task.attempt})"
            )

            # Get handler
            handler = self._handlers.get(task.task_type)
            if handler is None:
                logger.error(f"No handler registered for action: {task.task_type}")
                await self._client.worker.fail(
                    self.config.worker_id,
                    task.task_id,
                    f"No handler for: {task.task_type}",
                )
                return

            # Create action context
            ctx = ActionContext(
                task_id=task.task_id,
                action_name=task.task_type,
                payload=task.payload,
                attempt=task.attempt,
                created_at=task.created_at,
                namespace=self.config.namespace,
                _worker=self,
            )

            # Execute with timeout
            try:
                result = await asyncio.wait_for(
                    handler(ctx),
                    timeout=self.config.action_timeout,
                )

                # Success - complete the task
                await self._client.worker.complete(
                    self.config.worker_id,
                    task.task_id,
                    result,
                )
                logger.info(f"Action completed: {task.task_type}")

            except asyncio.TimeoutError:
                logger.error(f"Action timed out: {task.task_type}")
                await self._client.worker.fail(
                    self.config.worker_id,
                    task.task_id,
                    "Action timed out",
                )

            except asyncio.CancelledError:
                logger.warning(f"Action cancelled: {task.task_type}")
                await self._client.worker.fail(
                    self.config.worker_id,
                    task.task_id,
                    "Action cancelled",
                )

            except Exception as e:
                logger.error(f"Action failed: {task.task_type} - {e}")
                await self._client.worker.fail(
                    self.config.worker_id,
                    task.task_id,
                    str(e),
                )

        except Exception as e:
            logger.error(f"Failed to report task result: {e}")

        finally:
            if self._semaphore is not None:
                self._semaphore.release()

    async def _touch_task(self, task_id: str, extend_ms: int) -> None:
        """Extend lease on a task (internal method)."""
        if self._client is None:
            raise RuntimeError("Worker not connected")
        await self._client.worker.touch(
            self.config.worker_id,
            task_id,
            WorkerTouchOptions(extend_ms=extend_ms),
        )

    def stop(self) -> None:
        """Signal the worker to stop.

        This sets a flag that will cause the polling loop to exit
        after the current iteration completes.
        """
        logger.info("Stopping worker...")
        self._running = False
        self._stop_event.set()

    async def close(self) -> None:
        """Stop and close the worker."""
        self.stop()
        if self._client:
            await self._client.close()
