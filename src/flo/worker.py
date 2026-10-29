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
import dataclasses
import functools
import json
import logging
import secrets
import socket
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from .client import FloClient
from .exceptions import NonRetryableError, NotFoundError, ServerError, is_connection_error
from .retry import (
    _halted_backoff,
    _sleep_unless_stopped,
    _worker_backoff,
    is_refused_unrun,
    is_shard_halted,
)
from .types import (
    _DEFAULT_WORKER_BLOCK_MS,
    ActionRegisterOptions,
    ActionType,
    Reason,
    StatusCode,
    StreamGroupAckOptions,
    StreamGroupNackOptions,
    StreamGroupReadOptions,
    StreamID,
    StreamRecord,
    TaskAssignment,
    WorkerAwaitOptions,
    WorkerRegisterOptions,
    WorkerStatus,
    WorkerTouchOptions,
    WorkerType,
)
from .wire import _validate_block_ms

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


class _EmptyPollBackoff:
    """Paces a poll loop whose blocking reads come back empty early.

    A blocking read can come back empty early for two reasons that look the
    same on the wire. The server may have no room to park it (re-polling at
    once would spin), or an append woke it so the consumer re-reads. A full
    server answers within a round trip, so only answers under
    min(250 ms, block_ms/2) count as early. Even then, one may be an append
    wake, so the first in a row is re-polled at once.
    """

    _EARLY_S = 0.25
    _FIRST_S = 0.05
    _MAX_S = 1.0

    def __init__(self, block_ms: int, stop: asyncio.Event) -> None:
        self._early_s = min(self._EARLY_S, block_ms / 2000.0)
        self._stop = stop
        self._streak = 0

    def reset(self) -> None:
        self._streak = 0

    def _next_pause(self, elapsed_s: float) -> float:
        if elapsed_s >= self._early_s:
            self._streak = 0
            return 0.0
        self._streak += 1
        if self._streak == 1:
            return 0.0
        # The exponent is capped: the float power overflows on a long streak,
        # and the pause reaches _MAX_S long before the cap.
        return min(self._FIRST_S * 2.0 ** min(self._streak - 2, 16), self._MAX_S)

    async def empty(self, elapsed_s: float) -> None:
        pause = self._next_pause(elapsed_s)
        if pause:
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=pause)


# A result (complete, fail, ack, nack) is sent at most this many times. Past
# that it is dropped: the lease or ack timeout runs out on the server and the
# task or record is delivered again, so delivery stays at-least-once.
_RESULT_SENDS = 3

# A result to send: the op name, the send on a given client, and whether the
# handler was cancelled.
_Report = tuple[str, Callable[[FloClient], Awaitable[None]], bool]

# How long a stopping or draining worker keeps reconnecting to resend a lost
# result, so neither can hang on a server that is gone.
_STOP_RECONNECT_S = 2.0

# auto_touch never touches more often than this, whatever the timeout.
_MIN_TOUCH_INTERVAL_MS = 100

# The longest lease a touch may ask for; the server refuses more.
_MAX_TOUCH_EXTEND_MS = 3_600_000

# How often a worker heartbeats. The server refuses work to a worker silent
# past its heartbeat timeout (90 s by default) and later removes it.
_HEARTBEAT_INTERVAL_S = 30.0

# How often a draining worker tries again to tell the server, and how long a
# reconnect for that notice, or for deregistering, may take: neither may hold
# up the end of a run.
_DRAIN_NOTICE_INTERVAL_S = 1.0


async def _cancel(task: "asyncio.Future[Any]") -> None:
    """Cancel `task` and wait for it to end. The cancel is sent again until
    it lands: before Python 3.12, asyncio.wait_for swallows a cancel that
    arrives as its inner call finishes, as a refused dial does at once, and
    the task would carry on (a reconnect, for up to 5 minutes)."""
    while not task.done():
        task.cancel()
        await asyncio.wait({task}, timeout=0.05)
    if not task.cancelled():
        task.exception()  # retrieved: it finished or failed as it was cancelled


def _lost(e: BaseException) -> bool:
    """Whether a request failed for want of a connection, not by refusal."""
    return is_connection_error(e) or isinstance(e, asyncio.TimeoutError)


async def _unless_set(event: asyncio.Event, work: Awaitable[None]) -> bool:
    """Run `work` unless `event` is set first, which cancels it. True if it
    finished; its error is raised."""
    task = asyncio.ensure_future(work)
    waiter = asyncio.ensure_future(event.wait())
    try:
        await asyncio.wait({task, waiter}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        waiter.cancel()
    if not task.done():
        await _cancel(task)
        return False
    task.result()
    return True


async def _acquire_unless_set(sem: asyncio.Semaphore, event: asyncio.Event) -> bool:
    """Take a slot of `sem` unless `event` is set first. True if taken."""
    if event.is_set():
        return False
    acquire = asyncio.ensure_future(sem.acquire())
    waiter = asyncio.ensure_future(event.wait())
    try:
        await asyncio.wait({acquire, waiter}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        waiter.cancel()
    if acquire.done() and not acquire.cancelled():
        if event.is_set():
            sem.release()
            return False
        return True
    acquire.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await acquire
    # It may have taken the slot as it was cancelled.
    if acquire.done() and not acquire.cancelled():
        sem.release()
    return False


class _RegisteredWorker:
    """What both workers share: a place in the server's worker registry,
    kept by heartbeats, and the end of a run by drain or stop.

    A drain (drain(), a DRAINING heartbeat, or an await refused as draining)
    stops new work while running handlers finish, heartbeats going on. A
    drain started on the server answers a held action await at once but not
    a held stream read, so a stream worker learns of it from its next
    heartbeat (_HEARTBEAT_INTERVAL_S); either way the worker then interrupts its own poll. The
    server is told at once, and again after any re-registration, which marks
    a worker active. stop() ends polling too, without telling the server.
    Both are cleared when start() returns, not when it begins, so one that
    lands before or during startup still ends that run.
    """

    _client: FloClient | None
    _tasks: set[asyncio.Task[None]]

    def _init_run_state(self) -> None:
        self._stop_event = asyncio.Event()
        self._drain_event = asyncio.Event()
        # Set by stop() or a drain: the poll loop leaves, and its pauses and
        # reconnects end.
        self._leave_event = asyncio.Event()
        self._notice_due = False
        self._heartbeat_task: asyncio.Task[None] | None = None

    @property
    def _worker_id(self) -> str:
        raise NotImplementedError

    def _control(self) -> FloClient | None:
        """The connection heartbeats, drain notices and the deregister ride:
        not the poll connection, held up to block_ms by a blocking read."""
        raise NotImplementedError

    def _control_generation(self) -> int:
        raise NotImplementedError

    async def _reconnect_control(self, failed_generation: int | None = None) -> None:
        raise NotImplementedError

    async def _register(self, client: FloClient) -> None:
        raise NotImplementedError

    async def _while_halted(self, what: str, call: Callable[[], Awaitable[object]]) -> bool:
        """Run call, asking again while the shard is halted: it resumes once an
        operator repairs it, and the worker comes up by itself then. False if
        a stop or drain ended the wait."""
        halted = _halted_backoff()
        while True:
            try:
                await call()
                return True
            except Exception as e:
                if not is_shard_halted(e):
                    raise
                d = halted.next()
                logger.warning(f"{what} refused, shard halted: {e}; retrying in {d:.3f}s")
                await _sleep_unless_stopped(self._leave_event, d)
                if self._leave_event.is_set():
                    return False

    def _load(self) -> int:
        """The handlers running: the load heartbeats report."""
        raise NotImplementedError

    def drain(self) -> None:
        """Stop taking work and let the running handlers finish; start()
        then returns. The server is told, so it assigns this worker nothing
        more, and heartbeats go on meanwhile. A blocking await or read is
        interrupted, so the worker leaves it at once rather than after
        block_ms. A drain before start() ends that run once it has
        registered."""
        logger.info("Draining worker...")
        self._begin_drain()

    def _begin_drain(self) -> None:
        if not self._drain_event.is_set():
            self._notice_due = True
        self._drain_event.set()
        self._leave_event.set()
        # Unblock a long poll, as stop() does.
        if self._client is not None:
            self._client.interrupt()

    def _signal_stop(self) -> None:
        self._stop_event.set()
        self._leave_event.set()
        # Only the poll connection is interrupted: the other still has to
        # carry the results of the handlers that are finishing.
        if self._client is not None:
            self._client.interrupt()

    def _end_run(self) -> None:
        self._stop_event.clear()
        self._drain_event.clear()
        self._leave_event.clear()
        self._notice_due = False

    async def _heartbeat(self) -> None:
        """Send one heartbeat, registering again if the server removed this
        worker: unregistered, it would go on taking work the server can't
        see it holding. A DRAINING answer drains it."""
        client = self._control()
        assert client is not None
        load = self._load()
        seen = self._control_generation()
        try:
            status = await client.worker.heartbeat(self._worker_id, current_load=load)
        except NotFoundError:
            logger.warning("Worker was removed by the server; registering again")
            await self._register(client)
            if self._drain_event.is_set():
                self._notice_due = True
            return
        except Exception as e:
            # Nothing else reconnects this connection for an idle worker, which
            # would then miss every heartbeat and be refused work for good.
            if _lost(e):
                with contextlib.suppress(Exception):
                    await self._reconnect_control(seen)
            raise
        if status == WorkerStatus.DRAINING and not self._drain_event.is_set():
            logger.info("The server drained this worker; finishing its running work")
            self._begin_drain()
        logger.debug(f"Heartbeat sent (load={load})")

    async def _heartbeat_loop(self) -> None:
        """Heartbeat until cancelled at the end of the run, after the last
        handler, so a draining or stopping worker's handlers still count as
        alive."""
        while True:
            await asyncio.sleep(_HEARTBEAT_INTERVAL_S)
            try:
                await self._heartbeat()
            except Exception as e:
                logger.warning(f"Heartbeat failed: {e}")

    def _start_heartbeats(self) -> None:
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _stop_heartbeats(self) -> None:
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat_task
            self._heartbeat_task = None

    async def _finish_drain(self) -> None:
        """Wait for the running handlers, keeping the server told of the
        drain. A notice that can't be sent is tried again every
        _DRAIN_NOTICE_INTERVAL_S while handlers run; an idle worker tries
        once. A stop ends the wait; start() still waits for the handlers."""
        while True:
            if self._notice_due:
                await self._send_drain_notice()
            pending = set(self._tasks)
            if not pending or self._stop_event.is_set():
                return
            stopped = asyncio.ensure_future(self._stop_event.wait())
            try:
                await asyncio.wait(
                    pending | {stopped},
                    timeout=_DRAIN_NOTICE_INTERVAL_S,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                stopped.cancel()

    async def _send_drain_notice(self) -> None:
        client = self._control()
        assert client is not None
        seen = self._control_generation()
        try:
            await client.worker.drain(self._worker_id)
            self._notice_due = False
        except Exception as e:
            if not _lost(e):
                logger.warning(f"Drain notice refused: {e}")
                self._notice_due = False
                return
            logger.warning(f"Drain notice lost: {e}; trying again")
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self._reconnect_control(seen), _DRAIN_NOTICE_INTERVAL_S)

    async def _on_control_once(
        self, what: str, send: Callable[[FloClient], Awaitable[None]]
    ) -> None:
        """Send a closing request (deregister, group leave), reconnecting the
        control connection once if it is down; never raises."""
        client = self._control()
        if client is None:
            return
        seen = self._control_generation()
        try:
            await send(client)
            return
        except Exception as e:
            if not _lost(e):
                logger.warning(f"{what} refused: {e}")
                return
        try:
            await asyncio.wait_for(self._reconnect_control(seen), _DRAIN_NOTICE_INTERVAL_S)
            await send(client)
        except Exception as e:
            logger.warning(f"{what} failed: {e}")


def _touch_pace(timeout_ms: int) -> tuple[float, int]:
    """How often (in seconds) to touch a run of an action with timeout_ms,
    and the lease each touch asks for: the timeout, at most an hour, touched
    every third of that (at least _MIN_TOUCH_INTERVAL_MS apart) so two can
    fail in a row."""
    extend_ms = min(timeout_ms, _MAX_TOUCH_EXTEND_MS)
    return max(extend_ms / 3, _MIN_TOUCH_INTERVAL_MS) / 1000, extend_ms


# Touch refusals that mean the run moved on: touching again can't help.
_TOUCH_FINAL = (StatusCode.CONFLICT, StatusCode.NOT_FOUND, StatusCode.BAD_REQUEST)


async def _send_result(
    what: str,
    send: Callable[[], Awaitable[object]],
    generation: Callable[[], int],
    reconnect: Callable[[int], Awaitable[None]],
    stop: asyncio.Event,
) -> bool:
    """Send a result, resending the identical request after a reconnect when
    the connection drops or the request times out.

    A refusal is not resent: the server read the request and said no. Once
    the worker is leaving (stopping or draining, so neither waits on a server
    that is gone), reconnects get _STOP_RECONNECT_S in all. Nothing here
    raises; a result that can't be sent is logged and dropped.
    """
    loop = asyncio.get_running_loop()
    stop_deadline: float | None = None

    async def reconnect_bounded(seen: int) -> bool:
        """Reconnect; False if the stop allowance ran out first."""
        nonlocal stop_deadline
        task = asyncio.ensure_future(reconnect(seen))
        try:
            if not stop.is_set():
                waiter = asyncio.ensure_future(stop.wait())
                try:
                    await asyncio.wait({task, waiter}, return_when=asyncio.FIRST_COMPLETED)
                finally:
                    waiter.cancel()
            if not task.done():
                if stop_deadline is None:
                    stop_deadline = loop.time() + _STOP_RECONNECT_S
                await asyncio.wait({task}, timeout=max(stop_deadline - loop.time(), 0.0))
        finally:
            if not task.done():
                await _cancel(task)
        if task.cancelled():
            return False
        task.result()
        return True

    for attempt in range(1, _RESULT_SENDS + 1):
        seen = generation()
        try:
            await send()
            return True
        except Exception as e:
            if not (is_connection_error(e) or isinstance(e, asyncio.TimeoutError)):
                logger.error(f"{what} was refused, not resending: {e}")
                return False
            if attempt == _RESULT_SENDS:
                logger.error(f"Dropped {what} after {_RESULT_SENDS} sends: {e}")
                return False
            if stop.is_set() and stop_deadline is None:
                stop_deadline = loop.time() + _STOP_RECONNECT_S
            if stop_deadline is not None and loop.time() >= stop_deadline:
                logger.error(f"Dropped {what}: connection lost while the worker stops: {e}")
                return False
            logger.warning(
                f"Lost {what} (send {attempt}/{_RESULT_SENDS}): {e}; reconnecting to resend"
            )
            try:
                if not await reconnect_bounded(seen):
                    logger.error(f"Dropped {what}: no connection before the worker stopped")
                    return False
            except Exception as rc_err:
                # The next send fails fast on the closed connection and
                # counts as one of the attempts.
                logger.warning(f"Reconnect to resend {what} failed: {rc_err}")
    return False


def _worker_block_ms(block_ms: int | None) -> int:
    if not block_ms:
        return _DEFAULT_WORKER_BLOCK_MS
    _validate_block_ms(block_ms)
    return block_ms


@dataclass
class ActionWorkerOptions:
    """Configuration for a Flo action worker.

    Endpoint and namespace are inherited from the parent FloClient.
    """

    worker_id: str = ""
    concurrency: int = 10
    action_timeout: float = 300.0  # 5 minutes
    block_ms: int = _DEFAULT_WORKER_BLOCK_MS
    # Touch each run every third of its action's timeout while the handler
    # runs, so a long handler isn't taken back and run again elsewhere.
    auto_touch: bool = False

    def __post_init__(self) -> None:
        self.block_ms = _worker_block_ms(self.block_ms)


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


class ActionWorker(_RegisteredWorker):
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
        block_ms: int = _DEFAULT_WORKER_BLOCK_MS,
        auto_touch: bool = False,
    ):
        """Initialize a Flo worker from a connected client.

        Args:
            parent_client: The parent FloClient whose endpoint and namespace are used.
            worker_id: Unique worker identifier (auto-generated if not provided).
            concurrency: Maximum number of concurrent actions.
            action_timeout: Timeout for action handlers in seconds.
            block_ms: Long-poll wait per await in milliseconds, at most 300000.
                0 means the 30000 default.
            auto_touch: Touch each run every third of its action's timeout
                while its handler runs. Off by default: a handler that runs
                past the timeout without touching is taken back and run again.
        """
        self._parent_client = parent_client
        self.config = ActionWorkerOptions(
            worker_id=worker_id or self._generate_worker_id(),
            concurrency=concurrency,
            action_timeout=action_timeout,
            block_ms=block_ms,
            auto_touch=auto_touch,
        )

        self._client: FloClient | None = None
        self._result_client: FloClient | None = None
        # Concurrent results that fail on the same dead connection reconnect
        # it once: the generation tells a waiter someone already did.
        self._result_reconnect_lock = asyncio.Lock()
        self._result_generation = 0
        self._handlers: dict[str, ActionHandler] = {}
        self._action_options: dict[str, ActionRegisterOptions] = {}
        self._running = False
        self._init_run_state()
        self._tasks = set()
        self._semaphore: asyncio.Semaphore | None = None
        self._action_names: list[str] = []
        # The load heartbeats report. Not the semaphore, which also counts the
        # slot an outstanding await holds, nor _tasks, which also counts
        # results still being sent.
        self._running_handlers = 0

    @staticmethod
    def _generate_worker_id() -> str:
        """Generate a unique worker ID."""
        try:
            hostname = socket.gethostname()
        except Exception:
            hostname = "unknown"
        return f"{hostname}-{secrets.token_hex(4)}"

    def action(
        self, name: str, options: ActionRegisterOptions | None = None
    ) -> Callable[[ActionHandler], ActionHandler]:
        """Decorator to register an action handler (see register_action).

        Args:
            name: The action name to register.
            options: The action's timeout, attempts and owner.

        Returns:
            Decorator function.

        Example:
            @worker.action("process-order")
            async def process_order(ctx: ActionContext) -> bytes:
                return ctx.to_bytes({"status": "ok"})
        """

        def decorator(handler: ActionHandler) -> ActionHandler:
            self.register_action(name, handler, options)
            return handler

        return decorator

    def register_action(
        self,
        name: str,
        handler: ActionHandler,
        options: ActionRegisterOptions | None = None,
    ) -> None:
        """Register an action handler.

        The worker registers the action with the server when it starts, with
        exactly these options (the server's defaults when none: a 30 s
        timeout, 3 attempts). Registering replaces the action's settings, so
        workers registering one action with different options leave it with
        whichever registered last. The options' namespace is ignored: the
        action goes in the worker's.

        Args:
            name: The action name.
            handler: Async function that handles the action.
            options: The action's timeout, attempts and owner.

        Raises:
            ValueError: If action is already registered.
        """
        if name in self._handlers:
            raise ValueError(f"Action '{name}' is already registered")
        self._handlers[name] = handler
        self._action_options[name] = dataclasses.replace(
            options or ActionRegisterOptions(), namespace=None
        )
        logger.info(f"Registered action: {name}")

    async def start(self) -> None:
        """Start the worker and process actions until stop(), or a drain
        once its running handlers finish. Either one before or during
        startup ends this run. Running handlers finish and send their results
        before start() returns. It may be called again afterwards.

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

        self._running = True
        registered = False
        try:
            # A dedicated connection, with the parent client's endpoint and
            # namespace, for polling.
            worker_timeout_ms = int(self._parent_client._timeout * 1000)
            self._client = FloClient(
                self._parent_client._endpoint,
                namespace=self._parent_client.namespace,
                debug=self._parent_client._debug,
                timeout_ms=worker_timeout_ms,
                retry=self._parent_client._retry,
            )
            await self._client.connect()

            # A second connection for results, heartbeats and drain notices.
            # The polling connection holds its lock during blocking Await
            # calls (up to block_ms), so a separate connection prevents
            # contention.
            self._result_client = FloClient(
                self._parent_client._endpoint,
                namespace=self._parent_client.namespace,
                debug=self._parent_client._debug,
                timeout_ms=worker_timeout_ms,
                retry=self._parent_client._retry,
            )
            await self._result_client.connect()
            if self._stop_event.is_set():
                return

            # On the result connection: a stop or drain during startup cuts
            # the poll one.
            action_names = list(self._handlers.keys())
            self._action_names = action_names
            results = self._result_client
            for action_name in action_names:
                opts = self._action_options[action_name]
                if not await self._while_halted(
                    f"Registering action {action_name}",
                    functools.partial(results.action.register, action_name, ActionType.USER, opts),
                ):
                    return
                logger.debug(f"Registered action with server: {action_name}")

            # Deregistered at the end even if this answer is lost: the
            # registration may still have taken.
            registered = True
            if not await self._while_halted("Registering", lambda: self._register(results)):
                return
            logger.info(f"Worker registered with {len(action_names)} actions")

            self._semaphore = asyncio.Semaphore(self.config.concurrency)
            self._start_heartbeats()
            await self._poll_loop(action_names)
            if self._drain_event.is_set():
                await self._finish_drain()

        finally:
            if self._tasks:
                logger.info(f"Waiting for {len(self._tasks)} tasks to complete...")
                await asyncio.gather(*self._tasks, return_exceptions=True)
            await self._stop_heartbeats()
            if registered:
                wid = self.config.worker_id
                await self._on_control_once("Deregister", lambda c: c.worker.deregister(wid))

            if self._result_client:
                await self._result_client.close()
                self._result_client = None
            if self._client:
                await self._client.close()
                self._client = None
            self._running = False
            self._end_run()
            logger.info("Worker stopped")

    @property
    def _worker_id(self) -> str:
        return self.config.worker_id

    def _control(self) -> FloClient | None:
        return self._result_client

    def _control_generation(self) -> int:
        return self._result_generation

    async def _reconnect_control(self, failed_generation: int | None = None) -> None:
        await self._reconnect_result_client(failed_generation)

    def _load(self) -> int:
        return self._running_handlers

    async def _register(self, client: FloClient) -> None:
        """Register this worker, with the same options every time."""
        await client.worker.register(
            self.config.worker_id,
            self._action_names,
            WorkerRegisterOptions(
                concurrency=self.config.concurrency,
                machine_id=socket.gethostname(),
            ),
        )

    async def _poll_loop(self, action_names: list[str]) -> None:
        """Main polling loop for tasks."""
        assert self._client is not None
        assert self._semaphore is not None
        # Pause before each reconnect after the first in a row: a server that
        # accepts and then drops the connection would otherwise be hit in a
        # tight loop. A poll that gets an answer resets it.
        reconnect_pause = 0.0
        loop = asyncio.get_running_loop()
        leave = self._leave_event
        backoff = _EmptyPollBackoff(self.config.block_ms, leave)
        failed = _worker_backoff()
        halted = _halted_backoff()
        while not leave.is_set():
            try:
                # Wait for a slot; a stop or drain ends the wait.
                if not await _acquire_unless_set(self._semaphore, leave):
                    break

                # Await task from server
                started = loop.time()
                result = await self._client.worker.await_task(
                    self.config.worker_id,
                    action_names,
                    WorkerAwaitOptions(block_ms=self.config.block_ms),
                )
                reconnect_pause = 0.0
                failed.reset()
                halted.reset()

                if result.task is None:
                    # No task available, release semaphore and continue
                    self._semaphore.release()
                    await backoff.empty(loop.time() - started)
                    continue
                backoff.reset()

                # Execute task in background
                task = asyncio.create_task(self._execute_task(result.task))
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._semaphore.release()
                reason = e.reason if isinstance(e, ServerError) else None
                if reason == Reason.WORKER_DRAINING:
                    # The server drained this worker: take no more work, and
                    # let the running handlers finish.
                    logger.info("Worker is draining; no longer taking work")
                    self._begin_drain()
                    break
                if reason == Reason.WORKER_UNHEALTHY:
                    # It missed its heartbeats: one now lets it take work
                    # again; the backoff keeps a heartbeat that fails from
                    # turning this into a hot loop.
                    try:
                        await self._heartbeat()
                    except Exception as hb_err:
                        logger.warning(f"Heartbeat failed: {hb_err}")
                    d = failed.next()
                    logger.warning(f"Await refused, worker unhealthy; retrying in {d:.3f}s")
                    await _sleep_unless_stopped(leave, d)
                elif is_shard_halted(e):
                    d = halted.next()
                    logger.warning(f"Await refused, shard halted: {e}; retrying in {d:.3f}s")
                    await _sleep_unless_stopped(leave, d)
                elif is_connection_error(e):
                    # A stop or drain interrupts the poll connection: that is
                    # not a drop to recover from.
                    if leave.is_set():
                        break
                    if reconnect_pause:
                        await _sleep_unless_stopped(leave, reconnect_pause)
                        if leave.is_set():
                            break
                    reconnect_pause = min(max(reconnect_pause * 2, 1.0), 30.0)
                    logger.warning("Connection lost, reconnecting...")
                    try:
                        # Up to 5 min of tries, which a stop or drain ends.
                        if not await _unless_set(leave, self._client.reconnect()):
                            break
                        # Also reconnect result client
                        if self._result_client is not None:
                            try:
                                await self._reconnect_result_client(only_if_down=True)
                            except Exception as rc_err:
                                logger.warning(f"Failed to reconnect result client: {rc_err}")
                        # Re-register worker after reconnect
                        try:
                            await self._register(self._client)
                        except Exception as reg_err:
                            logger.warning(f"Failed to re-register worker: {reg_err}")
                        logger.info("Reconnected, resuming work")
                    except Exception as recon_err:
                        logger.error(f"Reconnect failed: {recon_err}, retrying...")
                else:
                    d = failed.next()
                    logger.error(f"Await error: {e}, retrying in {d:.3f}s...")
                    await _sleep_unless_stopped(leave, d)

    async def _reconnect_result_client(
        self, failed_generation: int | None = None, *, only_if_down: bool = False
    ) -> None:
        """Reconnect the result connection, unless it was reconnected since
        `failed_generation` was observed, or, with `only_if_down`, it is up.

        The poll connection dropping says nothing certain about this one, and
        replacing a healthy connection would cut off results in flight on it.
        """
        assert self._result_client is not None
        async with self._result_reconnect_lock:
            if failed_generation is not None and failed_generation != self._result_generation:
                return
            if only_if_down and self._result_client.is_connected:
                return
            await self._result_client.reconnect()
            self._result_generation += 1

    async def _execute_task(self, task: TaskAssignment) -> None:
        """Run a task's handler, then report its outcome."""
        rc = self._result_client
        assert rc is not None
        try:
            self._running_handlers += 1
            try:
                op, send, cancelled = await self._run_handler(task)
            finally:
                self._running_handlers -= 1
            # Reporting stays outside the handler's error handling: a complete
            # that can't be delivered must not turn into a fail.
            sent = await _send_result(
                f"{op} for task {task.task_id}",
                lambda: send(rc),
                lambda: self._result_generation,
                self._reconnect_result_client,
                self._leave_event,
            )
            if sent and op == "complete":
                logger.info(f"Action completed: {task.task_type} (task={task.task_id})")
            if cancelled:
                raise asyncio.CancelledError()
        except Exception as e:
            logger.error(f"Failed to report result for task {task.task_id}: {e}")
        finally:
            if self._semaphore is not None:
                self._semaphore.release()

    async def _run_handler(self, task: TaskAssignment) -> "_Report":
        """Run the task's handler. Its outcome alone decides complete or fail.

        Returns the result to send, and whether the handler was cancelled, in
        which case the caller re-raises the cancellation after reporting.
        """
        from .types import WorkerCompleteOptions, WorkerFailOptions

        wid = self.config.worker_id

        def fail(message: str, retry: bool = True, cancelled: bool = False) -> _Report:
            options = WorkerFailOptions(retry=retry)
            return (
                "fail",
                lambda c: c.worker.fail(wid, task.task_type, task.task_id, message, options),
                cancelled,
            )

        def complete(data: bytes, options: WorkerCompleteOptions | None = None) -> _Report:
            return (
                "complete",
                lambda c: c.worker.complete(wid, task.task_type, task.task_id, data, options),
                False,
            )

        logger.info(
            f"Executing action: {task.task_type} (task={task.task_id}, attempt={task.attempt})"
        )
        handler = self._handlers.get(task.task_type)
        if handler is None:
            logger.error(f"No handler registered for action: {task.task_type}")
            return fail(f"No handler for: {task.task_type}")

        ctx = ActionContext(
            task_id=task.task_id,
            action_name=task.task_type,
            payload=task.payload,
            attempt=task.attempt,
            created_at=task.created_at,
            namespace=self._parent_client.namespace,
            _worker=self,
        )
        # Not wait_for: a TimeoutError the handler raises itself would look
        # the same as the deadline passing. Only the deadline is final.
        run = asyncio.ensure_future(handler(ctx))
        timeout_ms = self._action_options[task.task_type].timeout_ms
        stop_touching = asyncio.Event()
        touching = (
            asyncio.ensure_future(self._keep_touching(task, timeout_ms, stop_touching))
            if self.config.auto_touch and timeout_ms > 0
            else None
        )
        try:
            await asyncio.wait({run}, timeout=self.config.action_timeout)
        except asyncio.CancelledError:
            run.cancel()
            logger.warning(f"Action cancelled: {task.task_type}")
            return fail("Action cancelled", cancelled=True)
        finally:
            if touching is not None:
                # Let a touch in flight finish: cancelling it mid-request
                # would close the result connection the outcome goes on.
                stop_touching.set()
                with contextlib.suppress(Exception):
                    await asyncio.shield(touching)
        if not run.done():
            run.cancel()
            with contextlib.suppress(BaseException):
                await run
            logger.error(f"Action timed out: {task.task_type}")
            return fail("Action timed out", retry=False)
        try:
            result = run.result()
        except asyncio.CancelledError:
            logger.warning(f"Action cancelled: {task.task_type}")
            return fail("Action cancelled", cancelled=True)
        except Exception as exc:
            logger.error(f"Action failed: {task.task_type} - {exc!r}")
            message = str(exc) or type(exc).__name__
            return fail(message, retry=not isinstance(exc, NonRetryableError))

        if isinstance(result, ActionResult):
            return complete(result.data, WorkerCompleteOptions(outcome=result.outcome))
        if isinstance(result, dict):
            return complete(json.dumps(result).encode("utf-8"))
        return complete(result if isinstance(result, bytes) else b"")

    async def _touch_task(self, action_name: str, task_id: str, extend_ms: int) -> None:
        """Extend lease on a task (internal method).

        On the result connection: the poll connection may be held by a
        blocking await for up to block_ms, longer than a lease can wait.
        """
        client = self._result_client or self._client
        if client is None:
            raise RuntimeError("Worker not connected")
        await client.worker.touch(
            self.config.worker_id,
            action_name,
            task_id,
            WorkerTouchOptions(extend_ms=extend_ms),
        )

    async def _keep_touching(
        self, task: TaskAssignment, timeout_ms: int, stop: asyncio.Event
    ) -> None:
        """Touch a run at _touch_pace(timeout_ms) until stop is set.

        Each touch asks for this worker's timeout (at most an hour), so the
        lease covers it even if another worker's registration shortened the
        action's. Conflict, not_found or bad_request means the run moved on
        (finished, cancelled, taken back) and touching stops; any other
        refusal, an overloaded leader say, is tried again next interval, and
        a lost connection is reconnected for it.
        """
        interval_s, extend_ms = _touch_pace(timeout_ms)
        while True:
            try:
                await asyncio.wait_for(stop.wait(), interval_s)
                return
            except asyncio.TimeoutError:
                pass
            seen = self._result_generation
            try:
                await self._touch_task(task.task_type, task.task_id, extend_ms)
            except ServerError as e:
                if e.status_code in _TOUCH_FINAL:
                    logger.warning(f"Touch for task {task.task_id} was refused, stopping: {e}")
                    return
                logger.warning(f"Touch for task {task.task_id} was refused: {e}; trying again")
            except Exception as e:
                logger.warning(f"Touch for task {task.task_id} lost: {e!r}")
                if is_connection_error(e) or isinstance(e, asyncio.TimeoutError):
                    with contextlib.suppress(Exception):
                        await self._reconnect_result_client(seen)

    def stop(self) -> None:
        """Signal the worker to stop.

        Ends the polling loop, interrupting a blocking Await or a reconnect,
        and a drain's wait. Running handlers finish and their results are
        sent before start() returns. A stop before start() ends that run at
        once.
        """
        logger.info("Stopping worker...")
        self._signal_stop()

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
    block_ms: int = _DEFAULT_WORKER_BLOCK_MS
    message_timeout: float = 300.0  # 5 minutes
    # Drain this consumer's pending (delivered-but-unacked) entries via
    # group_claim after a reconnect, before resuming normal group_read.
    # Required for at-least-once across reconnects: group_read alone only
    # returns records past last_delivered_id. Set False for at-most-once.
    redeliver_pending_on_reconnect: bool = True
    # Minimum idle time (ms) an entry must have before the reconnect drain
    # claims it. 0 = drain everything this consumer already owns; > 0 also
    # steals entries abandoned by other (dead) consumers idle at least this long.
    claim_min_idle_ms: int = 0

    def __post_init__(self) -> None:
        self.block_ms = _worker_block_ms(self.block_ms)


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


class StreamWorker(_RegisteredWorker):
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
        block_ms: int = _DEFAULT_WORKER_BLOCK_MS,
        message_timeout: float = 300.0,
        redeliver_pending_on_reconnect: bool = True,
        claim_min_idle_ms: int = 0,
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
            redeliver_pending_on_reconnect=redeliver_pending_on_reconnect,
            claim_min_idle_ms=claim_min_idle_ms,
        )

        self._client: FloClient | None = None
        self._ack_client: FloClient | None = None
        # Concurrent acks that fail on the same dead connection reconnect it
        # once: the generation tells a waiter someone already did.
        self._ack_reconnect_lock = asyncio.Lock()
        self._ack_generation = 0
        self._running = False
        self._init_run_state()
        self._tasks = set()
        self._semaphore: asyncio.Semaphore | None = None
        self._running_handlers = 0

    @staticmethod
    def _generate_consumer_id() -> str:
        """Generate a unique consumer ID."""
        try:
            hostname = socket.gethostname()
        except Exception:
            hostname = "unknown"
        return f"{hostname}-{secrets.token_hex(4)}"

    async def start(self) -> None:
        """Start the stream worker and process records until stop(), or a
        drain once its running handlers finish and ack. Either one before or
        during startup ends this run. It registers in the worker registry and
        heartbeats while it runs, then leaves the group and deregisters. It may
        be called again afterwards.
        """
        logger.info(
            f"Starting stream worker (stream={self.config.stream}, "
            f"group={self.config.group}, consumer={self.config.consumer}, "
            f"concurrency={self.config.concurrency})"
        )

        self._running = True
        registered = False
        joined = False
        try:
            worker_timeout_ms = int(self._parent_client._timeout * 1000)
            self._client = FloClient(
                self._parent_client._endpoint,
                namespace=self._parent_client.namespace,
                debug=self._parent_client._debug,
                timeout_ms=worker_timeout_ms,
                retry=self._parent_client._retry,
            )
            await self._client.connect()

            # Acks, nacks, heartbeats and drain notices get their own
            # connection: the polling one is held by a group_read for up to
            # block_ms, and an ack queued behind it can arrive after the
            # group's ack timeout and see the record redelivered.
            self._ack_client = FloClient(
                self._parent_client._endpoint,
                namespace=self._parent_client.namespace,
                debug=self._parent_client._debug,
                timeout_ms=worker_timeout_ms,
                retry=self._parent_client._retry,
            )
            await self._ack_client.connect()
            if self._stop_event.is_set():
                return

            # Semaphore must be set up before the join: a connection error
            # during the join triggers _handle_reconnect -> _drain_pending,
            # which acquires the semaphore.
            self._semaphore = asyncio.Semaphore(self.config.concurrency)

            # Deregistered at the end even if this answer is lost: the
            # registration may still have taken.
            registered = True
            acks = self._ack_client
            if not await self._while_halted("Registering", lambda: self._register(acks)):
                return
            self._start_heartbeats()

            # Join consumer group. A connection error here (a stalled or dropped
            # connection at startup) is retried via reconnect rather than
            # killing the worker — mirroring the poll loop and ack retry.
            joined = True
            await self._join_group()
            logger.info(f"Joined consumer group {self.config.group} on stream {self.config.stream}")

            await self._poll_loop()
            if self._drain_event.is_set():
                await self._finish_drain()

        finally:
            # Wait for in-flight tasks
            if self._tasks:
                logger.info(f"Waiting for {len(self._tasks)} tasks to complete...")
                await asyncio.gather(*self._tasks, return_exceptions=True)
            await self._stop_heartbeats()

            # On the ack connection: a stop or drain cut the poll one.
            if joined:
                cfg = self.config
                await self._on_control_once(
                    "Leaving the consumer group",
                    lambda c: c.stream.group_leave(cfg.stream, cfg.group, cfg.consumer),
                )
            if registered:
                wid = self.config.worker_id
                await self._on_control_once("Deregister", lambda c: c.worker.deregister(wid))

            if self._client:
                await self._client.close()
                self._client = None
            if self._ack_client:
                await self._ack_client.close()
                self._ack_client = None

            self._running = False
            self._end_run()
            logger.info("Stream worker stopped")

    @property
    def _worker_id(self) -> str:
        return self.config.worker_id

    def _control(self) -> FloClient | None:
        return self._ack_client

    def _control_generation(self) -> int:
        return self._ack_generation

    async def _reconnect_control(self, failed_generation: int | None = None) -> None:
        await self._reconnect_ack_client(failed_generation)

    def _load(self) -> int:
        return self._running_handlers

    async def _register(self, client: FloClient) -> None:
        """Register this worker, with the same options every time: one
        process, its stream and group."""
        await client.worker.register(
            self.config.worker_id,
            [f"{self.config.stream}/{self.config.group}"],
            WorkerRegisterOptions(
                worker_type=WorkerType.STREAM,
                concurrency=self.config.concurrency,
                machine_id=socket.gethostname(),
            ),
        )

    async def _poll_loop(self) -> None:
        """Main polling loop for stream records."""
        assert self._client is not None
        assert self._semaphore is not None
        loop = asyncio.get_running_loop()
        leave = self._leave_event
        backoff = _EmptyPollBackoff(self.config.block_ms, leave)
        failed = _worker_backoff()
        halted = _halted_backoff()

        while not leave.is_set():
            try:
                # Wait for a slot; a stop or drain ends the wait.
                if not await _acquire_unless_set(self._semaphore, leave):
                    break

                started = loop.time()
                result = await self._client.stream.group_read(
                    self.config.stream,
                    self.config.group,
                    self.config.consumer,
                    StreamGroupReadOptions(
                        count=self.config.batch_size,
                        block_ms=self.config.block_ms,
                    ),
                )
                failed.reset()
                halted.reset()

                if not result.records:
                    self._semaphore.release()
                    await backoff.empty(loop.time() - started)
                    continue
                backoff.reset()

                # Release semaphore before dispatching — each task will
                # acquire its own slot via _process_record.
                self._semaphore.release()

                # A drain handles the whole batch, already delivered here; a
                # stop hands back what it hasn't started.
                await self._dispatch(result.records)
                if self._stop_event.is_set():
                    return

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._semaphore.release()
                if leave.is_set():
                    # A stop or drain interrupted the read. Reconnecting would
                    # also reconnect the ack connection under the handlers.
                    break
                if is_connection_error(e):
                    logger.warning("Stream worker lost connection, reconnecting...")
                    try:
                        if not await self._handle_reconnect():
                            break
                        logger.info("Stream worker reconnected, resuming")
                    except Exception as recon_err:
                        logger.error(f"Stream worker reconnect failed: {recon_err}, retrying...")
                        await _sleep_unless_stopped(leave, 1)
                elif is_shard_halted(e):
                    d = halted.next()
                    logger.warning(f"Stream read refused, shard halted: {e}; retrying in {d:.3f}s")
                    await _sleep_unless_stopped(leave, d)
                else:
                    d = failed.next()
                    logger.error(f"Stream read error: {e}, retrying in {d:.3f}s...")
                    await _sleep_unless_stopped(leave, d)

    async def _dispatch(self, records: list[StreamRecord]) -> None:
        """Hand each record to a handler as a slot frees. On stop, the
        records not yet started are nacked, so the group hands them out again
        now rather than after its ack timeout."""
        assert self._semaphore is not None
        started = 0
        for record in records:
            # A stop ends the wait; a drain handles the whole batch.
            if not await _acquire_unless_set(self._semaphore, self._stop_event):
                break
            task = asyncio.create_task(self._process_record(record))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
            started += 1
        rest = [r.id for r in records[started:]]
        if not rest:
            return
        cfg = self.config
        await self._on_ack_connection(
            f"nack of {len(rest)}",
            rest[0],
            lambda c: c.stream.group_nack(
                cfg.stream, cfg.group, rest, StreamGroupNackOptions(consumer=cfg.consumer)
            ),
        )

    async def _join_group(self) -> None:
        """Join the consumer group, retrying on connection errors.

        A connection error (a stalled or dropped connection at startup) triggers
        a reconnect-and-retry instead of failing the worker. A join the server
        refused without running it (overloaded or unavailable, past the
        client's own retries) is tried again with the worker backoff, and one
        refused because the shard halted every 30 s at most, until an
        operator repairs it. Other errors (e.g. an invalid request) remain
        fatal and are raised.
        """
        assert self._client is not None
        failed = _worker_backoff()
        halted = _halted_backoff()
        leave = self._leave_event
        while True:
            if leave.is_set():
                return
            try:
                await self._client.stream.group_join(
                    self.config.stream,
                    self.config.group,
                    self.config.consumer,
                )
                return
            except Exception as e:
                if is_shard_halted(e):
                    d = halted.next()
                    logger.warning(f"Group join refused, shard halted: {e}; retrying in {d:.3f}s")
                    await _sleep_unless_stopped(leave, d)
                    continue
                if is_refused_unrun(e):
                    d = failed.next()
                    logger.warning(f"Group join refused: {e}, retrying in {d:.3f}s...")
                    await _sleep_unless_stopped(leave, d)
                    continue
                if not is_connection_error(e):
                    raise
                logger.warning(f"Connection lost during group join: {e}, reconnecting...")
                with contextlib.suppress(Exception):
                    if not await self._handle_reconnect():
                        return
                await _sleep_unless_stopped(leave, 1)

    async def _handle_reconnect(self) -> bool:
        """Reconnect, re-join the consumer group, and drain pending entries.
        False if a stop or drain ended the reconnect.

        After re-joining, this consumer's pending (delivered-but-unacked)
        entries are re-processed via a group_claim cursor loop before normal
        reads resume — group_read only returns records past last_delivered_id,
        so without this an in-flight record at crash time is never re-surfaced.
        See ``redeliver_pending_on_reconnect``.

        Only the reconnects end on a stop or drain: the join and the pending
        drain check for one themselves, and cutting them short would leave
        claimed records unhandled, or a nack cut off mid-request.
        """
        assert self._client is not None
        leave = self._leave_event
        if not await _unless_set(leave, self._client.reconnect()):
            return False
        # The ack connection went down with the poll connection (e.g. a server
        # restart) but may not notice until an ack fails on it.
        try:
            if not await _unless_set(leave, self._reconnect_ack_client()):
                return False
        except Exception as e:
            logger.warning(f"Failed to reconnect ack client: {e}")
        await self._client.stream.group_join(
            self.config.stream,
            self.config.group,
            self.config.consumer,
        )
        if self.config.redeliver_pending_on_reconnect:
            await self._drain_pending()
        return True

    async def _drain_pending(self) -> None:
        """Re-process this consumer's pending entries via a group_claim loop.

        Records flow through the same _process_record path (and concurrency
        gate) as live reads, so a handler that succeeds acks and clears the PEL
        entry. Bounded per stream by a max page count so a pathological backlog
        can't wedge the reconnect indefinitely; leftovers are reclaimed on the
        next reconnect or by group_read once acked.
        """
        if self._semaphore is None or self._client is None:
            # start() has not initialized the concurrency gate yet.
            return

        max_pages = 1000  # safety cap: max_pages * batch_size entries
        cursor = StreamID()  # MIN — scan from the start of the PEL
        pages = 0
        while True:
            if self._leave_event.is_set():
                return
            try:
                result = await self._client.stream.group_claim(
                    self.config.stream,
                    self.config.group,
                    self.config.consumer,
                    self.config.claim_min_idle_ms,
                    cursor,
                    self.config.batch_size,
                )
            except Exception as e:
                # Don't fail the reconnect on a drain error; normal reads resume.
                logger.warning(f"Pending drain claim failed: {e}")
                return

            # Same gate as live reads.
            await self._dispatch(result.records)
            if self._stop_event.is_set():
                return

            pages += 1
            if result.done or not result.records or pages >= max_pages:
                return
            cursor = result.next_cursor

    async def _reconnect_ack_client(self, failed_generation: int | None = None) -> None:
        """Reconnect the ack connection, unless it was reconnected since
        `failed_generation` was observed."""
        assert self._ack_client is not None
        async with self._ack_reconnect_lock:
            if failed_generation is not None and failed_generation != self._ack_generation:
                return
            await self._ack_client.reconnect()
            self._ack_generation += 1

    async def _on_ack_connection(
        self,
        op: str,
        record_id: StreamID,
        send: Callable[[FloClient], Awaitable[object]],
    ) -> bool:
        """Send an ack or nack on the ack connection, resending it after a
        reconnect if the connection drops. Never raises; False if dropped."""
        ack_client = self._ack_client
        assert ack_client is not None
        return await _send_result(
            f"{op} for record {record_id}",
            lambda: send(ack_client),
            lambda: self._ack_generation,
            self._reconnect_ack_client,
            self._leave_event,
        )

    async def _process_record(self, record: StreamRecord) -> None:
        """Process a single record: call handler, then ack or nack."""
        assert self._semaphore is not None
        try:
            ctx = StreamContext(
                record=record,
                namespace=self._parent_client.namespace,
                stream=self.config.stream,
                group=self.config.group,
                consumer=self.config.consumer,
            )
            self._running_handlers += 1
            try:
                await asyncio.wait_for(self._handler(ctx), timeout=self.config.message_timeout)
                handled = True
            except Exception as e:
                logger.error(
                    f"Record processing failed (stream={self.config.stream}, id={record.id}): {e}"
                )
                handled = False
            finally:
                self._running_handlers -= 1

            # Outside the handler's error handling: an ack that can't be
            # delivered must not turn into a nack.
            if handled:
                await self._on_ack_connection(
                    "ack",
                    record.id,
                    lambda c: c.stream.group_ack(
                        self.config.stream,
                        self.config.group,
                        [record.id],
                        StreamGroupAckOptions(consumer=self.config.consumer),
                    ),
                )
            else:
                await self._on_ack_connection(
                    "nack",
                    record.id,
                    lambda c: c.stream.group_nack(
                        self.config.stream,
                        self.config.group,
                        [record.id],
                        StreamGroupNackOptions(consumer=self.config.consumer),
                    ),
                )
        except Exception as e:
            logger.error(f"Failed to process record: {e}")
        finally:
            self._semaphore.release()

    def stop(self) -> None:
        """Signal the stream worker to stop.

        Ends polling, interrupting a blocking read or a reconnect, and a
        drain's wait; records of the last batch not yet started are nacked.
        Running handlers finish and ack before start() returns. A stop before
        start() ends that run at once.
        """
        logger.info("Stopping stream worker...")
        self._signal_stop()

    async def close(self) -> None:
        """Stop and close the stream worker."""
        self.stop()
        if self._client:
            await self._client.close()
        if self._ack_client:
            await self._ack_client.close()

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
