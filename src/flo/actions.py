"""Flo Actions - Low-Level Action/Worker Protocol

This module provides low-level operations for actions and workers:
- Action registration, invocation, and status checking
- Worker registration, task awaiting, completion, and failure reporting

For a higher-level API, see `Worker` in worker.py.
"""

from typing import TYPE_CHECKING

from .kv_body import (
    decode_action_info_answer,
    decode_action_invoke_answer,
    decode_action_list_answer,
    decode_action_status_answer,
    decode_await_answer,
    decode_empty_answer,
    decode_touch_answer,
    decode_worker_info_answer,
    decode_worker_list_answer,
    encode_await_body,
    encode_complete_body,
    encode_fail_body,
    encode_touch_body,
    encode_worker_list_body,
)
from .types import (
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
    OpCode,
    TaskAssignment,
    WorkerAwaitOptions,
    WorkerAwaitResult,
    WorkerCompleteOptions,
    WorkerDetails,
    WorkerFailOptions,
    WorkerListOptions,
    WorkerListResult,
    WorkerRegisterOptions,
    WorkerStatus,
)
from .wire import (
    serialize_action_invoke_value,
    serialize_action_list_value,
    serialize_action_register_value,
    serialize_worker_register_value,
)

if TYPE_CHECKING:
    from .client import FloClient


class ActionOperations:
    """Action operations for the Flo client.

    Provides low-level operations for registering and invoking actions.
    For a higher-level API, see `Worker` in worker.py.
    """

    def __init__(self, client: "FloClient") -> None:
        self._client = client

    async def register(
        self,
        name: str,
        action_type: ActionType = ActionType.USER,
        options: ActionRegisterOptions | None = None,
    ) -> None:
        """Register an action.

        Args:
            name: Action name.
            action_type: Type of action (USER only).
            options: Optional registration options.
        """
        opts = options or ActionRegisterOptions()
        namespace = self._client.get_namespace(opts.namespace)

        value = serialize_action_register_value(
            action_type=action_type,
            timeout_ms=opts.timeout_ms,
            max_attempts=opts.max_attempts,
            owner=opts.owner,
        )

        await self._client._send_and_check(
            OpCode.ACTION_REGISTER,
            namespace,
            name.encode("utf-8"),
            value,
        )

    async def invoke(
        self,
        name: str,
        input_data: bytes,
        options: ActionInvokeOptions | None = None,
    ) -> ActionInvokeResult:
        """Invoke an action.

        Args:
            name: Action name to invoke.
            input_data: Input data for the action.
            options: Optional invoke options.

        Returns:
            ActionInvokeResult with run_id.
        """
        opts = options or ActionInvokeOptions()
        namespace = self._client.get_namespace(opts.namespace)

        value = serialize_action_invoke_value(
            input_data=input_data,
            labels=opts.labels,
        )

        response = await self._client._send_and_check(
            OpCode.ACTION_INVOKE,
            namespace,
            name.encode("utf-8"),
            value,
        )

        return ActionInvokeResult(run_id=decode_action_invoke_answer(response.data))

    async def status(
        self,
        run_id: str,
        options: ActionStatusOptions | None = None,
    ) -> ActionRunStatus:
        """Get action run status.

        Args:
            run_id: The run ID to check.
            options: Optional status options.

        Returns:
            ActionRunStatus with current status.
        """
        opts = options or ActionStatusOptions()
        namespace = self._client.get_namespace(opts.namespace)

        response = await self._client._send_and_check(
            OpCode.ACTION_STATUS,
            namespace,
            run_id.encode("utf-8"),
            b"",
        )

        return decode_action_status_answer(run_id, response.data)

    async def list(
        self,
        options: ActionListOptions | None = None,
    ) -> ActionListResult:
        """List a page of the namespace's registered actions.

        Args:
            options: The page's limit and cursor (the last page's ``cursor``;
                None starts).

        Returns:
            ActionListResult with the page's action names and the next
            page's cursor, None on the last page.
        """
        opts = options or ActionListOptions()
        namespace = self._client.get_namespace(opts.namespace)

        response = await self._client._send_and_check(
            OpCode.ACTION_LIST,
            namespace,
            (opts.prefix or "").encode("utf-8"),
            serialize_action_list_value(limit=opts.limit, cursor=opts.cursor),
        )

        actions, cursor = decode_action_list_answer(response.data)
        return ActionListResult(actions=actions, cursor=cursor)

    async def info(self, name: str, namespace: str | None = None) -> ActionInfo:
        """An action's definition, its runs by status and how long its
        completed runs took.

        Raises:
            NotFoundError: No action has that name.
        """
        response = await self._client._send_and_check(
            OpCode.ACTION_INFO,
            self._client.get_namespace(namespace),
            name.encode("utf-8"),
            b"",
        )
        return decode_action_info_answer(response.data)

    async def delete(
        self,
        name: str,
        options: ActionDeleteOptions | None = None,
    ) -> None:
        """Delete an action.

        Args:
            name: Action name to delete.
            options: Optional delete options.
        """
        opts = options or ActionDeleteOptions()
        namespace = self._client.get_namespace(opts.namespace)

        await self._client._send_and_check(
            OpCode.ACTION_DELETE,
            namespace,
            name.encode("utf-8"),
            b"",
        )


class WorkerOperations:
    """Worker operations for the Flo client.

    Provides low-level operations for worker registration and task processing.
    For a higher-level API, see `Worker` in worker.py.
    """

    def __init__(self, client: "FloClient") -> None:
        self._client = client

    async def register(
        self,
        worker_id: str,
        task_types: list[str],
        options: WorkerRegisterOptions | None = None,
    ) -> None:
        """Register a worker.

        Args:
            worker_id: Unique worker identifier.
            task_types: List of task types this worker can handle.
            options: Optional registration options.
        """
        opts = options or WorkerRegisterOptions()
        namespace = self._client.get_namespace(opts.namespace)

        value = serialize_worker_register_value(
            task_types,
            worker_type=int(opts.worker_type),
            max_concurrency=opts.concurrency,
            metadata=opts.metadata,
            machine_id=opts.machine_id,
        )

        await self._client._send_and_check(
            OpCode.WORKER_REGISTER,
            namespace,
            worker_id.encode("utf-8"),
            value,
        )

    async def heartbeat(
        self,
        worker_id: str,
        current_load: int = 0,
        namespace: str | None = None,
    ) -> WorkerStatus:
        """Send a worker heartbeat to keep the registration alive.

        Args:
            worker_id: Worker identifier.
            current_load: Current number of active tasks.
            namespace: Optional namespace override.

        Returns:
            The worker's status on the server: DRAINING once it is drained.

        Raises:
            NotFoundError: The server doesn't know the worker (it never
                registered, or was removed after going silent).
        """
        import struct

        ns = self._client.get_namespace(namespace)
        value = struct.pack("<I", current_load)
        response = await self._client._send_and_check(
            OpCode.WORKER_HEARTBEAT,
            ns,
            worker_id.encode("utf-8"),
            value,
        )
        if len(response.data) != 1:
            raise ValueError(f"heartbeat answer is {len(response.data)} bytes, want 1")
        return WorkerStatus(response.data[0])

    async def drain(self, worker_id: str, namespace: str | None = None) -> None:
        """Mark a worker draining: the server gives it no more work, and
        its heartbeats answer DRAINING until it deregisters.

        Raises:
            NotFoundError: The server doesn't know the worker.
        """
        await self._client._send_and_check(
            OpCode.WORKER_DRAIN,
            self._client.get_namespace(namespace),
            worker_id.encode("utf-8"),
            b"",
        )

    async def deregister(self, worker_id: str, namespace: str | None = None) -> None:
        """Remove a worker from the registry."""
        await self._client._send_and_check(
            OpCode.WORKER_DEREGISTER,
            self._client.get_namespace(namespace),
            worker_id.encode("utf-8"),
            b"",
        )

    async def await_task(
        self,
        worker_id: str,
        actions: list[str],
        options: WorkerAwaitOptions | None = None,
    ) -> WorkerAwaitResult:
        """Wait up to ``block_ms`` for a run of one of ``actions`` (the first
        tried first), and take it.

        Returns:
            WorkerAwaitResult whose task is None if no run was ready in time.
        """
        opts = options or WorkerAwaitOptions()
        response = await self._client._send_and_check(
            OpCode.ACTION_AWAIT,
            self._client.get_namespace(opts.namespace),
            worker_id.encode("utf-8"),
            encode_await_body(actions, opts.block_ms, opts.request_key),
        )
        return WorkerAwaitResult(task=decode_await_answer(response.data))

    async def touch(self, worker_id: str, task: TaskAssignment) -> int:
        """Keep a task. With half its lease or less left, its lease runs a
        full lease from now (up to 12 h from its delivery); with more left,
        nothing changes. Touch at least every half lease.

        Returns:
            When the lease runs out without another touch, in Unix ms.

        Raises:
            StaleReportError: The run moved on from this delivery.
        """
        response = await self._client._send_and_check(
            OpCode.ACTION_TOUCH,
            task.namespace,
            task.run.encode("utf-8"),
            encode_touch_body(worker_id, task),
        )
        return decode_touch_answer(response.data)

    async def complete(
        self,
        worker_id: str,
        task: TaskAssignment,
        result: bytes = b"",
        options: WorkerCompleteOptions | None = None,
    ) -> None:
        """Complete a task: its run is done, with this result.

        Sending it again after it applied answers ok, so a report whose
        answer was lost can be resent unchanged.

        Raises:
            StaleReportError: The run moved on from this delivery, or was
                completed with another result.
        """
        opts = options or WorkerCompleteOptions()
        response = await self._client._send_and_check(
            OpCode.ACTION_COMPLETE,
            task.namespace,
            task.run.encode("utf-8"),
            encode_complete_body(worker_id, task, opts.outcome, result),
        )
        decode_empty_answer("action.complete", response.data)

    async def fail(
        self,
        worker_id: str,
        task: TaskAssignment,
        error: str = "",
        options: WorkerFailOptions | None = None,
    ) -> None:
        """Fail a task: its run is ready again after ``delay_ms``, unless its
        attempts are spent or ``retry`` is False. With ``abandoned`` it is
        handed back untried, and no failure is counted.

        Sending it again after it applied answers ok.

        Raises:
            StaleReportError: The run moved on from this delivery.
        """
        opts = options or WorkerFailOptions()
        response = await self._client._send_and_check(
            OpCode.ACTION_FAIL,
            task.namespace,
            task.run.encode("utf-8"),
            encode_fail_body(
                worker_id, task, error.encode("utf-8"), opts.retry, opts.abandoned, opts.delay_ms
            ),
        )
        decode_empty_answer("action.fail", response.data)

    async def list(
        self,
        options: WorkerListOptions | None = None,
    ) -> WorkerListResult:
        """List a page of the namespace's workers, in id order.

        Args:
            options: The page's limit and cursor (the last page's ``cursor``;
                None starts).

        Returns:
            WorkerListResult with the page's workers and the next page's
            cursor, None on the last page.
        """
        opts = options or WorkerListOptions()
        namespace = self._client.get_namespace(opts.namespace)

        response = await self._client._send_and_check(
            OpCode.WORKER_LIST,
            namespace,
            b"",
            encode_worker_list_body(opts.limit, opts.cursor, opts.action),
        )

        workers, cursor = decode_worker_list_answer(response.data)
        return WorkerListResult(workers=workers, cursor=cursor)

    async def info(self, worker_id: str, namespace: str | None = None) -> WorkerDetails:
        """A worker's record: its row, as :meth:`list` shows it, the metadata
        it registered with, and its processes with their run and fail counts.

        Raises:
            NotFoundError: The server doesn't know the worker.
        """
        response = await self._client._send_and_check(
            OpCode.WORKER_INFO,
            self._client.get_namespace(namespace),
            worker_id.encode("utf-8"),
            b"",
        )
        return decode_worker_info_answer(response.data)
