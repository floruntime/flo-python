"""Flo Actions - Low-Level Action/Worker Protocol

This module provides low-level operations for actions and workers:
- Action registration, invocation, and status checking
- Worker registration, task awaiting, completion, and failure reporting

For a higher-level API, see `Worker` in worker.py.
"""

from typing import TYPE_CHECKING, Optional

from .types import (
    ActionDeleteOptions,
    ActionInvokeOptions,
    ActionInvokeResult,
    ActionListOptions,
    ActionListResult,
    ActionRegisterOptions,
    ActionRunStatus,
    ActionStatusOptions,
    ActionType,
    OpCode,
    OptionTag,
    WorkerAwaitOptions,
    WorkerAwaitResult,
    WorkerCompleteOptions,
    WorkerFailOptions,
    WorkerListOptions,
    WorkerListResult,
    WorkerRegisterOptions,
    WorkerTouchOptions,
)
from .wire import (
    OptionsBuilder,
    parse_task_assignment,
    serialize_action_invoke_value,
    serialize_action_list_value,
    serialize_action_register_value,
    serialize_worker_await_value,
    serialize_worker_complete_value,
    serialize_worker_fail_value,
    serialize_worker_list_value,
    serialize_worker_register_value,
    serialize_worker_touch_value,
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
        options: Optional[ActionRegisterOptions] = None,
    ) -> None:
        """Register an action.

        Args:
            name: Action name.
            action_type: Type of action (USER or WASM).
            options: Optional registration options.
        """
        opts = options or ActionRegisterOptions()
        namespace = self._client.get_namespace(opts.namespace)

        value = serialize_action_register_value(
            action_type=action_type,
            timeout_ms=opts.timeout_ms,
            max_retries=opts.max_retries,
            description=opts.description,
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
        options: Optional[ActionInvokeOptions] = None,
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
            priority=opts.priority,
            idempotency_key=opts.idempotency_key,
        )

        response = await self._client._send_and_check(
            OpCode.ACTION_INVOKE,
            namespace,
            name.encode("utf-8"),
            value,
        )

        # Response contains run_id as string
        run_id = response.data.decode("utf-8") if response.data else ""
        return ActionInvokeResult(run_id=run_id)

    async def status(
        self,
        run_id: str,
        options: Optional[ActionStatusOptions] = None,
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

        # Parse status response - format TBD based on server implementation
        # For now return basic status
        return ActionRunStatus(
            run_id=run_id,
            status="unknown",
            result=response.data if response.data else None,
        )

    async def list(
        self,
        options: Optional[ActionListOptions] = None,
    ) -> ActionListResult:
        """List registered actions.

        Args:
            options: Optional list options.

        Returns:
            ActionListResult with list of actions.
        """
        opts = options or ActionListOptions()
        namespace = self._client.get_namespace(opts.namespace)

        value = serialize_action_list_value(limit=opts.limit)

        await self._client._send_and_check(
            OpCode.ACTION_LIST,
            namespace,
            (opts.prefix or "").encode("utf-8"),
            value,
        )

        # Parse list response - format TBD based on server implementation
        return ActionListResult(actions=[])

    async def delete(
        self,
        name: str,
        options: Optional[ActionDeleteOptions] = None,
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
        options: Optional[WorkerRegisterOptions] = None,
    ) -> None:
        """Register a worker.

        Args:
            worker_id: Unique worker identifier.
            task_types: List of task types this worker can handle.
            options: Optional registration options.
        """
        opts = options or WorkerRegisterOptions()
        namespace = self._client.get_namespace(opts.namespace)

        value = serialize_worker_register_value(task_types)

        await self._client._send_and_check(
            OpCode.WORKER_REGISTER,
            namespace,
            worker_id.encode("utf-8"),
            value,
        )

    async def await_task(
        self,
        worker_id: str,
        task_types: list[str],
        options: Optional[WorkerAwaitOptions] = None,
    ) -> WorkerAwaitResult:
        """Wait for a task assignment.

        Args:
            worker_id: Worker identifier.
            task_types: Task types to listen for.
            options: Optional await options (block_ms, timeout_ms).

        Returns:
            WorkerAwaitResult with task if available.
        """
        opts = options or WorkerAwaitOptions()
        namespace = self._client.get_namespace(opts.namespace)

        value = serialize_worker_await_value(task_types)

        # Build options for block_ms and timeout_ms
        options_builder = OptionsBuilder()
        if opts.block_ms is not None:
            options_builder.add_u32(OptionTag.BLOCK_MS, opts.block_ms)
        if opts.timeout_ms is not None:
            options_builder.add_u32(OptionTag.TIMEOUT_MS, opts.timeout_ms)

        response = await self._client._send_and_check(
            OpCode.WORKER_AWAIT,
            namespace,
            worker_id.encode("utf-8"),
            value,
            options_builder.build(),
        )

        # Parse task assignment response - if empty, no task available
        if not response.data:
            return WorkerAwaitResult(task=None)

        # Parse task assignment
        task = parse_task_assignment(response.data)
        return WorkerAwaitResult(task=task)

    async def touch(
        self,
        worker_id: str,
        task_id: str,
        options: Optional[WorkerTouchOptions] = None,
    ) -> None:
        """Extend task lease (heartbeat).

        Args:
            worker_id: Worker identifier.
            task_id: Task identifier to extend.
            options: Optional touch options (extend_ms).
        """
        opts = options or WorkerTouchOptions()
        namespace = self._client.get_namespace(opts.namespace)

        value = serialize_worker_touch_value(task_id, opts.extend_ms)

        await self._client._send_and_check(
            OpCode.WORKER_TOUCH,
            namespace,
            worker_id.encode("utf-8"),
            value,
        )

    async def complete(
        self,
        worker_id: str,
        task_id: str,
        result: bytes,
        options: Optional[WorkerCompleteOptions] = None,
    ) -> None:
        """Complete a task successfully.

        Args:
            worker_id: Worker identifier.
            task_id: Task identifier to complete.
            result: Result data from the task.
            options: Optional complete options.
        """
        opts = options or WorkerCompleteOptions()
        namespace = self._client.get_namespace(opts.namespace)

        value = serialize_worker_complete_value(task_id, result)

        await self._client._send_and_check(
            OpCode.WORKER_COMPLETE,
            namespace,
            worker_id.encode("utf-8"),
            value,
        )

    async def fail(
        self,
        worker_id: str,
        task_id: str,
        error_message: str,
        options: Optional[WorkerFailOptions] = None,
    ) -> None:
        """Fail a task.

        Args:
            worker_id: Worker identifier.
            task_id: Task identifier that failed.
            error_message: Error message describing the failure.
            options: Optional fail options (retry flag).
        """
        opts = options or WorkerFailOptions()
        namespace = self._client.get_namespace(opts.namespace)

        value = serialize_worker_fail_value(task_id, error_message)

        # Retry flag goes in TLV options (matches Go SDK)
        options_builder = OptionsBuilder()
        if opts.retry:
            options_builder.add_flag(OptionTag.RETRY)

        await self._client._send_and_check(
            OpCode.WORKER_FAIL,
            namespace,
            worker_id.encode("utf-8"),
            value,
            options_builder.build(),
        )

    async def list(
        self,
        options: Optional[WorkerListOptions] = None,
    ) -> WorkerListResult:
        """List registered workers.

        Args:
            options: Optional list options.

        Returns:
            WorkerListResult with list of workers.
        """
        opts = options or WorkerListOptions()
        namespace = self._client.get_namespace(opts.namespace)

        value = serialize_worker_list_value(opts.limit)

        await self._client._send_and_check(
            OpCode.WORKER_LIST,
            namespace,
            b"",
            value,
        )

        # Parse list response - format TBD based on server implementation
        return WorkerListResult(workers=[])
