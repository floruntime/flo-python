"""Flo Workflow Operations

Workflow operations: create, start, signal, cancel, status, history,
list runs, list definitions, disable, enable, sync.
"""

from __future__ import annotations

import os
import re
import struct
from typing import TYPE_CHECKING, Any

from .exceptions import raise_for_status
from .types import (
    OpCode,
    StatusCode,
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

if TYPE_CHECKING:
    from .client import FloClient


class WorkflowOperations:
    """Workflow operations for the Flo client."""

    def __init__(self, client: FloClient) -> None:
        self._client = client

    # =========================================================================
    # Core Operations
    # =========================================================================

    async def create(
        self, name: str, yaml: str | bytes, options: WorkflowCreateOptions | None = None
    ) -> None:
        """Create (or replace) a workflow from a YAML definition."""
        opts = options or WorkflowCreateOptions()
        namespace = self._client.get_namespace(opts.namespace)

        yaml_bytes = yaml.encode("utf-8") if isinstance(yaml, str) else yaml

        await self._client._send_and_check(
            OpCode.WORKFLOW_CREATE,
            namespace,
            name.encode("utf-8"),
            yaml_bytes,
        )

    async def get_definition(
        self, name: str, options: WorkflowGetDefinitionOptions | None = None
    ) -> str | None:
        """Get the YAML definition of a workflow. Returns None if not found."""
        opts = options or WorkflowGetDefinitionOptions()
        namespace = self._client.get_namespace(opts.namespace)

        value = opts.version.encode("utf-8") if opts.version else b""

        resp = await self._client._send_and_check(
            OpCode.WORKFLOW_GET_DEFINITION,
            namespace,
            name.encode("utf-8"),
            value,
            allow_not_found=True,
        )

        if resp.status == StatusCode.NOT_FOUND:
            return None

        return resp.data.decode("utf-8")

    async def start(
        self,
        name: str,
        input_data: str | bytes | None = None,
        options: WorkflowStartOptions | None = None,
    ) -> str:
        """Start a workflow run. Returns the run ID."""
        opts = options or WorkflowStartOptions()
        namespace = self._client.get_namespace(opts.namespace)

        input_bytes = b""
        if input_data is not None:
            input_bytes = (
                input_data.encode("utf-8") if isinstance(input_data, str) else input_data
            )

        # Wire format: [ver_len:u16][ver]?[has_idem:u8][idem_len:u16]?[idem]?
        #              [has_rid:u8][rid_len:u16]?[rid]?[input...]
        parts = bytearray()

        # Version prefix
        if opts.version:
            ver_bytes = opts.version.encode("utf-8")
            parts.extend(struct.pack("<H", len(ver_bytes)))
            parts.extend(ver_bytes)
        else:
            parts.extend(struct.pack("<H", 0))

        # Idempotency key
        if opts.idempotency_key:
            idem_bytes = opts.idempotency_key.encode("utf-8")
            parts.append(1)
            parts.extend(struct.pack("<H", len(idem_bytes)))
            parts.extend(idem_bytes)
        else:
            parts.append(0)

        # Explicit run ID
        if opts.run_id:
            rid_bytes = opts.run_id.encode("utf-8")
            parts.append(1)
            parts.extend(struct.pack("<H", len(rid_bytes)))
            parts.extend(rid_bytes)
        else:
            parts.append(0)

        # Input payload
        parts.extend(input_bytes)

        resp = await self._client._send_and_check(
            OpCode.WORKFLOW_START,
            namespace,
            name.encode("utf-8"),
            bytes(parts),
        )

        return resp.data.decode("utf-8")

    async def status(
        self, run_id: str, options: WorkflowStatusOptions | None = None
    ) -> dict[str, Any]:
        """Get the status of a workflow run. Returns parsed binary status."""
        import struct

        opts = options or WorkflowStatusOptions()
        namespace = self._client.get_namespace(opts.namespace)

        resp = await self._client._send_and_check(
            OpCode.WORKFLOW_STATUS,
            namespace,
            run_id.encode("utf-8"),
            b"",
        )

        data = resp.data
        pos = 0
        status_names = [
            "pending", "running", "waiting", "completed",
            "failed", "cancelled", "timed_out",
        ]

        def read_u16() -> int:
            nonlocal pos
            (v,) = struct.unpack_from("<H", data, pos)
            pos += 2
            return v

        def read_str() -> str:
            n = read_u16()
            nonlocal pos
            s = data[pos : pos + n].decode("utf-8")
            pos += n
            return s

        parsed_run_id = read_str()
        workflow = read_str()
        version = read_str()

        status_byte = data[pos]
        pos += 1
        status_str = status_names[status_byte] if status_byte < len(status_names) else f"unknown({status_byte})"

        current_step = read_str()

        (input_len,) = struct.unpack_from("<I", data, pos)
        pos += 4
        input_data = data[pos : pos + input_len]
        pos += input_len

        (created_at,) = struct.unpack_from("<q", data, pos)
        pos += 8

        result: dict[str, Any] = {
            "run_id": parsed_run_id,
            "workflow": workflow,
            "version": version,
            "status": status_str,
            "current_step": current_step,
            "input": input_data,
            "created_at": created_at,
        }

        # Optional: started_at
        if pos < len(data) and data[pos] == 1:
            pos += 1
            (started_at,) = struct.unpack_from("<q", data, pos)
            pos += 8
            result["started_at"] = started_at
        elif pos < len(data):
            pos += 1

        # Optional: completed_at
        if pos < len(data) and data[pos] == 1:
            pos += 1
            (completed_at,) = struct.unpack_from("<q", data, pos)
            pos += 8
            result["completed_at"] = completed_at
        elif pos < len(data):
            pos += 1

        # Optional: wait_signal
        if pos < len(data) and data[pos] == 1:
            pos += 1
            result["wait_signal"] = read_str()

        return result

    async def signal(
        self,
        run_id: str,
        signal_name: str,
        data: str | bytes | None = None,
        options: WorkflowSignalOptions | None = None,
    ) -> None:
        """Send a signal to a running workflow."""
        opts = options or WorkflowSignalOptions()
        namespace = self._client.get_namespace(opts.namespace)

        sig_bytes = signal_name.encode("utf-8")
        data_bytes = b""
        if data is not None:
            data_bytes = data.encode("utf-8") if isinstance(data, str) else data

        value = bytearray()
        value.extend(struct.pack("<H", len(sig_bytes)))
        value.extend(sig_bytes)
        value.extend(data_bytes)

        await self._client._send_and_check(
            OpCode.WORKFLOW_SIGNAL,
            namespace,
            run_id.encode("utf-8"),
            bytes(value),
        )

    async def cancel(
        self,
        run_id: str,
        reason: str | None = None,
        options: WorkflowCancelOptions | None = None,
    ) -> None:
        """Cancel a running workflow."""
        opts = options or WorkflowCancelOptions()
        namespace = self._client.get_namespace(opts.namespace)

        value = reason.encode("utf-8") if reason else b""

        await self._client._send_and_check(
            OpCode.WORKFLOW_CANCEL,
            namespace,
            run_id.encode("utf-8"),
            value,
        )

    async def history(
        self, run_id: str, options: WorkflowHistoryOptions | None = None
    ) -> bytes:
        """Get the execution history of a workflow run. Returns raw response bytes."""
        opts = options or WorkflowHistoryOptions()
        namespace = self._client.get_namespace(opts.namespace)

        value = struct.pack("<I", opts.limit)

        resp = await self._client._send_and_check(
            OpCode.WORKFLOW_HISTORY,
            namespace,
            run_id.encode("utf-8"),
            value,
            allow_not_found=True,
        )

        return resp.data

    async def list_runs(
        self, options: WorkflowListRunsOptions | None = None
    ) -> bytes:
        """List workflow runs. Returns raw response bytes."""
        opts = options or WorkflowListRunsOptions()
        namespace = self._client.get_namespace(opts.namespace)

        key = opts.workflow_name.encode("utf-8") if opts.workflow_name else b""

        # Value: [limit:u32][status_len:u16][status]?
        status_bytes = opts.status_filter.encode("utf-8") if opts.status_filter else b""
        value = bytearray()
        value.extend(struct.pack("<I", opts.limit))
        value.extend(struct.pack("<H", len(status_bytes)))
        value.extend(status_bytes)

        resp = await self._client._send_and_check(
            OpCode.WORKFLOW_LIST_RUNS,
            namespace,
            key,
            bytes(value),
        )

        return resp.data

    async def list_definitions(
        self, options: WorkflowListDefinitionsOptions | None = None
    ) -> bytes:
        """List workflow definitions. Returns raw response bytes."""
        opts = options or WorkflowListDefinitionsOptions()
        namespace = self._client.get_namespace(opts.namespace)

        value = struct.pack("<I", opts.limit)

        resp = await self._client._send_and_check(
            OpCode.WORKFLOW_LIST_DEFINITIONS,
            namespace,
            b"",
            value,
        )

        return resp.data

    async def disable(
        self, name: str, options: WorkflowDisableOptions | None = None
    ) -> None:
        """Disable a workflow definition (prevents new runs)."""
        opts = options or WorkflowDisableOptions()
        namespace = self._client.get_namespace(opts.namespace)

        await self._client._send_and_check(
            OpCode.WORKFLOW_DISABLE,
            namespace,
            name.encode("utf-8"),
            b"",
        )

    async def enable(
        self, name: str, options: WorkflowEnableOptions | None = None
    ) -> None:
        """Re-enable a disabled workflow definition."""
        opts = options or WorkflowEnableOptions()
        namespace = self._client.get_namespace(opts.namespace)

        await self._client._send_and_check(
            OpCode.WORKFLOW_ENABLE,
            namespace,
            name.encode("utf-8"),
            b"",
        )

    # =========================================================================
    # Declarative Sync
    # =========================================================================

    async def sync(
        self, yaml: str, options: WorkflowSyncOptions | None = None
    ) -> WorkflowSyncResult:
        """Declarative, idempotent sync of a workflow YAML string.

        Safe to call on every startup. Compares versions:
        - Not found → creates
        - Same version → no-op ("unchanged")
        - Different version → updates (upsert)
        """
        opts = options or WorkflowSyncOptions()
        name, version = _extract_workflow_meta(yaml)
        namespace = self._client.get_namespace(opts.namespace)

        existing = await self.get_definition(
            name, WorkflowGetDefinitionOptions(namespace=namespace)
        )

        if existing is not None:
            existing_version = _extract_yaml_field(existing, "version")
            if existing_version == version:
                return WorkflowSyncResult(name=name, version=version, action="unchanged")

        await self.create(name, yaml, WorkflowCreateOptions(namespace=namespace))

        action = "updated" if existing is not None else "created"
        return WorkflowSyncResult(name=name, version=version, action=action)

    async def sync_bytes(
        self, yaml: bytes, options: WorkflowSyncOptions | None = None
    ) -> WorkflowSyncResult:
        """Sync raw YAML bytes."""
        return await self.sync(yaml.decode("utf-8"), options)

    async def sync_dir(
        self, dir_path: str, options: WorkflowSyncOptions | None = None
    ) -> list[WorkflowSyncResult]:
        """Sync all YAML files in a directory."""
        results: list[WorkflowSyncResult] = []
        for entry in sorted(os.listdir(dir_path)):
            if entry.endswith((".yaml", ".yml")):
                file_path = os.path.join(dir_path, entry)
                with open(file_path) as f:
                    yaml_content = f.read()
                result = await self.sync(yaml_content, options)
                results.append(result)
        return results


# =============================================================================
# YAML Metadata Extraction (lightweight — no full parser needed)
# =============================================================================

_YAML_FIELD_RE = re.compile(r"""^\s*['"]?(\w+)['"]?\s*:\s*['"]?([^'"#\n]+?)['"]?\s*(?:#.*)?$""")


def _extract_yaml_field(yaml: str, field: str) -> str | None:
    """Extract a top-level scalar field from YAML."""
    for line in yaml.split("\n"):
        m = _YAML_FIELD_RE.match(line)
        if m and m.group(1) == field:
            return m.group(2).strip()
    return None


def _extract_workflow_meta(yaml: str) -> tuple[str, str]:
    """Extract name and version from workflow YAML."""
    name = _extract_yaml_field(yaml, "name")
    version = _extract_yaml_field(yaml, "version")
    if not name:
        raise ValueError("flo: workflow YAML missing required 'name' field")
    if not version:
        raise ValueError("flo: workflow YAML missing required 'version' field")
    return name, version
