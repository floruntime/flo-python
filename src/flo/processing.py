"""Flo Processing Operations

Stream processing operations: submit, status, list, stop, cancel,
savepoint, restore, rescale, sync.
"""

from __future__ import annotations

import builtins
import os
import re
import struct
from typing import TYPE_CHECKING

from .types import (
    OpCode,
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
)

if TYPE_CHECKING:
    from .client import FloClient

_PROCESSING_STATUS_NAMES = ["running", "stopped", "cancelled", "failed", "completed"]


class ProcessingOperations:
    """Processing operations for the Flo client."""

    def __init__(self, client: FloClient) -> None:
        self._client = client

    # =========================================================================
    # Core Operations
    # =========================================================================

    async def submit(
        self, yaml: str | bytes, options: ProcessingSubmitOptions | None = None
    ) -> str:
        """Submit a processing job from a YAML definition. Returns the job ID."""
        opts = options or ProcessingSubmitOptions()
        namespace = self._client.get_namespace(opts.namespace)

        yaml_bytes = yaml.encode("utf-8") if isinstance(yaml, str) else yaml

        resp = await self._client._send_and_check(
            OpCode.PROCESSING_SUBMIT,
            namespace,
            b"",
            yaml_bytes,
        )

        return resp.data.decode("utf-8")

    async def status(
        self, job_id: str, options: ProcessingStatusOptions | None = None
    ) -> ProcessingStatusResult | None:
        """Get the status of a processing job. Returns None if not found."""
        opts = options or ProcessingStatusOptions()
        namespace = self._client.get_namespace(opts.namespace)

        resp = await self._client._send_and_check(
            OpCode.PROCESSING_STATUS,
            namespace,
            job_id.encode("utf-8"),
            b"",
            allow_not_found=True,
        )

        from .types import StatusCode

        if resp.status == StatusCode.NOT_FOUND:
            return None

        return _parse_processing_status(resp.data)

    async def list(self, options: ProcessingListOptions | None = None) -> list[ProcessingListEntry]:
        """List processing jobs."""
        opts = options or ProcessingListOptions()
        namespace = self._client.get_namespace(opts.namespace)

        # Wire format: [limit:u32][cursor...]
        cursor = opts.cursor or b""
        value = struct.pack("<I", opts.limit) + cursor

        resp = await self._client._send_and_check(
            OpCode.PROCESSING_LIST,
            namespace,
            b"",
            value,
        )

        return _parse_processing_list(resp.data)

    async def stop(self, job_id: str, options: ProcessingStopOptions | None = None) -> None:
        """Gracefully stop a processing job."""
        opts = options or ProcessingStopOptions()
        namespace = self._client.get_namespace(opts.namespace)

        await self._client._send_and_check(
            OpCode.PROCESSING_STOP,
            namespace,
            job_id.encode("utf-8"),
            b"",
        )

    async def cancel(self, job_id: str, options: ProcessingCancelOptions | None = None) -> None:
        """Force-cancel a processing job."""
        opts = options or ProcessingCancelOptions()
        namespace = self._client.get_namespace(opts.namespace)

        await self._client._send_and_check(
            OpCode.PROCESSING_CANCEL,
            namespace,
            job_id.encode("utf-8"),
            b"",
        )

    async def savepoint(
        self, job_id: str, options: ProcessingSavepointOptions | None = None
    ) -> str:
        """Trigger a savepoint for a processing job. Returns the savepoint ID."""
        opts = options or ProcessingSavepointOptions()
        namespace = self._client.get_namespace(opts.namespace)

        resp = await self._client._send_and_check(
            OpCode.PROCESSING_SAVEPOINT,
            namespace,
            job_id.encode("utf-8"),
            b"",
        )

        return resp.data.decode("utf-8")

    async def restore(
        self,
        job_id: str,
        savepoint_id: str,
        options: ProcessingRestoreOptions | None = None,
    ) -> None:
        """Restore a processing job from a savepoint."""
        opts = options or ProcessingRestoreOptions()
        namespace = self._client.get_namespace(opts.namespace)

        await self._client._send_and_check(
            OpCode.PROCESSING_RESTORE,
            namespace,
            job_id.encode("utf-8"),
            savepoint_id.encode("utf-8"),
        )

    async def rescale(
        self,
        job_id: str,
        parallelism: int,
        options: ProcessingRescaleOptions | None = None,
    ) -> None:
        """Change the parallelism of a processing job."""
        opts = options or ProcessingRescaleOptions()
        namespace = self._client.get_namespace(opts.namespace)

        value = struct.pack("<I", parallelism)

        await self._client._send_and_check(
            OpCode.PROCESSING_RESCALE,
            namespace,
            job_id.encode("utf-8"),
            value,
        )

    # =========================================================================
    # Declarative Sync
    # =========================================================================

    async def sync(
        self, yaml_path: str, options: ProcessingSyncOptions | None = None
    ) -> ProcessingSyncResult:
        """Sync a processing job from a YAML file. Returns name + job ID."""
        with open(yaml_path) as f:
            yaml_content = f.read()
        return await self.sync_bytes(yaml_content.encode("utf-8"), options)

    async def sync_bytes(
        self, yaml: bytes, options: ProcessingSyncOptions | None = None
    ) -> ProcessingSyncResult:
        """Sync raw YAML bytes. Submits a new job and returns name + job ID."""
        opts = options or ProcessingSyncOptions()
        name = _extract_processing_meta(yaml)

        submit_opts = ProcessingSubmitOptions(namespace=opts.namespace)
        job_id = await self.submit(yaml, submit_opts)

        return ProcessingSyncResult(name=name, job_id=job_id)

    async def sync_dir(
        self, dir_path: str, options: ProcessingSyncOptions | None = None
    ) -> builtins.list[ProcessingSyncResult]:
        """Sync all YAML files in a directory."""
        results: builtins.list[ProcessingSyncResult] = []
        for entry in sorted(os.listdir(dir_path)):
            if entry.endswith((".yaml", ".yml")):
                file_path = os.path.join(dir_path, entry)
                result = await self.sync(file_path, options)
                results.append(result)
        return results


# =============================================================================
# Wire Format Parsers
# =============================================================================


def _parse_processing_status(data: bytes) -> ProcessingStatusResult:
    """Parse binary wire format for processing job status.

    Wire format: [job_id_len:u16][job_id][name_len:u16][name][status:u8]
                 [parallelism:u32][batch_size:u32][records_processed:u64][created_at:i64]
    """
    pos = 0

    def read_u16() -> int:
        nonlocal pos
        v: int = struct.unpack_from("<H", data, pos)[0]
        pos += 2
        return v

    def read_str() -> str:
        n = read_u16()
        nonlocal pos
        s = data[pos : pos + n].decode("utf-8")
        pos += n
        return s

    job_id = read_str()
    name = read_str()

    status_byte = data[pos]
    pos += 1
    status_str = (
        _PROCESSING_STATUS_NAMES[status_byte]
        if status_byte < len(_PROCESSING_STATUS_NAMES)
        else f"unknown({status_byte})"
    )

    (parallelism,) = struct.unpack_from("<I", data, pos)
    pos += 4

    (batch_size,) = struct.unpack_from("<I", data, pos)
    pos += 4

    (records_processed,) = struct.unpack_from("<Q", data, pos)
    pos += 8

    (created_at,) = struct.unpack_from("<q", data, pos)

    return ProcessingStatusResult(
        job_id=job_id,
        name=name,
        status=status_str,
        parallelism=parallelism,
        batch_size=batch_size,
        records_processed=records_processed,
        created_at=created_at,
    )


def _parse_processing_list(data: bytes) -> list[ProcessingListEntry]:
    """Parse binary wire format for processing job list.

    Wire format: [count:u32]([name_len:u16][name][job_id_len:u16][job_id]
                 [status_len:u16][status][parallelism:u32][created_at:i64])*
    """
    if len(data) < 4:
        return []

    (count,) = struct.unpack_from("<I", data, 0)
    pos = 4
    results: list[ProcessingListEntry] = []

    def read_u16() -> int:
        nonlocal pos
        v: int = struct.unpack_from("<H", data, pos)[0]
        pos += 2
        return v

    def read_str() -> str:
        n = read_u16()
        nonlocal pos
        s = data[pos : pos + n].decode("utf-8")
        pos += n
        return s

    for _ in range(count):
        name = read_str()
        job_id = read_str()
        status = read_str()

        (parallelism,) = struct.unpack_from("<I", data, pos)
        pos += 4

        (created_at,) = struct.unpack_from("<q", data, pos)
        pos += 8

        results.append(
            ProcessingListEntry(
                name=name,
                job_id=job_id,
                status=status,
                parallelism=parallelism,
                created_at=created_at,
            )
        )

    return results


# =============================================================================
# YAML Metadata Extraction
# =============================================================================

_YAML_FIELD_RE = re.compile(r"""^\s*['"]?(\w+)['"]?\s*:\s*['"]?([^'"#\n]+?)['"]?\s*(?:#.*)?$""")


def _extract_processing_meta(data: bytes) -> str:
    """Extract the job name from processing YAML."""
    text = data.decode("utf-8")
    for line in text.split("\n"):
        m = _YAML_FIELD_RE.match(line)
        if m and m.group(1) == "name":
            return m.group(2).strip()
    raise ValueError("flo: processing YAML missing required 'name' field")
