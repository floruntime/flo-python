"""Flo Processing Operations

Stream processing operations: submit, status, list, stop, cancel, sync.
"""

from __future__ import annotations

import builtins
import os
import re
import struct
from typing import TYPE_CHECKING

from .exceptions import IncompleteResponseError
from .types import (
    OpCode,
    ProcessingCancelOptions,
    ProcessingListEntry,
    ProcessingListOptions,
    ProcessingListResult,
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
# The most jobs one list page holds.
_MAX_PROCESSING_LIST = 1024


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

    async def list(self, options: ProcessingListOptions | None = None) -> ProcessingListResult:
        """List a page of the namespace's processing jobs.

        Args:
            options: The page's limit and cursor (the last page's ``cursor``;
                None starts).

        Returns:
            ProcessingListResult with the page's jobs and the next page's
            cursor, None on the last page.
        """
        opts = options or ProcessingListOptions()
        namespace = self._client.get_namespace(opts.namespace)

        # Wire format: [limit:u32][cursor...]
        value = struct.pack("<I", opts.limit) + (opts.cursor or b"")

        resp = await self._client._send_and_check(
            OpCode.PROCESSING_LIST,
            namespace,
            b"",
            value,
        )

        entries, next_cursor = _parse_processing_list(resp.data)
        return ProcessingListResult(jobs=entries, cursor=next_cursor)

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
                 [batch_size:u32][records_processed:u64][created_at:i64]
                 [reason_len:u16][reason]  (reason is empty unless the job failed)
    """
    pos = 0

    def take(n: int) -> bytes:
        nonlocal pos
        if pos + n > len(data):
            raise IncompleteResponseError("processing.status answer is truncated")
        b = data[pos : pos + n]
        pos += n
        return b

    def read_str() -> str:
        (n,) = struct.unpack("<H", take(2))
        raw = take(n)
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as e:
            raise IncompleteResponseError(
                "processing.status answer has a field that isn't UTF-8"
            ) from e

    job_id = read_str()
    name = read_str()

    status_byte = take(1)[0]
    if status_byte >= len(_PROCESSING_STATUS_NAMES):
        raise IncompleteResponseError(f"processing.status answer has unknown status {status_byte}")

    batch_size, records_processed, created_at = struct.unpack("<IQq", take(20))
    reason = read_str()
    if pos != len(data):
        raise IncompleteResponseError(
            f"processing.status answer has {len(data) - pos} bytes after the reason"
        )

    return ProcessingStatusResult(
        job_id=job_id,
        name=name,
        status=_PROCESSING_STATUS_NAMES[status_byte],
        batch_size=batch_size,
        records_processed=records_processed,
        created_at=created_at,
        failure_reason=reason or None,
    )


def _parse_processing_list(data: bytes) -> tuple[list[ProcessingListEntry], bytes | None]:
    """Parse binary wire format for processing job list: the page's jobs and
    the next page's cursor, None on the last page.

    Wire format: [count:u32]([name_len:u16][name][job_id_len:u16][job_id]
                 [status_len:u16][status][created_at:i64])*
                 [has_more:u8][cursor_len:u16][cursor]
    A failed job's status reads "FAILED: <reason>".
    """
    pos = 0

    def take(n: int) -> bytes:
        nonlocal pos
        if pos + n > len(data):
            raise IncompleteResponseError("processing.list answer is truncated")
        b = data[pos : pos + n]
        pos += n
        return b

    def read_str() -> str:
        (n,) = struct.unpack("<H", take(2))
        raw = take(n)
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as e:
            raise IncompleteResponseError(
                "processing.list answer has a field that isn't UTF-8"
            ) from e

    (count,) = struct.unpack("<I", take(4))
    if count > _MAX_PROCESSING_LIST:
        raise IncompleteResponseError(f"processing.list answer lists {count} jobs")
    entries: list[ProcessingListEntry] = []
    for _ in range(count):
        name = read_str()
        job_id = read_str()
        status = read_str()
        (created_at,) = struct.unpack("<q", take(8))
        entries.append(
            ProcessingListEntry(name=name, job_id=job_id, status=status, created_at=created_at)
        )

    has_more = take(1)[0]
    if has_more > 1:
        raise IncompleteResponseError(f"processing.list answer has a has_more of {has_more}")
    (cursor_len,) = struct.unpack("<H", take(2))
    cursor = take(cursor_len)
    if pos != len(data):
        raise IncompleteResponseError(
            f"processing.list answer has {len(data) - pos} bytes after the cursor"
        )
    if has_more and not cursor:
        raise IncompleteResponseError("processing.list answer has more pages but no cursor")
    if not has_more and cursor:
        raise IncompleteResponseError("processing.list answer has a cursor on its last page")
    return entries, cursor if has_more else None


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
