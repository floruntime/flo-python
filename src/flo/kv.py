"""Flo KV Operations

Key-value store operations for Flo client.
"""

from typing import TYPE_CHECKING

from .types import (
    DeleteOptions,
    GetOptions,
    HistoryOptions,
    OpCode,
    OptionTag,
    PutOptions,
    ScanOptions,
    ScanResult,
    VersionEntry,
)
from .wire import OptionsBuilder, parse_history_response, parse_scan_response

if TYPE_CHECKING:
    from .client import FloClient


class KVOperations:
    """KV operations mixin for FloClient."""

    def __init__(self, client: "FloClient"):
        self._client = client

    async def get(
        self,
        key: str | bytes,
        options: GetOptions | None = None,
    ) -> bytes | None:
        """Get value for a key.

        Args:
            key: The key to retrieve.
            options: Optional operation options (block_ms for blocking get).

        Returns:
            Value bytes if found, None if not found.

        Example:
            # Simple get
            value = await client.kv.get("user:123")
            if value is not None:
                print(f"Found: {value.decode()}")

            # Blocking get - wait up to 5 seconds for value to appear
            value = await client.kv.get("key", GetOptions(block_ms=5000))

            # Blocking get - wait indefinitely (0 = infinite)
            value = await client.kv.get("key", GetOptions(block_ms=0))
        """
        opts = options or GetOptions()
        namespace = self._client.get_namespace(opts.namespace)

        key_bytes = key.encode("utf-8") if isinstance(key, str) else key

        # Build TLV options
        builder = OptionsBuilder()

        if opts.block_ms is not None:
            builder.add_u32(OptionTag.BLOCK_MS, opts.block_ms)

        response = await self._client._send_and_check(
            OpCode.KV_GET,
            namespace,
            key_bytes,
            b"",
            builder.build(),
            allow_not_found=True,
        )

        if response.is_not_found():
            return None

        return response.data if response.data else None

    async def put(
        self,
        key: str | bytes,
        value: bytes,
        options: PutOptions | None = None,
    ) -> None:
        """Set a key-value pair.

        Args:
            key: The key to set.
            value: The value bytes.
            options: Optional operation options (TTL, CAS, etc.).

        Example:
            # Simple put
            await client.kv.put("user:123", b"John Doe")

            # Put with TTL
            await client.kv.put("session:abc", b"data", PutOptions(ttl_seconds=3600))

            # Put with CAS (optimistic locking)
            await client.kv.put("counter", b"2", PutOptions(cas_version=1))
        """
        opts = options or PutOptions()
        namespace = self._client.get_namespace(opts.namespace)

        key_bytes = key.encode("utf-8") if isinstance(key, str) else key

        # Build TLV options
        builder = OptionsBuilder()

        if opts.ttl_seconds is not None:
            builder.add_u64(OptionTag.TTL_SECONDS, opts.ttl_seconds)

        if opts.cas_version is not None:
            builder.add_u64(OptionTag.CAS_VERSION, opts.cas_version)

        if opts.if_not_exists:
            builder.add_flag(OptionTag.IF_NOT_EXISTS)

        if opts.if_exists:
            builder.add_flag(OptionTag.IF_EXISTS)

        await self._client._send_and_check(
            OpCode.KV_PUT,
            namespace,
            key_bytes,
            value,
            builder.build(),
        )

    async def delete(
        self,
        key: str | bytes,
        options: DeleteOptions | None = None,
    ) -> None:
        """Delete a key.

        Args:
            key: The key to delete.
            options: Optional operation options.

        Note:
            This operation succeeds even if the key doesn't exist.

        Example:
            await client.kv.delete("user:123")
        """
        opts = options or DeleteOptions()
        namespace = self._client.get_namespace(opts.namespace)

        key_bytes = key.encode("utf-8") if isinstance(key, str) else key

        # Delete succeeds for both OK and NOT_FOUND
        await self._client._send_and_check(
            OpCode.KV_DELETE,
            namespace,
            key_bytes,
            b"",
            allow_not_found=True,
        )

    async def scan(
        self,
        prefix: str | bytes,
        options: ScanOptions | None = None,
    ) -> ScanResult:
        """Scan keys with a prefix.

        Args:
            prefix: Key prefix to scan.
            options: Optional scan options (cursor, limit, keys_only).

        Returns:
            ScanResult with entries, cursor, and has_more flag.

        Example:
            # Scan all users
            result = await client.kv.scan("user:")
            for entry in result.entries:
                print(f"{entry.key}: {entry.value}")

            # Paginated scan
            result = await client.kv.scan("user:", ScanOptions(limit=100))
            while result.has_more:
                result = await client.kv.scan("user:", ScanOptions(cursor=result.cursor))

            # Keys only (more efficient)
            result = await client.kv.scan("user:", ScanOptions(keys_only=True))
        """
        opts = options or ScanOptions()
        namespace = self._client.get_namespace(opts.namespace)

        prefix_bytes = prefix.encode("utf-8") if isinstance(prefix, str) else prefix

        # Build TLV options
        builder = OptionsBuilder()

        if opts.limit is not None:
            builder.add_u32(OptionTag.LIMIT, opts.limit)

        if opts.keys_only:
            builder.add_u8(OptionTag.KEYS_ONLY, 1)

        # Cursor goes in value field
        value = opts.cursor if opts.cursor is not None else b""

        response = await self._client._send_and_check(
            OpCode.KV_SCAN,
            namespace,
            prefix_bytes,
            value,
            builder.build(),
        )

        return parse_scan_response(response.data)

    async def history(
        self,
        key: str | bytes,
        options: HistoryOptions | None = None,
    ) -> list[VersionEntry]:
        """Get version history for a key.

        Args:
            key: The key to get history for.
            options: Optional history options (limit).

        Returns:
            List of VersionEntry with version, timestamp, and value.

        Example:
            history = await client.kv.history("user:123", HistoryOptions(limit=10))
            for entry in history:
                print(f"v{entry.version}: {entry.value} at {entry.timestamp}")
        """
        opts = options or HistoryOptions()
        namespace = self._client.get_namespace(opts.namespace)

        key_bytes = key.encode("utf-8") if isinstance(key, str) else key

        # Build TLV options
        builder = OptionsBuilder()

        if opts.limit is not None:
            builder.add_u32(OptionTag.LIMIT, opts.limit)

        response = await self._client._send_and_check(
            OpCode.KV_HISTORY,
            namespace,
            key_bytes,
            b"",
            builder.build(),
        )

        return parse_history_response(response.data)
