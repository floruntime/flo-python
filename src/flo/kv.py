"""Flo KV Operations

Key-value store operations for Flo client.
"""

import struct
from typing import TYPE_CHECKING

from .types import (
    DeleteOptions,
    GetOptions,
    GetResult,
    HistoryOptions,
    KVExistsOptions,
    KVIncrOptions,
    KVJsonOptions,
    KVMGetOptions,
    KVTouchOptions,
    MGetEntry,
    OpCode,
    OptionTag,
    PutOptions,
    PutResult,
    ScanOptions,
    ScanResult,
    StatusCode,
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
    ) -> GetResult | None:
        """Get value and version for a key.

        Args:
            key: The key to retrieve.
            options: Optional operation options (block_ms for blocking get).

        Returns:
            ``GetResult(value, version)`` if found, ``None`` if not found.

        Example:
            result = await client.kv.get("user:123")
            if result is not None:
                print(f"Found {result.value!r} at version {result.version}")

            # Blocking get — wait up to 5 seconds
            result = await client.kv.get("key", GetOptions(block_ms=5000))
        """
        opts = options or GetOptions()
        namespace = self._client.get_namespace(opts.namespace)

        key_bytes = key.encode("utf-8") if isinstance(key, str) else key

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

        # Wire body: [version:u64 LE][value bytes]
        if not response.data or len(response.data) < 8:
            return GetResult(value=b"", version=0)
        version = struct.unpack("<Q", response.data[:8])[0]
        return GetResult(value=bytes(response.data[8:]), version=version)

    async def put(
        self,
        key: str | bytes,
        value: bytes,
        options: PutOptions | None = None,
    ) -> PutResult:
        """Set a key-value pair and return the new version.

        Args:
            key: The key to set.
            value: The value bytes.
            options: Optional operation options (TTL, CAS, etc.).

        Returns:
            ``PutResult(version)`` — the version assigned by the server,
            usable for CAS on the next write.

        Example:
            res = await client.kv.put("user:123", b"John Doe")
            # CAS the next write against this version:
            await client.kv.put("user:123", b"Jane",
                                PutOptions(cas_version=res.version))
        """
        opts = options or PutOptions()
        namespace = self._client.get_namespace(opts.namespace)

        key_bytes = key.encode("utf-8") if isinstance(key, str) else key

        builder = OptionsBuilder()
        if opts.ttl_seconds is not None:
            builder.add_u64(OptionTag.TTL_SECONDS, opts.ttl_seconds)
        if opts.cas_version is not None:
            builder.add_u64(OptionTag.CAS_VERSION, opts.cas_version)
        if opts.if_not_exists:
            builder.add_flag(OptionTag.IF_NOT_EXISTS)
        if opts.if_exists:
            builder.add_flag(OptionTag.IF_EXISTS)

        response = await self._client._send_and_check(
            OpCode.KV_PUT,
            namespace,
            key_bytes,
            value,
            builder.build(),
        )

        # Wire body: [version:u64 LE]
        if not response.data or len(response.data) < 8:
            return PutResult(version=0)
        return PutResult(version=struct.unpack("<Q", response.data[:8])[0])

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
            This operation succeeds even if the key doesn't exist (unless
            ``if_match`` is set, in which case a missing key is treated as a
            CAS mismatch).

        Example:
            await client.kv.delete("user:123")
            # CAS-protected release — only the owner deletes:
            await client.kv.delete("lock:resource",
                                   DeleteOptions(if_match=tag_version))
        """
        opts = options or DeleteOptions()
        namespace = self._client.get_namespace(opts.namespace)

        key_bytes = key.encode("utf-8") if isinstance(key, str) else key

        builder = OptionsBuilder()
        if opts.if_match is not None:
            builder.add_u64(OptionTag.CAS_VERSION, opts.if_match)

        # Delete succeeds for both OK and NOT_FOUND (when if_match is unset).
        await self._client._send_and_check(
            OpCode.KV_DELETE,
            namespace,
            key_bytes,
            b"",
            builder.build(),
            allow_not_found=opts.if_match is None,
        )

    async def mget(
        self,
        keys: list[str | bytes],
        options: KVMGetOptions | None = None,
    ) -> list[MGetEntry]:
        """Look up many keys in a single round trip.

        Keys may live on different shards — the server gathers results in
        parallel and returns one entry per requested key in the same order.

        Args:
            keys: Keys to fetch (max 256). Empty list returns ``[]``.
            options: Optional namespace override.

        Returns:
            List of :class:`MGetEntry`. ``found=False`` indicates a missing key.

        Example:
            for e in await client.kv.mget(["user:1", "user:2", "user:3"]):
                if e.found:
                    print(e.key, "@v", e.version, ":", e.value)
        """
        if not keys:
            return []
        if len(keys) > 256:
            raise ValueError("mget: too many keys (max 256)")
        opts = options or KVMGetOptions()
        namespace = self._client.get_namespace(opts.namespace)

        # Pack request: [count:u16 LE]([key_len:u16 LE][key])*
        encoded: list[bytes] = [
            (k.encode("utf-8") if isinstance(k, str) else k) for k in keys
        ]
        for k in encoded:
            if len(k) > 0xFFFF:
                raise ValueError("mget: key too long")
        parts: list[bytes] = [struct.pack("<H", len(encoded))]
        for k in encoded:
            parts.append(struct.pack("<H", len(k)))
            parts.append(k)
        value = b"".join(parts)

        response = await self._client._send_and_check(
            OpCode.KV_MGET,
            namespace,
            b"",
            value,
        )

        data = bytes(response.data or b"")
        if len(data) < 4:
            return []
        (count,) = struct.unpack("<I", data[:4])
        out: list[MGetEntry] = []
        off = 4
        for _ in range(count):
            if off + 1 + 2 > len(data):
                break
            status = data[off]
            off += 1
            (klen,) = struct.unpack("<H", data[off : off + 2])
            off += 2
            if off + klen + 8 + 4 > len(data):
                break
            key_bytes = data[off : off + klen]
            off += klen
            (version,) = struct.unpack("<Q", data[off : off + 8])
            off += 8
            (vlen,) = struct.unpack("<I", data[off : off + 4])
            off += 4
            if off + vlen > len(data):
                break
            val = data[off : off + vlen]
            off += vlen
            out.append(
                MGetEntry(
                    key=key_bytes.decode("utf-8", errors="replace"),
                    value=bytes(val),
                    version=version,
                    found=status == 0,
                )
            )
        return out

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

        # Build TLV options (keys_only only — limit is in value now)
        builder = OptionsBuilder()

        if opts.keys_only:
            builder.add_u8(OptionTag.KEYS_ONLY, 1)

        # Value: [limit:u32][cursor...]
        limit = opts.limit if opts.limit is not None else 0  # 0 = server default
        cursor = opts.cursor if opts.cursor is not None else b""
        value = struct.pack("<I", limit) + cursor

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

    # ── Extended ops: counters, TTL lifecycle, exists, JSON paths ──────

    async def incr(
        self,
        key: str | bytes,
        options: KVIncrOptions | None = None,
    ) -> int:
        """Atomically add ``delta`` (default +1) to the i64 counter at ``key``.

        The first ``incr`` on a missing key creates it at the delta value.
        Raises a server error if the key already holds a non-counter value.

        Returns:
            The new counter value as a Python int.
        """
        opts = options or KVIncrOptions()
        namespace = self._client.get_namespace(opts.namespace)
        key_bytes = key.encode("utf-8") if isinstance(key, str) else key

        value = b""
        if opts.delta is not None:
            value = struct.pack("<q", opts.delta)

        response = await self._client._send_and_check(
            OpCode.KV_INCR,
            namespace,
            key_bytes,
            value,
            b"",
        )
        # Wire body: [version:u64][counter:i64 LE]
        if not response.data or len(response.data) < 16:
            raise ValueError("incr: short response")
        return struct.unpack("<q", response.data[8:16])[0]

    async def touch(
        self,
        key: str | bytes,
        ttl_seconds: int,
        options: KVTouchOptions | None = None,
    ) -> None:
        """Update the TTL on an existing key. ``ttl_seconds=0`` clears the TTL.

        When ``options.if_match`` is set, the touch only succeeds if the
        current key version equals it — enabling race-free lease renewal.
        """
        opts = options or KVTouchOptions()
        namespace = self._client.get_namespace(opts.namespace)
        key_bytes = key.encode("utf-8") if isinstance(key, str) else key
        builder = OptionsBuilder()
        if opts.if_match is not None:
            builder.add_u64(OptionTag.CAS_VERSION, opts.if_match)
        await self._client._send_and_check(
            OpCode.KV_TOUCH,
            namespace,
            key_bytes,
            struct.pack("<Q", ttl_seconds),
            builder.build(),
        )

    async def persist(
        self,
        key: str | bytes,
        options: KVTouchOptions | None = None,
    ) -> None:
        """Clear the TTL on an existing key, making it permanent.

        When ``options.if_match`` is set, the persist only succeeds if the
        current key version equals it.
        """
        opts = options or KVTouchOptions()
        namespace = self._client.get_namespace(opts.namespace)
        key_bytes = key.encode("utf-8") if isinstance(key, str) else key
        builder = OptionsBuilder()
        if opts.if_match is not None:
            builder.add_u64(OptionTag.CAS_VERSION, opts.if_match)
        await self._client._send_and_check(
            OpCode.KV_PERSIST,
            namespace,
            key_bytes,
            b"",
            builder.build(),
        )

    async def exists(
        self,
        key: str | bytes,
        options: KVExistsOptions | None = None,
    ) -> bool:
        """Return ``True`` if ``key`` is present without transferring its value."""
        opts = options or KVExistsOptions()
        namespace = self._client.get_namespace(opts.namespace)
        key_bytes = key.encode("utf-8") if isinstance(key, str) else key
        response = await self._client._send_and_check(
            OpCode.KV_EXISTS,
            namespace,
            key_bytes,
            b"",
            b"",
        )
        # Wire body: [version:u64][1 byte 0/1]
        if not response.data or len(response.data) < 9:
            return False
        return response.data[8] == 1

    async def json_get(
        self,
        key: str | bytes,
        path: str = "$",
        options: KVJsonOptions | None = None,
    ) -> GetResult | None:
        """Extract the value at ``path`` from the JSON document at ``key``.

        Returns a :class:`GetResult` carrying the extracted JSON bytes and the
        document's current version, or ``None`` if the key or path is missing.
        """
        opts = options or KVJsonOptions()
        namespace = self._client.get_namespace(opts.namespace)
        key_bytes = key.encode("utf-8") if isinstance(key, str) else key
        path_bytes = path.encode("utf-8") if isinstance(path, str) else path

        response = await self._client._send_and_check(
            OpCode.KV_JSON_GET,
            namespace,
            key_bytes,
            path_bytes,
            b"",
            allow_not_found=True,
        )
        if response.is_not_found():
            return None
        # Wire body: [version:u64 LE][json bytes]
        if not response.data or len(response.data) < 8:
            return None
        version = int.from_bytes(response.data[:8], "little")
        return GetResult(value=bytes(response.data[8:]), version=version)

    async def json_set(
        self,
        key: str | bytes,
        path: str,
        json_value: bytes,
        options: KVJsonOptions | None = None,
    ) -> PutResult:
        """Set the JSON value at ``path`` inside the document at ``key``.

        Path ``"$"`` replaces the whole document (and creates the key if missing).
        Sub-paths require the key to already exist. Returns a :class:`PutResult`
        with the new document version.
        """
        opts = options or KVJsonOptions()
        namespace = self._client.get_namespace(opts.namespace)
        key_bytes = key.encode("utf-8") if isinstance(key, str) else key
        path_str = path or "$"
        path_bytes = path_str.encode("utf-8")
        if len(path_bytes) > 0xFFFF:
            raise ValueError("json_set: path too long")
        value = struct.pack("<H", len(path_bytes)) + path_bytes + json_value
        response = await self._client._send_and_check(
            OpCode.KV_JSON_SET,
            namespace,
            key_bytes,
            value,
            b"",
        )
        version = (
            int.from_bytes(response.data[:8], "little")
            if response.data and len(response.data) >= 8
            else 0
        )
        return PutResult(version=version)

    async def json_del(
        self,
        key: str | bytes,
        path: str = "$",
        options: KVJsonOptions | None = None,
    ) -> PutResult:
        """Remove the value at ``path`` from the JSON document at ``key``.

        For sub-paths the returned :class:`PutResult` carries the new document
        version. For ``"$"`` (whole document delete) the version is ``0`` since
        the key is gone.
        """
        opts = options or KVJsonOptions()
        namespace = self._client.get_namespace(opts.namespace)
        key_bytes = key.encode("utf-8") if isinstance(key, str) else key
        path_bytes = (path or "$").encode("utf-8")
        response = await self._client._send_and_check(
            OpCode.KV_JSON_DEL,
            namespace,
            key_bytes,
            path_bytes,
            b"",
        )
        version = (
            int.from_bytes(response.data[:8], "little")
            if response.data and len(response.data) >= 8
            else 0
        )
        return PutResult(version=version)

    async def begin(self, routing_key: str) -> "Transaction":
        """Open a per-shard KV transaction pinned to ``routing_key``'s partition.

        Every key written or read inside the transaction must hash to the same
        partition; otherwise the server returns a "kv_txn_cross_shard" error.

        Returns a :class:`Transaction` handle. Use as an async context manager
        for automatic rollback on exception::

            async with await client.kv.begin("user:123") as txn:
                await txn.put("user:123:name", b"Jane")
                await txn.incr("user:123:visits", 1)
                await txn.commit()
        """
        from .kv_txn import begin as _begin

        namespace = self._client.get_namespace(None)
        return await _begin(self._client, namespace, routing_key)