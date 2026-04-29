"""Flo KV Per-Shard Transactions

Transactions are pinned to a single partition (chosen by the routing key
passed to ``begin``). Every key written or read inside the transaction must
hash to the same partition; otherwise the server returns a
"kv_txn_cross_shard" error.

Caps (server-enforced):
    * 256 ops per transaction
    * 1 MiB total payload across buffered writes

The following operations are NOT supported inside a transaction and raise
:class:`TxnUnsupportedOpError` without a server round-trip: ``scan``,
``mget``, ``json_get``, ``json_set``, ``json_del``, ``history``.
"""

from __future__ import annotations

import struct
from typing import TYPE_CHECKING

from .types import (
    DeleteOptions,
    GetResult,
    KVBeginResult,
    KVCommitResult,
    OpCode,
    OptionTag,
    PutOptions,
    PutResult,
)
from .wire import OptionsBuilder

if TYPE_CHECKING:
    from .client import FloClient


class TxnUnsupportedOpError(Exception):
    """Raised when an operation is not allowed inside a KV transaction."""


class TxnFinishedError(Exception):
    """Raised when an operation is attempted on a closed transaction."""


class Transaction:
    """Per-shard KV transaction handle.

    Operations are buffered on the server's pinned shard until :meth:`commit`
    or :meth:`rollback` is called. Use as an async context manager for
    automatic rollback on exception::

        async with await client.kv.begin("user:123") as txn:
            await txn.put("user:123:name", b"Jane")
            await txn.incr("user:123:visits", 1)
            result = await txn.commit()
            print(result.commit_index)
    """

    def __init__(
        self,
        client: "FloClient",
        namespace: str,
        routing_key: str,
        txn_id: int,
        pinned_hash: int,
    ):
        self._client = client
        self._namespace = namespace
        self._routing_key = routing_key
        self._txn_id = txn_id
        self._pinned_hash = pinned_hash
        self._done = False

    @property
    def id(self) -> int:
        """The server-assigned transaction id."""
        return self._txn_id

    @property
    def pinned_hash(self) -> int:
        """The partition hash this transaction is bound to."""
        return self._pinned_hash

    def _txn_options(self) -> OptionsBuilder:
        b = OptionsBuilder()
        if self._routing_key:
            b.add_bytes(OptionTag.ROUTING_KEY, self._routing_key.encode("utf-8"))
        b.add_u64(OptionTag.TXN_ID, self._txn_id)
        return b

    def _check_alive(self) -> None:
        if self._done:
            raise TxnFinishedError("transaction already committed or rolled back")

    async def put(
        self,
        key: str | bytes,
        value: bytes,
        options: PutOptions | None = None,
    ) -> PutResult:
        """Buffer a put inside the transaction."""
        self._check_alive()
        opts = options or PutOptions()
        key_bytes = key.encode("utf-8") if isinstance(key, str) else key
        builder = self._txn_options()
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
            self._namespace,
            key_bytes,
            value,
            builder.build(),
        )
        if not response.data or len(response.data) < 8:
            return PutResult(version=0)
        return PutResult(version=struct.unpack("<Q", response.data[:8])[0])

    async def get(self, key: str | bytes) -> GetResult | None:
        """Read a key inside the transaction (sees buffered writes)."""
        self._check_alive()
        key_bytes = key.encode("utf-8") if isinstance(key, str) else key
        builder = self._txn_options()
        response = await self._client._send_and_check(
            OpCode.KV_GET,
            self._namespace,
            key_bytes,
            b"",
            builder.build(),
            allow_not_found=True,
        )
        if response.is_not_found():
            return None
        if not response.data or len(response.data) < 8:
            return GetResult(value=b"", version=0)
        version = struct.unpack("<Q", response.data[:8])[0]
        return GetResult(value=bytes(response.data[8:]), version=version)

    async def delete(
        self,
        key: str | bytes,
        options: DeleteOptions | None = None,
    ) -> None:
        """Buffer a delete inside the transaction."""
        self._check_alive()
        key_bytes = key.encode("utf-8") if isinstance(key, str) else key
        builder = self._txn_options()
        await self._client._send_and_check(
            OpCode.KV_DELETE,
            self._namespace,
            key_bytes,
            b"",
            builder.build(),
            allow_not_found=True,
        )

    async def incr(self, key: str | bytes, delta: int = 1) -> int:
        """Buffer an atomic counter increment inside the transaction."""
        self._check_alive()
        key_bytes = key.encode("utf-8") if isinstance(key, str) else key
        builder = self._txn_options()
        value = struct.pack("<q", delta)
        response = await self._client._send_and_check(
            OpCode.KV_INCR,
            self._namespace,
            key_bytes,
            value,
            builder.build(),
        )
        if not response.data or len(response.data) < 8:
            return 0
        return struct.unpack("<q", response.data[:8])[0]

    async def touch(self, key: str | bytes, ttl_seconds: int) -> None:
        """Update the TTL on an existing key inside the transaction."""
        self._check_alive()
        key_bytes = key.encode("utf-8") if isinstance(key, str) else key
        builder = self._txn_options()
        await self._client._send_and_check(
            OpCode.KV_TOUCH,
            self._namespace,
            key_bytes,
            struct.pack("<Q", ttl_seconds),
            builder.build(),
        )

    async def persist(self, key: str | bytes) -> None:
        """Remove the TTL on a key inside the transaction."""
        self._check_alive()
        key_bytes = key.encode("utf-8") if isinstance(key, str) else key
        builder = self._txn_options()
        await self._client._send_and_check(
            OpCode.KV_PERSIST,
            self._namespace,
            key_bytes,
            b"",
            builder.build(),
        )

    async def exists(self, key: str | bytes) -> bool:
        """Check key existence inside the transaction."""
        self._check_alive()
        key_bytes = key.encode("utf-8") if isinstance(key, str) else key
        builder = self._txn_options()
        response = await self._client._send_and_check(
            OpCode.KV_EXISTS,
            self._namespace,
            key_bytes,
            b"",
            builder.build(),
        )
        # Wire body: [version:u64 LE][1 byte 0/1]
        if not response.data or len(response.data) < 9:
            return False
        return response.data[8] == 1

    # ── Disallowed inside a transaction ───────────────────────────────

    async def scan(self, *args, **kwargs) -> "None":  # pragma: no cover
        raise TxnUnsupportedOpError("scan is not supported inside a KV transaction")

    async def mget(self, *args, **kwargs) -> "None":  # pragma: no cover
        raise TxnUnsupportedOpError("mget is not supported inside a KV transaction")

    async def json_get(self, *args, **kwargs) -> "None":  # pragma: no cover
        raise TxnUnsupportedOpError("json_get is not supported inside a KV transaction")

    async def json_set(self, *args, **kwargs) -> "None":  # pragma: no cover
        raise TxnUnsupportedOpError("json_set is not supported inside a KV transaction")

    async def json_del(self, *args, **kwargs) -> "None":  # pragma: no cover
        raise TxnUnsupportedOpError("json_del is not supported inside a KV transaction")

    async def history(self, *args, **kwargs) -> "None":  # pragma: no cover
        raise TxnUnsupportedOpError("history is not supported inside a KV transaction")

    # ── Lifecycle ──────────────────────────────────────────────────────

    async def commit(self) -> KVCommitResult:
        """Atomically apply all buffered operations.

        After commit returns the transaction is closed and further operations
        raise :class:`TxnFinishedError`.
        """
        self._check_alive()
        self._done = True
        builder = OptionsBuilder()
        if self._routing_key:
            builder.add_bytes(OptionTag.ROUTING_KEY, self._routing_key.encode("utf-8"))
        builder.add_u64(OptionTag.TXN_ID, self._txn_id)
        response = await self._client._send_and_check(
            OpCode.KV_COMMIT_TXN,
            self._namespace,
            self._routing_key.encode("utf-8"),
            b"",
            builder.build(),
        )
        # Wire body: [variant:u8=1][commit_index:u64 LE][op_count:u16 LE]
        if not response.data or len(response.data) < 11:
            raise RuntimeError(
                f"flo: short KV commit reply ({len(response.data) if response.data else 0} bytes)"
            )
        return KVCommitResult(
            commit_index=struct.unpack("<Q", response.data[1:9])[0],
            op_count=struct.unpack("<H", response.data[9:11])[0],
        )

    async def rollback(self) -> None:
        """Discard the buffered operations without committing.

        Idempotent: calling rollback after commit (or vice versa) is a no-op.
        """
        if self._done:
            return
        self._done = True
        builder = OptionsBuilder()
        if self._routing_key:
            builder.add_bytes(OptionTag.ROUTING_KEY, self._routing_key.encode("utf-8"))
        builder.add_u64(OptionTag.TXN_ID, self._txn_id)
        await self._client._send_and_check(
            OpCode.KV_ROLLBACK_TXN,
            self._namespace,
            self._routing_key.encode("utf-8"),
            b"",
            builder.build(),
        )

    async def __aenter__(self) -> "Transaction":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if not self._done:
            try:
                await self.rollback()
            except Exception:
                # Best-effort cleanup; surface the original exception.
                pass


async def begin(
    client: "FloClient",
    namespace: str,
    routing_key: str,
) -> Transaction:
    """Open a new per-shard KV transaction."""
    builder = OptionsBuilder()
    if routing_key:
        builder.add_bytes(OptionTag.ROUTING_KEY, routing_key.encode("utf-8"))
    response = await client._send_and_check(
        OpCode.KV_BEGIN_TXN,
        namespace,
        routing_key.encode("utf-8"),
        b"",
        builder.build(),
    )
    # Wire body: [variant:u8=0][txn_id:u64 LE][pinned_hash:u64 LE]
    if not response.data or len(response.data) < 17:
        raise RuntimeError(
            f"flo: short KV begin reply ({len(response.data) if response.data else 0} bytes)"
        )
    return Transaction(
        client=client,
        namespace=namespace,
        routing_key=routing_key,
        txn_id=struct.unpack("<Q", response.data[1:9])[0],
        pinned_hash=struct.unpack("<Q", response.data[9:17])[0],
    )


__all__ = [
    "Transaction",
    "TxnUnsupportedOpError",
    "TxnFinishedError",
    "begin",
]
