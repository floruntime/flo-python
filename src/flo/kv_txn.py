"""Flo KV Per-Shard Transactions

Transactions are pinned to a single partition, chosen by the routing key
passed to ``begin``; every op inside the transaction is routed by it.

Caps (server-enforced):
    * 256 ops per transaction
    * 1 MiB total payload across buffered writes

The following operations are NOT supported inside a transaction and raise
:class:`TxnUnsupportedOpError` without a server round-trip: ``scan``,
``mget``, ``json_get``, ``json_set``, ``json_del``, ``history``.
"""

from __future__ import annotations

import contextlib
import dataclasses
from typing import TYPE_CHECKING, Any

from .exceptions import ValidationError
from .kv import kv_delete, kv_exists, kv_expire, kv_get, kv_incr, kv_set
from .kv_body import (
    decode_empty_answer,
    decode_txn_begin_answer,
    decode_txn_commit_answer,
    encode_txn_body,
)
from .types import (
    DeleteOptions,
    GetOptions,
    GetResult,
    KVCommitResult,
    KVExistsOptions,
    KVExpireOptions,
    KVIncrOptions,
    OpCode,
    PutOptions,
    PutResult,
)

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
        client: FloClient,
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

    def _check_alive(self) -> None:
        if self._done:
            raise TxnFinishedError("transaction already committed or rolled back")

    async def put(
        self,
        key: str | bytes,
        value: bytes,
        options: PutOptions | None = None,
    ) -> PutResult:
        """Buffer a put inside the transaction; it has no version until commit (0)."""
        self._check_alive()
        opts = dataclasses.replace(options or PutOptions(), routing_key=self._routing_key)
        return await kv_set(self._client, self._namespace, key, value, opts, self._txn_id)

    async def get(self, key: str | bytes) -> GetResult | None:
        """Read a key inside the transaction (sees buffered writes)."""
        self._check_alive()
        return await kv_get(
            self._client,
            self._namespace,
            key,
            GetOptions(routing_key=self._routing_key),
            self._txn_id,
        )

    async def delete(
        self,
        key: str | bytes,
        options: DeleteOptions | None = None,
    ) -> None:
        """Buffer a delete inside the transaction; whether the key existed is unknown."""
        self._check_alive()
        opts = dataclasses.replace(options or DeleteOptions(), routing_key=self._routing_key)
        await kv_delete(self._client, self._namespace, key, opts, self._txn_id)

    async def incr(self, key: str | bytes, delta: int = 1) -> None:
        """Buffer an atomic counter increment; its value is unknown until commit."""
        self._check_alive()
        opts = KVIncrOptions(delta=delta, routing_key=self._routing_key)
        await kv_incr(self._client, self._namespace, key, opts, self._txn_id)

    async def expire(
        self,
        key: str | bytes,
        ttl_ms: int,
        options: KVExpireOptions | None = None,
    ) -> None:
        """Buffer setting the TTL on a key, in milliseconds; ``ttl_ms=0`` clears it."""
        self._check_alive()
        opts = dataclasses.replace(options or KVExpireOptions(), routing_key=self._routing_key)
        await kv_expire(self._client, self._namespace, key, ttl_ms, opts, self._txn_id)

    async def exists(self, key: str | bytes) -> bool:
        """Check key existence inside the transaction (sees buffered writes)."""
        self._check_alive()
        opts = KVExistsOptions(routing_key=self._routing_key)
        return await kv_exists(self._client, self._namespace, key, opts, self._txn_id)

    # ── Disallowed inside a transaction ───────────────────────────────

    async def scan(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
        raise TxnUnsupportedOpError("scan is not supported inside a KV transaction")

    async def mget(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
        raise TxnUnsupportedOpError("mget is not supported inside a KV transaction")

    async def json_get(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
        raise TxnUnsupportedOpError("json_get is not supported inside a KV transaction")

    async def json_set(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
        raise TxnUnsupportedOpError("json_set is not supported inside a KV transaction")

    async def json_del(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
        raise TxnUnsupportedOpError("json_del is not supported inside a KV transaction")

    async def history(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
        raise TxnUnsupportedOpError("history is not supported inside a KV transaction")

    # ── Lifecycle ──────────────────────────────────────────────────────

    async def commit(self) -> KVCommitResult:
        """Atomically apply all buffered operations.

        After commit returns the transaction is closed and further operations
        raise :class:`TxnFinishedError`.
        """
        self._check_alive()
        self._done = True
        response = await self._client._send_and_check(
            OpCode.KV_COMMIT_TXN, self._namespace, b"", encode_txn_body(self._txn_id)
        )
        commit_index, op_count = decode_txn_commit_answer(response.data)
        return KVCommitResult(commit_index=commit_index, op_count=op_count)

    async def rollback(self) -> None:
        """Discard the buffered operations without committing.

        Idempotent: calling rollback after commit (or vice versa) is a no-op.
        """
        if self._done:
            return
        self._done = True
        response = await self._client._send_and_check(
            OpCode.KV_ROLLBACK_TXN, self._namespace, b"", encode_txn_body(self._txn_id)
        )
        decode_empty_answer("kv.txn_rollback", response.data)

    async def __aenter__(self) -> Transaction:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        if not self._done:
            # Best-effort cleanup; surface the original exception.
            with contextlib.suppress(Exception):
                await self.rollback()


async def begin(
    client: FloClient,
    namespace: str,
    routing_key: str,
) -> Transaction:
    """Open a new per-shard KV transaction: its routing key is the request's key."""
    if not routing_key:
        raise ValidationError("a transaction needs a routing key")
    response = await client._send_and_check(
        OpCode.KV_BEGIN_TXN, namespace, routing_key.encode("utf-8"), b""
    )
    txn_id, pinned_hash = decode_txn_begin_answer(response.data)
    return Transaction(
        client=client,
        namespace=namespace,
        routing_key=routing_key,
        txn_id=txn_id,
        pinned_hash=pinned_hash,
    )


__all__ = [
    "Transaction",
    "TxnUnsupportedOpError",
    "TxnFinishedError",
    "begin",
]
