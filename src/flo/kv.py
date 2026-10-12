"""Flo KV Operations

Key-value store operations for Flo client.
"""

import json
from typing import TYPE_CHECKING, Any

from .exceptions import IncompleteResponseError, ServerError, raise_for_status
from .kv_body import (
    decode_delete_answer,
    decode_empty_answer,
    decode_exists_answer,
    decode_get_answer,
    decode_history_answer,
    decode_incr_answer,
    decode_json_del_answer,
    decode_json_get_answer,
    decode_json_set_answer,
    decode_mget_answer,
    decode_scan_answer,
    decode_set_answer,
    encode_delete_body,
    encode_exists_body,
    encode_expire_body,
    encode_get_body,
    encode_history_body,
    encode_incr_body,
    encode_json_del_body,
    encode_json_get_body,
    encode_json_set_body,
    encode_mget_body,
    encode_scan_body,
    encode_set_body,
    split_mget_keys,
)
from .types import (
    DeleteOptions,
    GetJsonResult,
    GetOptions,
    GetResult,
    HistoryOptions,
    IncrResult,
    KVExistsOptions,
    KVExpireOptions,
    KVIncrOptions,
    KVJsonOptions,
    KVMGetOptions,
    MGetEntry,
    OpCode,
    PutOptions,
    PutResult,
    Reason,
    ScanOptions,
    ScanResult,
    VersionEntry,
)
from .wire import RawResponse

if TYPE_CHECKING:
    from .client import FloClient
    from .kv_txn import Transaction


def _key(key: str | bytes) -> bytes:
    return key.encode("utf-8") if isinstance(key, str) else key


def _absent(response: RawResponse) -> bool:
    """Whether a not-found answer says the thing is absent; any other not-found
    (no such transaction) is raised."""
    if not response.is_not_found():
        return False
    try:
        raise_for_status(response.status, response.data)
    except ServerError as e:
        if e.reason == Reason.NOT_FOUND:
            return True
        raise
    return False  # pragma: no cover - raise_for_status always raises here


async def kv_get(
    client: "FloClient", namespace: str, key: str | bytes, opts: GetOptions, txn: int | None = None
) -> GetResult | None:
    """kv.get, inside transaction ``txn`` when given."""
    response = await client._send_and_check(
        OpCode.KV_GET, namespace, _key(key), encode_get_body(opts, txn), allow_not_found=True
    )
    if _absent(response):
        return None
    return decode_get_answer(response.data)


async def kv_set(
    client: "FloClient",
    namespace: str,
    key: str | bytes,
    value: bytes,
    opts: PutOptions,
    txn: int | None = None,
) -> PutResult:
    """kv.set, inside transaction ``txn`` when given."""
    response = await client._send_and_check(
        OpCode.KV_PUT, namespace, _key(key), encode_set_body(value, opts, txn)
    )
    return decode_set_answer(response.data)


async def kv_delete(
    client: "FloClient",
    namespace: str,
    key: str | bytes,
    opts: DeleteOptions,
    txn: int | None = None,
) -> bool | None:
    """kv.delete, inside transaction ``txn`` when given: whether the key existed
    (None inside a transaction)."""
    response = await client._send_and_check(
        OpCode.KV_DELETE,
        namespace,
        _key(key),
        encode_delete_body(opts.cas_version, opts.routing_key, txn),
    )
    return decode_delete_answer(response.data)


async def kv_incr(
    client: "FloClient",
    namespace: str,
    key: str | bytes,
    opts: KVIncrOptions,
    txn: int | None = None,
) -> tuple[int | None, int | None]:
    """kv.incr, inside transaction ``txn`` when given: (version, value)."""
    response = await client._send_and_check(
        OpCode.KV_INCR, namespace, _key(key), encode_incr_body(opts.delta, opts.routing_key, txn)
    )
    return decode_incr_answer(response.data)


async def kv_expire(
    client: "FloClient",
    namespace: str,
    key: str | bytes,
    ttl_ms: int,
    opts: KVExpireOptions,
    txn: int | None = None,
) -> None:
    """kv.expire, inside transaction ``txn`` when given."""
    response = await client._send_and_check(
        OpCode.KV_EXPIRE,
        namespace,
        _key(key),
        encode_expire_body(ttl_ms, opts.cas_version, opts.routing_key, txn),
    )
    decode_empty_answer("kv.expire", response.data)


async def kv_exists(
    client: "FloClient",
    namespace: str,
    key: str | bytes,
    opts: KVExistsOptions,
    txn: int | None = None,
) -> bool:
    """kv.exists, inside transaction ``txn`` when given."""
    response = await client._send_and_check(
        OpCode.KV_EXISTS, namespace, _key(key), encode_exists_body(opts.routing_key, txn)
    )
    return decode_exists_answer(response.data)[0]


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

        Returns ``GetResult(value, version)``, or ``None`` if the key is not
        found, also when a wait (``wait_ms``) ends without it.

        Example:
            result = await client.kv.get("user:123")

            # Wait up to 2 seconds for the key to exist
            result = await client.kv.get("key", GetOptions(wait_ms=2000))
        """
        opts = options or GetOptions()
        return await kv_get(self._client, self._client.get_namespace(opts.namespace), key, opts)

    async def put(
        self,
        key: str | bytes,
        value: bytes,
        options: PutOptions | None = None,
    ) -> PutResult:
        """Set a key-value pair and return the new version.

        A ``cas_version`` that isn't the key's, or a ``condition`` that doesn't
        hold, raises a conflict with reason CAS_MISMATCH or CONDITION_FAILED.

        Example:
            res = await client.kv.put("user:123", b"John Doe")
            await client.kv.put("user:123", b"Jane", PutOptions(cas_version=res.version))
        """
        opts = options or PutOptions()
        return await kv_set(
            self._client, self._client.get_namespace(opts.namespace), key, value, opts
        )

    async def delete(
        self,
        key: str | bytes,
        options: DeleteOptions | None = None,
    ) -> bool:
        """Delete a key and return whether it existed.

        Deleting a missing key is not an error. A ``cas_version`` that isn't
        the key's raises a conflict with reason CAS_MISMATCH.

        Example:
            existed = await client.kv.delete("user:123")
            # Only the owner releases the lock:
            await client.kv.delete("lock:resource", DeleteOptions(cas_version=held))
        """
        opts = options or DeleteOptions()
        existed = await kv_delete(
            self._client, self._client.get_namespace(opts.namespace), key, opts
        )
        if existed is None:
            raise IncompleteResponseError("kv.delete answer has no existed outside a transaction")
        return existed

    async def mget(
        self,
        keys: list[str | bytes],
        options: KVMGetOptions | None = None,
    ) -> list[MGetEntry]:
        """Look up many keys, one entry per key in the order asked.

        Keys are split across requests so each fits one request frame, and
        the server answers as many keys as fit one answer; this asks again
        from where it stopped until every key is answered.

        Args:
            keys: Keys to fetch (max 256). Empty list returns ``[]``.
            options: Optional namespace override, and the routing key every
                key was written with (all are then looked up on its shard).

        Returns:
            List of :class:`MGetEntry`. ``found=False`` indicates a missing key.

        Example:
            for e in await client.kv.mget(["user:1", "user:2", "user:3"]):
                if e.found:
                    print(e.key, "@v", e.version, ":", e.value)
        """
        if not keys:
            return []
        opts = options or KVMGetOptions()
        namespace = self._client.get_namespace(opts.namespace)
        encoded = [_key(k) for k in keys]
        names = [k if isinstance(k, str) else k.decode("utf-8", errors="replace") for k in keys]
        encode_mget_body(encoded[:1], opts.routing_key)  # refuses a bad routing key up front
        out: list[MGetEntry] = []
        for run in split_mget_keys(encoded):
            done = 0
            while done < len(run):
                body = encode_mget_body(run[done:], opts.routing_key)
                response = await self._client._send_and_check(OpCode.KV_MGET, namespace, b"", body)
                entries, next_index = decode_mget_answer(
                    response.data, names[len(out) : len(out) + len(run) - done]
                )
                if next_index == 0:
                    raise IncompleteResponseError("kv.mget answered none of the keys asked")
                out.extend(entries)
                done += len(entries)
        return out

    async def scan(
        self,
        prefix: str | bytes = "",
        options: ScanOptions | None = None,
    ) -> ScanResult:
        """List the names of keys with a prefix ("" for all), a page at a time.

        Example:
            result = await client.kv.scan("user:")
            for name in result.names:
                print(name)

            # Next pages
            while result.cursor is not None:
                result = await client.kv.scan("user:", ScanOptions(cursor=result.cursor))
        """
        opts = options or ScanOptions()
        response = await self._client._send_and_check(
            OpCode.KV_SCAN,
            self._client.get_namespace(opts.namespace),
            _key(prefix),
            encode_scan_body(opts.limit, opts.cursor),
        )
        names, cursor = decode_scan_answer(response.data)
        return ScanResult(names=names, cursor=cursor)

    async def history(
        self,
        key: str | bytes,
        options: HistoryOptions | None = None,
    ) -> list[VersionEntry]:
        """A key's versions, newest first, each with when it was written; empty
        for a missing key.

        Example:
            for entry in await client.kv.history("user:123", HistoryOptions(limit=10)):
                print(f"v{entry.version} at {entry.at_ms}: {entry.value}")
        """
        opts = options or HistoryOptions()
        response = await self._client._send_and_check(
            OpCode.KV_HISTORY,
            self._client.get_namespace(opts.namespace),
            _key(key),
            encode_history_body(opts.limit, opts.routing_key),
            allow_not_found=True,
        )
        if _absent(response):
            return []
        return decode_history_answer(response.data)

    # ── Counters, TTL, exists, JSON paths ──────────────────────────────

    async def incr(
        self,
        key: str | bytes,
        options: KVIncrOptions | None = None,
    ) -> IncrResult:
        """Atomically add ``delta`` (default +1) to the i64 counter at ``key``.

        The first ``incr`` on a missing key creates it at the delta value. A
        key that holds something else raises with reason WRONG_TYPE.
        """
        opts = options or KVIncrOptions()
        version, value = await kv_incr(
            self._client, self._client.get_namespace(opts.namespace), key, opts
        )
        if version is None or value is None:
            raise IncompleteResponseError("kv.incr answer has no value outside a transaction")
        return IncrResult(value=value, version=version)

    async def expire(
        self,
        key: str | bytes,
        ttl_ms: int,
        options: KVExpireOptions | None = None,
    ) -> None:
        """Set the TTL on an existing key, in milliseconds; ``ttl_ms=0`` clears it.

        With ``cas_version`` it only succeeds when the key's version is that,
        for race-free lease renewal.
        """
        opts = options or KVExpireOptions()
        await kv_expire(self._client, self._client.get_namespace(opts.namespace), key, ttl_ms, opts)

    async def exists(
        self,
        key: str | bytes,
        options: KVExistsOptions | None = None,
    ) -> bool:
        """Return ``True`` if ``key`` is present without transferring its value."""
        opts = options or KVExistsOptions()
        return await kv_exists(self._client, self._client.get_namespace(opts.namespace), key, opts)

    async def json_get(
        self,
        key: str | bytes,
        path: str = "$",
        options: KVJsonOptions | None = None,
    ) -> GetJsonResult[Any] | None:
        """Extract the value at ``path`` from the JSON document at ``key``.

        Returns a :class:`GetJsonResult` whose ``value`` is the parsed JSON,
        with the document's current version, or ``None`` if the key or path
        is missing.
        """
        opts = options or KVJsonOptions()
        response = await self._client._send_and_check(
            OpCode.KV_JSON_GET,
            self._client.get_namespace(opts.namespace),
            _key(key),
            encode_json_get_body(path, opts.routing_key),
            allow_not_found=True,
        )
        if _absent(response):
            return None
        version, document = decode_json_get_answer(response.data)
        return GetJsonResult(value=json.loads(document.decode("utf-8")), version=version)

    async def json_set(
        self,
        key: str | bytes,
        path: str,
        json_value: bytes,
        options: KVJsonOptions | None = None,
    ) -> PutResult:
        """Set the JSON value at ``path`` ("$" when empty) inside the document at ``key``.

        Path ``"$"`` replaces the whole document (and creates the key if missing).
        Sub-paths require the key to already exist. Returns the new version.
        """
        opts = options or KVJsonOptions()
        response = await self._client._send_and_check(
            OpCode.KV_JSON_SET,
            self._client.get_namespace(opts.namespace),
            _key(key),
            encode_json_set_body(path, json_value, opts.routing_key),
        )
        return PutResult(version=decode_json_set_answer(response.data))

    async def json_del(
        self,
        key: str | bytes,
        path: str = "$",
        options: KVJsonOptions | None = None,
    ) -> PutResult | None:
        """Remove the value at ``path`` from the JSON document at ``key``.

        Returns the document's new version, or ``None`` for ``"$"``, which
        deletes the key.
        """
        opts = options or KVJsonOptions()
        response = await self._client._send_and_check(
            OpCode.KV_JSON_DEL,
            self._client.get_namespace(opts.namespace),
            _key(key),
            encode_json_del_body(path, opts.routing_key),
        )
        version = decode_json_del_answer(response.data)
        return None if version is None else PutResult(version=version)

    async def begin(self, routing_key: str) -> "Transaction":
        """Open a per-shard KV transaction pinned to ``routing_key``'s partition.

        Every op inside the transaction is routed by ``routing_key``; the
        server refuses one routed otherwise with reason WRONG_ROUTING_KEY.

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
