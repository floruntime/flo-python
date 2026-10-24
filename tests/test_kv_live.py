"""kv.get's expiry and kv.history's deleted flag, against a live server."""

import secrets
import time

from flo import FloClient, HistoryOptions, PutOptions


async def test_get_shows_when_a_ttl_runs_out(client: FloClient) -> None:
    key = f"py-ttl-{secrets.token_hex(4)}"
    ttl_ms = 60_000
    before = int(time.time() * 1000)
    await client.kv.put(key, b"v", PutOptions(ttl_ms=ttl_ms))
    after = int(time.time() * 1000)
    got = await client.kv.get(key)
    assert got is not None
    assert got.expires_at_ms is not None
    assert before + ttl_ms <= got.expires_at_ms <= after + ttl_ms

    await client.kv.put(key, b"v")
    got = await client.kv.get(key)
    assert got is not None and got.expires_at_ms is None


async def test_history_marks_the_deleted_version(client: FloClient) -> None:
    key = f"py-hist-{secrets.token_hex(4)}"
    await client.kv.put(key, b"one")
    await client.kv.delete(key)
    await client.kv.put(key, b"three")
    history = await client.kv.history(key, HistoryOptions(limit=10))
    assert [(e.deleted, e.value) for e in history] == [
        (False, b"three"),
        (True, b""),
        (False, b"one"),
    ]
