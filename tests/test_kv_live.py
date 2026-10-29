"""kv.get's expiry, and kv.history's deleted flag and paging, against a live server."""

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
    assert [(e.deleted, e.value) for e in history.versions] == [
        (False, b"three"),
        (True, b""),
        (False, b"one"),
    ]
    assert (history.more, history.stale) == (False, False)


async def test_history_pages_by_before_version(client: FloClient) -> None:
    key = f"py-page-{secrets.token_hex(4)}"
    written = [(await client.kv.put(key, f"v{i}".encode())).version for i in range(5)]
    pages = []
    opts = HistoryOptions(limit=2)
    while True:
        page = await client.kv.history(key, opts)
        pages.append(page)
        if not page.more:
            break
        opts = HistoryOptions(limit=2, before_version=page.versions[-1].version)
    assert [len(p.versions) for p in pages] == [2, 2, 1]
    seen = [e.version for p in pages for e in p.versions]
    assert seen == sorted(written, reverse=True)
    assert [e.value for p in pages for e in p.versions] == [b"v4", b"v3", b"v2", b"v1", b"v0"]

    past = await client.kv.history(key, HistoryOptions(limit=2, before_version=min(written)))
    assert (past.versions, past.more) == ([], False)
