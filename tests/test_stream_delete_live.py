"""stream.delete against a live server: missing is fine, non-empty needs force."""

import secrets

import pytest

from flo import ConflictError, FloClient, StreamDeleteOptions


async def test_delete_refuses_records_without_force(client: FloClient) -> None:
    stream = f"py-del-{secrets.token_hex(4)}"
    await client.stream.delete(stream)  # missing: succeeds

    await client.stream.append(stream, b"r")
    with pytest.raises(ConflictError):
        await client.stream.delete(stream)
    assert (await client.stream.read(stream)).records

    await client.stream.delete(stream, StreamDeleteOptions(force=True))
    # The old record is gone: a fresh append is the only one read back.
    await client.stream.append(stream, b"after")
    records = (await client.stream.read(stream)).records
    assert [r.payload for r in records] == [b"after"]
    await client.stream.delete(stream, StreamDeleteOptions(force=True))
