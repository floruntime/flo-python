"""What doesn't exist is refused, not answered as empty: the info of an unknown
stream, and a read in a namespace that was never created."""

import secrets

import pytest

from flo import FloClient, GetOptions, PutOptions, StreamInfoOptions
from flo.exceptions import NoNamespaceError, NotFoundError


async def test_stream_info_of_an_unknown_stream_is_not_found(client: FloClient) -> None:
    ns = f"py-missing-{secrets.token_hex(4)}"
    # The namespace exists, so the refusal is about the stream.
    await client.kv.put("k", b"v", PutOptions(namespace=ns))
    with pytest.raises(NotFoundError):
        await client.stream.info(f"no-such-{secrets.token_hex(4)}", StreamInfoOptions(namespace=ns))


async def test_a_read_in_a_namespace_never_created_is_no_namespace(client: FloClient) -> None:
    with pytest.raises(NoNamespaceError):
        await client.kv.get("k", GetOptions(namespace=f"py-never-{secrets.token_hex(4)}"))
