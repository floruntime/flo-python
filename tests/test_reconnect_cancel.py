"""A cancelled reconnect ends at once, even when the cancel lands as a
connect is refused. Before Python 3.12, asyncio.wait_for drops a cancel that
arrives in the same loop turn its inner future finishes; a reconnect loop
built on it then retried on past its caller's deadline."""

import asyncio
import socket

import pytest

from flo import FloClient


def _refused_port() -> int:
    """A port nothing listens on: bound, then closed."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port: int = s.getsockname()[1]
    return port


# The cancel has to land in the turn the refused connect completes; which
# turn that is depends on the event loop, so try a run of them.
@pytest.mark.parametrize("turns", range(12))
async def test_a_cancelled_reconnect_raises_cancelled_at_once(turns: int) -> None:
    client = FloClient(f"127.0.0.1:{_refused_port()}", timeout_ms=1000)
    for _ in range(5):
        task = asyncio.ensure_future(client.reconnect(timeout=30))
        for _ in range(turns):
            await asyncio.sleep(0)
        task.cancel()
        # A lost cancel leaves it in its 1 s backoff, retrying.
        done, _ = await asyncio.wait({task}, timeout=0.5)
        if not done:
            task.cancel()
            await asyncio.wait({task}, timeout=5)
            pytest.fail(f"reconnect ran on after a cancel {turns} turns in")
        assert task.cancelled()


async def test_a_connect_to_a_refused_port_still_fails_as_before() -> None:
    from flo.exceptions import ConnectionFailedError

    with pytest.raises(ConnectionFailedError):
        await FloClient(f"127.0.0.1:{_refused_port()}", timeout_ms=1000).connect()
