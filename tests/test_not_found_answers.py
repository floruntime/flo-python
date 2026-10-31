"""A NOT_FOUND answer to an op with no "absent" result raises, rather than
handing the refusal's [reason][ran][message] body to the answer parser."""

from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from flo import FloClient
from flo.exceptions import NotFoundError
from flo.types import Ran, Reason, StatusCode, StreamID
from flo.wire import RawResponse

REFUSAL = Reason.NOT_FOUND.to_bytes(2, "little") + bytes([Ran.NO]) + b"consumer group not found"

CALLS: dict[str, Callable[[FloClient], Awaitable[Any]]] = {
    "stream.append": lambda c: c.stream.append("s", b"x"),
    "stream.read": lambda c: c.stream.read("s"),
    "stream.info": lambda c: c.stream.info("s"),
    "stream.group_join": lambda c: c.stream.group_join("s", "g", "c"),
    "stream.group_leave": lambda c: c.stream.group_leave("s", "g", "c"),
    "stream.group_read": lambda c: c.stream.group_read("s", "g", "c"),
    "stream.group_pending": lambda c: c.stream.group_pending("s", "g"),
    "stream.group_claim": lambda c: c.stream.group_claim("s", "g", "c", 0, StreamID(), 10),
    "workflow.history": lambda c: c.workflow.history("no-such-run"),
}


@pytest.mark.parametrize("name", CALLS)
async def test_not_found_raises(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    client = FloClient("localhost:1")

    async def answer(*_: Any, **__: Any) -> RawResponse:
        return RawResponse(status=StatusCode.NOT_FOUND, data=REFUSAL, request_id=0)

    monkeypatch.setattr(client, "_send_request", answer)
    with pytest.raises(NotFoundError) as raised:
        await CALLS[name](client)
    assert raised.value.reason == Reason.NOT_FOUND
