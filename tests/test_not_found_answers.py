"""A NOT_FOUND answer to an op with no "absent" result raises, rather than
handing the refusal's [reason][ran][message] body to the answer parser."""

from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from flo import FloClient
from flo.exceptions import NotFoundError
from flo.types import Ran, Reason, StatusCode
from flo.wire import RawResponse

REFUSAL = Reason.NOT_FOUND.to_bytes(2, "little") + bytes([Ran.NO]) + b"consumer group not found"

CALLS: dict[str, Callable[[FloClient], Awaitable[Any]]] = {
    "stream.append": lambda c: c.stream.append("s", b"x"),
    "stream.read": lambda c: c.stream.read("s"),
    "ts.read": lambda c: c.ts.read("m"),
    "stream.info": lambda c: c.stream.info("s"),
    "stream.group_read": lambda c: c.stream.group_read("s", "g", "c"),
    "stream.group_pending": lambda c: c.stream.group_pending("s", "g"),
    "stream.group_delete": lambda c: c.stream.group_delete("s", "g"),
    "stream.group_info": lambda c: c.stream.group_info("s", "g"),
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


async def test_an_ok_empty_read_stays_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    # A stream or series that exists with nothing in the window answers ok
    # and empty; only a missing one is not found.
    client = FloClient("localhost:1")
    answers = {
        # [count:u32] and no records.
        "stream": (0).to_bytes(4, "little"),
        # [count:u16], then no cursor.
        "ts": (0).to_bytes(2, "little") + b"\x00",
    }
    kind = ""

    async def answer(*_: Any, **__: Any) -> RawResponse:
        return RawResponse(status=StatusCode.OK, data=answers[kind], request_id=0)

    monkeypatch.setattr(client, "_send_request", answer)
    kind = "stream"
    assert (await client.stream.read("s")).records == []
    kind = "ts"
    page = await client.ts.read("m")
    assert (page.points, page.cursor) == ([], None)
