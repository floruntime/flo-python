"""A worker registers each action with the options it was given, and with
auto_touch renews a running task's lease on the result connection."""

import asyncio
import struct

import pytest

from flo import ActionRegisterOptions, FloClient, RetryPolicy
from flo.types import HEADER_SIZE, ActionType, OpCode, Ran, StatusCode
from flo.wire import REQUEST_HEADER_FORMAT, serialize_action_register_value
from flo.worker import ActionContext

from .test_result_resend import ACTION, TASK_ID, _task, _until
from .test_retry import _frame, _refusal


class FakeServer:
    """Hands out one task on the first await, then parks every later await
    unanswered, as a long poll holds its connection. Records each request
    as (connection number, op, payload)."""

    def __init__(self, refuse_touch: StatusCode | None = None, touch_delay: float = 0.0) -> None:
        self.refuse_touch = refuse_touch
        self.touch_delay = touch_delay
        self.requests: list[tuple[int, int, bytes]] = []
        self.delivered = False
        self.writers: list[asyncio.StreamWriter] = []

    def of(self, op: int) -> list[tuple[int, int, bytes]]:
        return [r for r in self.requests if r[1] == op]

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn = len(self.writers)
        self.writers.append(writer)
        try:
            while True:
                header = await reader.readexactly(HEADER_SIZE)
                _, payload_len, request_id, _, op, *_ = struct.unpack(REQUEST_HEADER_FORMAT, header)
                payload = await reader.readexactly(payload_len)
                self.requests.append((conn, op, payload))
                if op == OpCode.ACTION_AWAIT:
                    if not self.delivered:
                        self.delivered = True
                        writer.write(_frame(request_id, StatusCode.OK, _task()))
                    continue  # parked: a touch behind it would never be answered
                if op == OpCode.ACTION_TOUCH and self.refuse_touch is not None:
                    writer.write(_refusal(request_id, self.refuse_touch, Ran.NO))
                    continue
                if op == OpCode.ACTION_TOUCH and self.touch_delay:
                    asyncio.get_running_loop().call_later(
                        self.touch_delay, writer.write, _frame(request_id, StatusCode.OK, b"")
                    )
                    continue
                writer.write(_frame(request_id, StatusCode.OK, b""))
        except (asyncio.IncompleteReadError, ConnectionError):
            pass


async def _run(fake: FakeServer, *, auto_touch: bool, timeout_ms: int, handler_s: float) -> None:
    server = await asyncio.start_server(fake.handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    client = FloClient(f"127.0.0.1:{port}", retry=RetryPolicy(max_attempts=1))
    worker = client.new_action_worker(block_ms=30000, auto_touch=auto_touch)

    async def slow(ctx: ActionContext) -> bytes:
        await asyncio.sleep(handler_s)
        return b"done"

    options = ActionRegisterOptions(timeout_ms=timeout_ms, owner="payments")
    worker.register_action(ACTION, slow, options)
    run = asyncio.create_task(worker.start())
    try:
        await _until(lambda: bool(fake.of(OpCode.ACTION_COMPLETE)))
        await asyncio.sleep(0.3)  # long enough for a stray touch to show
    finally:
        worker.stop()
        await asyncio.wait_for(run, timeout=10)
        server.close()
        for w in fake.writers:
            w.close()
        await server.wait_closed()


async def test_a_worker_registers_its_action_with_the_options_it_was_given() -> None:
    fake = FakeServer()
    await _run(fake, auto_touch=False, timeout_ms=600000, handler_s=0)
    want = serialize_action_register_value(ActionType.USER, 600000, 3, "payments")
    # [type][timeout_ms][max_retries][owner_len][owner], as the server parses it.
    assert want == bytes.fromhex("00c0270900030000000800") + b"payments"
    [(_, _, payload)] = fake.of(OpCode.ACTION_REGISTER)
    assert struct.pack("<I", len(want)) + want in payload


async def test_auto_touch_renews_the_lease_on_the_result_connection_while_it_runs() -> None:
    fake = FakeServer()
    # A 300 ms timeout touches every 100 ms: a 450 ms handler gets about four.
    await _run(fake, auto_touch=True, timeout_ms=300, handler_s=0.45)
    touches = fake.of(OpCode.ACTION_TOUCH)
    assert 2 <= len(touches) <= 5
    poll_conn = fake.of(OpCode.ACTION_AWAIT)[0][0]
    for conn, _, payload in touches:
        assert conn != poll_conn
        # extend_ms: this worker's own timeout.
        assert TASK_ID.encode() + struct.pack("<I", 300) in payload
    # None after the result.
    complete_at = fake.requests.index(fake.of(OpCode.ACTION_COMPLETE)[0])
    assert all(fake.requests.index(t) < complete_at for t in touches)


async def test_without_auto_touch_nothing_is_touched() -> None:
    fake = FakeServer()
    await _run(fake, auto_touch=False, timeout_ms=300, handler_s=0.45)
    assert fake.of(OpCode.ACTION_TOUCH) == []


@pytest.mark.parametrize("status", [StatusCode.CONFLICT, StatusCode.NOT_FOUND])
async def test_a_touch_refused_because_the_run_moved_on_stops_the_touching(
    status: StatusCode,
) -> None:
    fake = FakeServer(refuse_touch=status)
    await _run(fake, auto_touch=True, timeout_ms=300, handler_s=0.45)
    assert len(fake.of(OpCode.ACTION_TOUCH)) == 1


async def test_a_touch_refused_as_overloaded_is_tried_again() -> None:
    fake = FakeServer(refuse_touch=StatusCode.OVERLOADED)
    await _run(fake, auto_touch=True, timeout_ms=300, handler_s=0.45)
    assert len(fake.of(OpCode.ACTION_TOUCH)) >= 2


async def test_a_touch_in_flight_when_the_handler_ends_keeps_the_result_connection() -> None:
    # The first touch goes at 100 ms and is answered at 250 ms; the handler
    # ends at 150 ms, with it in flight.
    fake = FakeServer(touch_delay=0.15)
    await _run(fake, auto_touch=True, timeout_ms=300, handler_s=0.15)
    assert len(fake.of(OpCode.ACTION_TOUCH)) == 1
    assert len(fake.of(OpCode.ACTION_COMPLETE)) == 1
    assert len(fake.writers) == 2  # the poll and result connections, never redialed


@pytest.mark.parametrize("field", ["timeout_ms", "max_retries"])
@pytest.mark.parametrize("value", [-1, 2**32])
def test_register_options_outside_u32_are_refused(field: str, value: int) -> None:
    with pytest.raises(ValueError, match=field):
        ActionRegisterOptions(**{field: value})


async def test_an_action_that_never_times_out_is_not_touched() -> None:
    fake = FakeServer()
    await _run(fake, auto_touch=True, timeout_ms=0, handler_s=0.3)
    assert fake.of(OpCode.ACTION_TOUCH) == []
