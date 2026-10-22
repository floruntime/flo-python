"""An action worker stays registered and reports its real load: a heartbeat
answered not_found registers it again, an await refused because it missed
its heartbeats sends one, and an await refused because it is draining stops
it taking work. Runs against an in-process fake server."""

import asyncio
import struct
from collections.abc import Callable

import pytest

from flo import FloClient, RetryPolicy
from flo import worker as worker_mod
from flo.types import HEADER_SIZE, OpCode, Ran, Reason, StatusCode
from flo.wire import REQUEST_HEADER_FORMAT
from flo.worker import ActionContext

from .test_result_resend import ACTION, _task, _until
from .test_retry import _frame

# How the fake answers an await: "task", "park" (never answer, as a long poll
# holds its connection), or a refusal reason.
AwaitReply = Callable[[int], "str | Reason"]


def _refused(request_id: int, status: StatusCode, reason: Reason) -> bytes:
    body = int(reason).to_bytes(2, "little") + bytes([int(Ran.NO)]) + b"refused"
    return _frame(request_id, status, body)


class FakeServer:
    """Records each request as (connection number, op, payload). The nth
    await (from 0) gets await_reply(n); the nth heartbeat is answered
    not_found when n is in heartbeats_not_found."""

    def __init__(
        self,
        await_reply: AwaitReply,
        heartbeats_not_found: frozenset[int] = frozenset(),
        complete_delay: float = 0.0,
    ) -> None:
        self.await_reply = await_reply
        self.heartbeats_not_found = heartbeats_not_found
        self.complete_delay = complete_delay
        self.requests: list[tuple[int, int, bytes]] = []
        self.writers: list[asyncio.StreamWriter] = []

    def of(self, op: int) -> list[tuple[int, int, bytes]]:
        return [r for r in self.requests if r[1] == op]

    def ops(self) -> list[int]:
        return [op for _, op, _ in self.requests]

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn = len(self.writers)
        self.writers.append(writer)
        try:
            while True:
                header = await reader.readexactly(HEADER_SIZE)
                _, payload_len, request_id, _, op, *_ = struct.unpack(REQUEST_HEADER_FORMAT, header)
                payload = await reader.readexactly(payload_len)
                if op == OpCode.ACTION_AWAIT:
                    reply = self.await_reply(len(self.of(op)))
                    self.requests.append((conn, op, payload))
                    if reply == "task":
                        writer.write(_frame(request_id, StatusCode.OK, _task()))
                    elif isinstance(reply, Reason):
                        writer.write(_refused(request_id, StatusCode.BAD_REQUEST, reply))
                    continue
                if op == OpCode.WORKER_HEARTBEAT:
                    n = len(self.of(op))
                    self.requests.append((conn, op, payload))
                    if n in self.heartbeats_not_found:
                        writer.write(_refused(request_id, StatusCode.NOT_FOUND, Reason.NOT_FOUND))
                    else:
                        writer.write(_frame(request_id, StatusCode.OK, b"\x00"))
                    continue
                self.requests.append((conn, op, payload))
                if op == OpCode.ACTION_COMPLETE:
                    await asyncio.sleep(self.complete_delay)
                writer.write(_frame(request_id, StatusCode.OK, b""))
        except (asyncio.IncompleteReadError, ConnectionError):
            pass


def _load(payload: bytes) -> int:
    """A heartbeat's current_load, the value after [ns][key]."""
    ns_len = struct.unpack_from("<H", payload)[0]
    key_len = struct.unpack_from("<H", payload, 2 + ns_len)[0]
    at = 2 + ns_len + 2 + key_len + 4  # past the value's length
    return int(struct.unpack_from("<I", payload, at)[0])


async def _serve(
    fake: FakeServer,
    handler: Callable[[ActionContext], object] | None = None,
    until: Callable[[], bool] = lambda: False,
    stop: bool = True,
) -> asyncio.Task[None]:
    server = await asyncio.start_server(fake.handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    client = FloClient(f"127.0.0.1:{port}", retry=RetryPolicy(max_attempts=1))
    worker = client.new_action_worker(block_ms=30000)

    async def done(ctx: ActionContext) -> bytes:
        if handler is not None:
            await handler(ctx)  # type: ignore[misc]
        return b"done"

    worker.register_action(ACTION, done)
    run = asyncio.create_task(worker.start())
    try:
        await _until(until)
    finally:
        if stop:
            worker.stop()
        await asyncio.wait_for(run, timeout=10)
        server.close()
        for w in fake.writers:
            w.close()
        await server.wait_closed()
    return run


@pytest.fixture
def fast_heartbeats(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_mod, "_HEARTBEAT_INTERVAL_S", 0.05)


@pytest.mark.usefixtures("fast_heartbeats")
async def test_a_heartbeat_answered_not_found_registers_the_worker_again() -> None:
    fake = FakeServer(lambda n: "park", heartbeats_not_found=frozenset({1}))
    await _serve(fake, until=lambda: len(fake.of(OpCode.WORKER_HEARTBEAT)) >= 4)
    registers = fake.of(OpCode.WORKER_REGISTER)
    assert len(registers) == 2
    # The same registration, every field: type, concurrency, processes,
    # metadata and machine id.
    assert registers[0][2] == registers[1][2]
    ops = fake.ops()
    second_heartbeat = [i for i, op in enumerate(ops) if op == OpCode.WORKER_HEARTBEAT][1]
    assert ops[second_heartbeat + 1] == OpCode.WORKER_REGISTER


async def test_an_await_refused_as_unhealthy_heartbeats_at_once_then_polls_again() -> None:
    # Heartbeats are 30 s apart here: any heartbeat comes from the refusal.
    fake = FakeServer(lambda n: Reason.WORKER_UNHEALTHY if n == 0 else "park")
    await _serve(fake, until=lambda: len(fake.of(OpCode.ACTION_AWAIT)) >= 2)
    ops = [op for op in fake.ops() if op != OpCode.ACTION_REGISTER]
    assert ops == [
        OpCode.WORKER_REGISTER,
        OpCode.ACTION_AWAIT,
        OpCode.WORKER_HEARTBEAT,
        OpCode.ACTION_AWAIT,
    ]


async def test_an_unhealthy_worker_the_server_removed_registers_again() -> None:
    fake = FakeServer(
        lambda n: Reason.WORKER_UNHEALTHY if n == 0 else "park",
        heartbeats_not_found=frozenset({0}),
    )
    await _serve(fake, until=lambda: len(fake.of(OpCode.ACTION_AWAIT)) >= 2)
    ops = [op for op in fake.ops() if op != OpCode.ACTION_REGISTER]
    assert ops == [
        OpCode.WORKER_REGISTER,
        OpCode.ACTION_AWAIT,
        OpCode.WORKER_HEARTBEAT,
        OpCode.WORKER_REGISTER,
        OpCode.ACTION_AWAIT,
    ]
    first, again = fake.of(OpCode.WORKER_REGISTER)
    assert first[2] == again[2]


async def test_awaits_refused_as_unhealthy_back_off() -> None:
    fake = FakeServer(lambda n: Reason.WORKER_UNHEALTHY)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 0.6
    await _serve(fake, until=lambda: loop.time() > deadline)
    awaits = len(fake.of(OpCode.ACTION_AWAIT))
    # Waits doubling from 100 ms: a handful in 0.6 s, where a hot loop
    # would send hundreds.
    assert 1 <= awaits <= 12
    assert len(fake.of(OpCode.WORKER_HEARTBEAT)) in (awaits - 1, awaits)


async def test_an_await_refused_as_draining_stops_taking_work_and_finishes_handlers() -> None:
    fake = FakeServer(lambda n: "task" if n == 0 else Reason.WORKER_DRAINING)
    finished = asyncio.Event()

    async def slow(ctx: ActionContext) -> None:
        await asyncio.sleep(0.3)
        finished.set()

    # start() returns by itself: no stop().
    run = await _serve(fake, slow, until=lambda: True, stop=False)
    assert run.done()
    assert finished.is_set()
    assert len(fake.of(OpCode.ACTION_COMPLETE)) == 1
    assert len(fake.of(OpCode.ACTION_AWAIT)) == 2


@pytest.mark.usefixtures("fast_heartbeats")
async def test_heartbeats_report_the_handlers_running_and_nothing_else() -> None:
    # The await after the task parks, holding a slot, and the complete is held
    # up after the handler returns: neither is a running handler. A heartbeat
    # due meanwhile is sent after the complete, on the same connection.
    fake = FakeServer(lambda n: "task" if n == 0 else "park", complete_delay=0.3)
    marks: dict[str, int] = {}

    async def held(ctx: ActionContext) -> None:
        marks["start"] = len(fake.of(OpCode.WORKER_HEARTBEAT))
        await asyncio.sleep(0.3)
        marks["end"] = len(fake.of(OpCode.WORKER_HEARTBEAT))

    def after_complete() -> list[int]:
        ops = fake.ops()
        if OpCode.ACTION_COMPLETE not in ops:
            return []
        at = ops.index(OpCode.ACTION_COMPLETE)
        return [_load(p) for _, op, p in fake.requests[at:] if op == OpCode.WORKER_HEARTBEAT]

    await _serve(fake, held, until=lambda: len(after_complete()) >= 3)
    loads = [_load(p) for _, _, p in fake.of(OpCode.WORKER_HEARTBEAT)]
    assert set(loads[: marks["start"]]) <= {0}
    during = loads[marks["start"] + 1 : marks["end"]]
    assert during and set(during) == {1}
    assert set(after_complete()) == {0}
