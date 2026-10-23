"""worker.list, action.status's worker and re-registering after removal,
against a live server.

The removal test needs a server that removes silent workers fast: set
FLO_EXPIRY_ENDPOINT to one started with a config holding
``[workers] heartbeat_timeout_ms = 1000, remove_after_ms = 1000``.
"""

import asyncio
import contextlib
import json
import os
import secrets
import socket

import pytest

from flo import (
    ActionRunStatus,
    FloClient,
    RunStatus,
    WorkerListOptions,
    WorkerRegisterOptions,
    WorkerStatus,
)
from flo import worker as worker_mod
from flo.types import WorkerInfo, WorkerType
from flo.worker import ActionContext, ActionWorker, StreamContext

from .conftest import IN_CI

FLO_EXPIRY_ENDPOINT = os.environ.get("FLO_EXPIRY_ENDPOINT")


def _three_free_ports() -> int:
    """A port whose next two are free too: the server listens on all three."""
    while True:
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        try:
            with socket.socket() as a, socket.socket() as b:
                a.bind(("127.0.0.1", port + 1))
                b.bind(("127.0.0.1", port + 2))
            return port
        except OSError:
            continue


async def _own_server(
    binary: str, tmp_path: "os.PathLike[str]"
) -> tuple[asyncio.subprocess.Process, FloClient]:
    """Starts a server of its own and connects to it. A port taken between
    the check and the server's bind makes the server exit, so it starts again
    on another; a slow CI runner gets a minute in all."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 60
    attempt = 0
    while True:
        attempt += 1
        port = _three_free_ports()
        log = os.path.join(tmp_path, f"server-{attempt}.log")
        with open(log, "wb") as out:
            server = await asyncio.create_subprocess_exec(
                binary,
                "server",
                "start",
                "--port",
                str(port),
                "--data-dir",
                os.path.join(tmp_path, f"data-{attempt}"),
                stdout=out,
                stderr=asyncio.subprocess.STDOUT,
                env={**os.environ, "HOME": str(tmp_path)},
            )
        while server.returncode is None:
            try:
                client = FloClient(f"127.0.0.1:{port}", namespace="default")
                await client.connect()
                return server, client
            except Exception:
                if loop.time() >= deadline:
                    server.kill()
                    await server.wait()
                    with open(log, errors="replace") as f:
                        pytest.fail(f"the server never came up:\n{f.read()[-4000:]}")
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(server.wait(), 0.1)
        if loop.time() >= deadline:
            with open(log, errors="replace") as f:
                pytest.fail(f"the server exited ({server.returncode}):\n{f.read()[-4000:]}")


async def _all_workers(client: FloClient, limit: int = 1024) -> list[WorkerInfo]:
    """Every worker, page by page; each page's cursor must move on."""
    workers: list[WorkerInfo] = []
    cursor = b""
    while True:
        page = await client.worker.list(WorkerListOptions(limit=limit, cursor=cursor))
        assert len(page.workers) <= limit
        workers.extend(page.workers)
        if page.cursor is None:
            return workers
        assert page.cursor > cursor
        cursor = page.cursor


async def _find(client: FloClient, worker_id: str) -> WorkerInfo | None:
    return next((w for w in await _all_workers(client) if w.worker_id == worker_id), None)


async def _completed(client: FloClient, run_id: str, timeout: float = 15.0) -> ActionRunStatus:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        status = await client.action.status(run_id)
        if status.status not in (RunStatus.PENDING, RunStatus.RUNNING):
            return status
        assert asyncio.get_running_loop().time() < deadline, f"run {run_id} never finished"
        await asyncio.sleep(0.1)


async def test_worker_list_pages_every_worker_in_id_order(client: FloClient) -> None:
    prefix = f"py-list-{secrets.token_hex(4)}"
    ids = [f"{prefix}-{i}" for i in range(3)]
    for wid in ids:
        await client.worker.register(
            wid, ["py-list-none"], WorkerRegisterOptions(concurrency=7, machine_id="m-py")
        )
    workers = await _all_workers(client, limit=1)
    listed = [w.worker_id for w in workers]
    assert listed == sorted(listed, key=lambda s: s.encode())
    ours = [w for w in workers if w.worker_id.startswith(prefix)]
    assert [w.worker_id for w in ours] == ids
    for w in ours:
        assert (w.worker_type, w.status) == (WorkerType.ACTION, WorkerStatus.ACTIVE)
        assert (w.max_concurrency, w.machine_id, w.current_load) == (7, "m-py", 0)
        assert w.registered_at_ms > 0 and w.last_heartbeat_ms >= w.registered_at_ms


async def test_a_runs_status_names_the_worker_that_ran_it(
    client: FloClient, worker: ActionWorker
) -> None:
    run = await client.action.invoke(
        "validate-order", json.dumps({"orderId": "py-status", "amount": 1}).encode()
    )
    status = await _completed(client, run.run_id)
    assert status.status == RunStatus.COMPLETED
    assert status.worker_id == worker.config.worker_id


@pytest.fixture
async def expiry_client():
    if not FLO_EXPIRY_ENDPOINT:
        if IN_CI:
            pytest.fail("FLO_EXPIRY_ENDPOINT is not set")
        pytest.skip("FLO_EXPIRY_ENDPOINT is not set")
    c = FloClient(FLO_EXPIRY_ENDPOINT, namespace="default")
    await c.connect()
    yield c
    await c.close()


async def test_a_worker_the_server_removed_registers_again_and_keeps_working(
    expiry_client: FloClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = expiry_client
    # Longer than the server's 1 s removal plus its 1 s sweep: each
    # heartbeat finds the worker removed.
    monkeypatch.setattr(worker_mod, "_HEARTBEAT_INTERVAL_S", 3.5)
    action = f"py-expiry-{secrets.token_hex(4)}"
    wid = f"{action}-worker"
    w = client.new_action_worker(worker_id=wid, concurrency=3, block_ms=1000)

    async def echo(ctx: ActionContext) -> bytes:
        return ctx.payload

    w.register_action(action, echo)
    run = asyncio.create_task(w.start())
    try:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 15
        first = None
        while first is None:
            assert loop.time() < deadline, "the worker never registered"
            first = await _find(client, wid)
            await asyncio.sleep(0.05)
        # Removed, then registered again by the heartbeat that found it gone.
        removed = False
        again = None
        while again is None:
            assert loop.time() < deadline, f"the worker never came back (removed={removed})"
            listed = await _find(client, wid)
            if listed is None:
                removed = True
            elif removed:
                again = listed
            await asyncio.sleep(0.05)
        assert again.registered_at_ms > first.registered_at_ms
        assert (again.worker_type, again.max_concurrency, again.machine_id) == (
            WorkerType.ACTION,
            3,
            socket.gethostname(),
        )
        assert (first.max_concurrency, first.machine_id) == (3, socket.gethostname())

        invoked = await client.action.invoke(action, b"still here")
        status = await _completed(client, invoked.run_id)
        assert (status.status, status.output, status.worker_id) == (
            RunStatus.COMPLETED,
            b"still here",
            wid,
        )
    finally:
        w.stop()
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(run, timeout=10)


async def test_an_idle_worker_whose_result_connection_drops_keeps_heartbeating_and_working(
    expiry_client: FloClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = expiry_client
    # Well inside the server's 1 s timeout, so only a lost connection can
    # make the worker miss its heartbeats.
    monkeypatch.setattr(worker_mod, "_HEARTBEAT_INTERVAL_S", 0.3)
    action = f"py-drop-{secrets.token_hex(4)}"
    wid = f"{action}-worker"
    w = client.new_action_worker(worker_id=wid, concurrency=2, block_ms=1000)

    async def echo(ctx: ActionContext) -> bytes:
        return ctx.payload

    w.register_action(action, echo)
    run = asyncio.create_task(w.start())
    try:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 15
        while await _find(client, wid) is None:
            assert loop.time() < deadline, "the worker never registered"
            await asyncio.sleep(0.05)
        # Heartbeats ride the result connection; an idle worker sends nothing
        # else on it, so only the heartbeat can notice it's gone.
        assert w._result_client is not None
        w._result_client.interrupt()
        # Past the server's unhealthy and removal times.
        await asyncio.sleep(3)
        listed = await _find(client, wid)
        assert listed is not None, "the worker was removed after its connection dropped"
        assert listed.status == WorkerStatus.ACTIVE, f"worker after the drop: {listed}"

        invoked = await client.action.invoke(action, b"after the drop")
        status = await _completed(client, invoked.run_id)
        assert (status.status, status.output, status.worker_id) == (
            RunStatus.COMPLETED,
            b"after the drop",
            wid,
        )
    finally:
        w.stop()
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(run, timeout=10)


async def test_a_server_drain_lets_the_running_action_complete_once_and_ends_start(
    client: FloClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(worker_mod, "_HEARTBEAT_INTERVAL_S", 0.2)
    action = f"py-drain-{secrets.token_hex(4)}"
    wid = f"{action}-worker"
    w = client.new_action_worker(worker_id=wid, concurrency=2, block_ms=1000)
    running = asyncio.Event()
    runs = 0

    async def slow(ctx: ActionContext) -> bytes:
        nonlocal runs
        runs += 1
        running.set()
        await asyncio.sleep(1.0)
        return b"done"

    w.register_action(action, slow)
    run = asyncio.create_task(w.start())
    try:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 15
        while await _find(client, wid) is None:
            assert loop.time() < deadline, "the worker never registered"
            await asyncio.sleep(0.05)
        invoked = await client.action.invoke(action, b"")
        await asyncio.wait_for(running.wait(), 15)
        await client.worker.drain(wid)
        # start() returns by itself once the run has reported.
        await asyncio.wait_for(run, 10)
    finally:
        if not run.done():
            w.stop()
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(run, 10)
    status = await _completed(client, invoked.run_id)
    assert (status.status, status.output, status.attempt, runs) == (
        RunStatus.COMPLETED,
        b"done",
        1,
        1,
    )
    # It deregistered once drained.
    assert await _find(client, wid) is None


async def test_a_stream_worker_shows_in_the_worker_list_while_it_runs(client: FloClient) -> None:
    stream = f"py-sw-{secrets.token_hex(4)}"
    wid = f"{stream}-worker"

    async def ignore(ctx: StreamContext) -> None:
        pass

    w = client.new_stream_worker(
        stream=stream, group="g", handler=ignore, worker_id=wid, block_ms=1000
    )
    run = asyncio.create_task(w.start())
    try:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 15
        listed = None
        while listed is None:
            assert loop.time() < deadline, "the stream worker never registered"
            listed = await _find(client, wid)
            await asyncio.sleep(0.05)
        assert (listed.worker_type, listed.status) == (WorkerType.STREAM, WorkerStatus.ACTIVE)
    finally:
        w.drain()
        await asyncio.wait_for(run, 10)
    assert await _find(client, wid) is None


async def test_a_drain_with_a_handler_running_ends_soon_after_it_when_the_server_dies(
    tmp_path: "os.PathLike[str]",
) -> None:
    binary = os.environ.get("FLO_BINARY")
    if not binary:
        if IN_CI:
            pytest.fail("FLO_BINARY is not set")
        pytest.skip("FLO_BINARY is not set")
    server, client = await _own_server(binary, tmp_path)
    try:
        loop = asyncio.get_running_loop()
        action = f"py-dies-{secrets.token_hex(4)}"
        w = client.new_action_worker(worker_id=f"{action}-worker", concurrency=2, block_ms=1000)
        running, finished = asyncio.Event(), asyncio.Event()

        async def slow(ctx: ActionContext) -> bytes:
            running.set()
            await asyncio.sleep(1.0)
            finished.set()
            return b"done"

        w.register_action(action, slow)
        run = asyncio.create_task(w.start())
        await asyncio.sleep(0.3)
        await client.action.invoke(action, b"")
        await asyncio.wait_for(running.wait(), 15)
        server.kill()
        await server.wait()
        t0 = loop.time()
        w.drain()
        # The handler, then its result's bounded resends.
        await asyncio.wait_for(run, 15)
        assert loop.time() - t0 < 6
        assert finished.is_set()
        await client.close()
    finally:
        if server.returncode is None:
            server.kill()
            await server.wait()
