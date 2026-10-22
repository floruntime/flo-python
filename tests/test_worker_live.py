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
from flo.worker import ActionContext, ActionWorker

from .conftest import IN_CI

FLO_EXPIRY_ENDPOINT = os.environ.get("FLO_EXPIRY_ENDPOINT")


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
