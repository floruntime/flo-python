"""worker.info, and the server's limit on a registration's processes, against a
live server."""

import secrets

import pytest

from flo import (
    FloClient,
    ProcessKind,
    WorkerListOptions,
    WorkerRegisterOptions,
    WorkerStatus,
    WorkerType,
)
from flo.exceptions import BadRequestError, NotFoundError
from flo.types import Ran, Reason, StatusCode


async def test_info_shows_the_row_metadata_and_processes_registered(client: FloClient) -> None:
    worker_id = f"py-info-{secrets.token_hex(4)}"
    await client.worker.register(
        worker_id,
        ["py-info-a", "py-info-b"],
        WorkerRegisterOptions(concurrency=3, machine_id="m-py", metadata='{"zone": "a"}'),
    )
    try:
        info = await client.worker.info(worker_id)
        w = info.worker
        assert (w.worker_id, w.worker_type, w.status) == (
            worker_id,
            WorkerType.ACTION,
            WorkerStatus.ACTIVE,
        )
        assert (w.max_concurrency, w.machine_id) == (3, "m-py")
        assert info.metadata == b'{"zone": "a"}'
        assert [(p.name, p.kind, p.run_count, p.fail_count) for p in info.processes] == [
            ("py-info-a", ProcessKind.ACTION, 0, 0),
            ("py-info-b", ProcessKind.ACTION, 0, 0),
        ]
        listed = [r for r in (await client.worker.list()).workers if r.worker_id == worker_id]
        assert listed == [w]
    finally:
        await client.worker.deregister(worker_id)

    stream_id = f"py-info-s-{secrets.token_hex(4)}"
    await client.worker.register(
        stream_id, ["py-info-stream"], WorkerRegisterOptions(worker_type=WorkerType.STREAM)
    )
    try:
        info = await client.worker.info(stream_id)
        assert (info.worker.machine_id, info.metadata) == (None, None)
        assert [(p.name, p.kind) for p in info.processes] == [
            ("py-info-stream", ProcessKind.STREAM_CONSUMER)
        ]
    finally:
        await client.worker.deregister(stream_id)


@pytest.mark.parametrize(
    ("processes", "options", "reason", "message"),
    [
        (
            [f"p{i}" for i in range(1025)],
            WorkerRegisterOptions(),
            Reason.OUT_OF_RANGE,
            "more than 1024 processes",
        ),
        (
            ["p" * 4097],
            WorkerRegisterOptions(),
            Reason.FIELD_TOO_LONG,
            "process name longer than 4096 bytes",
        ),
        (
            ["p"],
            WorkerRegisterOptions(machine_id="m" * 256),
            Reason.FIELD_TOO_LONG,
            "machine id longer than 255 bytes",
        ),
    ],
    ids=["1025-processes", "4097-byte-name", "256-byte-machine-id"],
)
async def test_a_registration_past_the_servers_limits_is_refused(
    client: FloClient,
    processes: list[str],
    options: WorkerRegisterOptions,
    reason: Reason,
    message: str,
) -> None:
    worker_id = f"py-info-limit-{secrets.token_hex(4)}"
    with pytest.raises(BadRequestError) as e:
        await client.worker.register(worker_id, processes, options)
    assert (e.value.status_code, e.value.reason, e.value.ran, str(e.value)) == (
        StatusCode.BAD_REQUEST,
        reason,
        Ran.NO,
        message,
    )
    with pytest.raises(NotFoundError):
        await client.worker.info(worker_id)


async def test_worker_list_filters_by_action(client: FloClient) -> None:
    tag = secrets.token_hex(4)
    a, b = f"py-filter-a-{tag}", f"py-filter-b-{tag}"
    await client.worker.register(a, [f"py-act-a-{tag}"])
    await client.worker.register(b, [f"py-act-b-{tag}", f"py-act-c-{tag}"])
    try:
        for action, want in [
            (f"py-act-a-{tag}", [a]),
            (f"py-act-c-{tag}", [b]),
            (f"py-act-none-{tag}", []),
        ]:
            page = await client.worker.list(WorkerListOptions(limit=1024, action=action))
            assert [w.worker_id for w in page.workers] == want
        every = await client.worker.list(WorkerListOptions(limit=1024))
        assert {a, b} <= {w.worker_id for w in every.workers}
    finally:
        await client.worker.deregister(a)
        await client.worker.deregister(b)


async def test_info_for_a_missing_worker_is_not_found(client: FloClient) -> None:
    with pytest.raises(NotFoundError) as e:
        await client.worker.info(f"py-info-none-{secrets.token_hex(4)}")
    assert e.value.reason == Reason.NOT_FOUND
