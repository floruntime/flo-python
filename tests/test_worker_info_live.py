"""worker.info, and the server's limit on a registration's processes, against a
live server."""

import secrets

import pytest

from flo import FloClient, ProcessKind, WorkerRegisterOptions, WorkerStatus, WorkerType
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


async def test_a_registration_of_1025_processes_is_refused(client: FloClient) -> None:
    worker_id = f"py-info-many-{secrets.token_hex(4)}"
    with pytest.raises(BadRequestError) as e:
        await client.worker.register(worker_id, [f"p{i}" for i in range(1025)])
    assert (e.value.status_code, e.value.reason, e.value.ran) == (
        StatusCode.BAD_REQUEST,
        Reason.UNCLASSIFIED,
        Ran.NO,
    )
    assert str(e.value) == "more than 1024 processes"
    with pytest.raises(NotFoundError):
        await client.worker.info(worker_id)


async def test_info_for_a_missing_worker_is_not_found(client: FloClient) -> None:
    with pytest.raises(NotFoundError) as e:
        await client.worker.info(f"py-info-none-{secrets.token_hex(4)}")
    assert e.value.reason == Reason.NOT_FOUND
