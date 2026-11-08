"""A processing job's status, list entry and removal, against a live server."""

import secrets

import pytest

from flo import FloClient, ProcessingListEntry, ProcessingListOptions
from flo.exceptions import ConflictError
from flo.types import Reason


async def test_live_a_job_shows_its_runtime_and_is_removed_once_stopped(
    client: FloClient,
) -> None:
    # In the default namespace: a stream sink elsewhere must share the job's shard.
    t = secrets.token_hex(4)
    yaml = (
        f"kind: Processing\nname: py-job-{t}\n"
        f"sources:\n  - stream:\n      name: py-in-{t}\n"
        f"sinks:\n  - stream:\n      name: py-out-{t}\n"
    )
    job_id = await client.processing.submit(yaml)
    try:
        status = await client.processing.status(job_id)
        assert status is not None
        assert (status.name, status.status) == (f"py-job-{t}", "running")
        assert status.yaml == yaml.encode()

        jobs: list[ProcessingListEntry] = []
        cursor = None
        for _ in range(100):
            page = await client.processing.list(ProcessingListOptions(cursor=cursor))
            jobs.extend(page.jobs)
            if not page.has_more:
                break
            cursor = page.cursor
        [mine] = [j for j in jobs if j.job_id == job_id]
        assert (mine.name, mine.batch_size) == (f"py-job-{t}", status.batch_size)

        with pytest.raises(ConflictError) as running:
            await client.processing.remove(job_id)
        assert running.value.reason == Reason.JOB_RUNNING
    finally:
        await client.processing.stop(job_id)
    await client.processing.remove(job_id)
    assert await client.processing.status(job_id) is None
