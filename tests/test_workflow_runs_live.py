"""Workflow runs on a multi-shard server: a run list pages through every run
once, newest first, and a run started with the caller's id is found by it."""

import secrets
import struct
import uuid

import pytest

from flo import (
    FloClient,
    WorkflowCancelOptions,
    WorkflowCreateOptions,
    WorkflowHistoryOptions,
    WorkflowListRunsOptions,
    WorkflowRunEntry,
    WorkflowSignalOptions,
    WorkflowStartOptions,
    WorkflowStatusOptions,
)
from flo.exceptions import ConflictError

LIMIT = 3

# No worker serves the action, so every run waits on it and stays live.
WORKFLOW_YAML = """\
kind: Workflow
name: {name}
version: "1.0.0"
start:
  run: "@actions/py-runs-unserved"
  transitions:
    success: flo.Completed
    failure: flo.Failed
"""


def fresh_namespace() -> str:
    return f"py-runs-{secrets.token_hex(4)}"


async def create(client: FloClient, ns: str, name: str) -> None:
    await client.workflow.create(
        name, WORKFLOW_YAML.format(name=name), WorkflowCreateOptions(namespace=ns)
    )


async def list_all(client: FloClient, ns: str, workflow: str | None) -> list[WorkflowRunEntry]:
    """Every run across the pages, paging only by has_more and the cursor."""
    runs: list[WorkflowRunEntry] = []
    cursor: bytes | None = None
    for _ in range(1000):
        page = await client.workflow.list_runs(
            WorkflowListRunsOptions(
                namespace=ns, workflow_name=workflow, limit=LIMIT, cursor=cursor
            )
        )
        assert len(page.runs) <= LIMIT, f"a page holds {len(page.runs)} runs, limit {LIMIT}"
        runs.extend(page.runs)
        if not page.has_more:
            return runs
        cursor = page.cursor
    pytest.fail("the cursor never ran out")


def assert_newest_first(runs: list[WorkflowRunEntry]) -> None:
    order = [(-r.created_at, r.run_id) for r in runs]
    assert order == sorted(order), "runs are not newest first (then by id)"


async def test_list_runs_pages_every_run_once_newest_first(client: FloClient) -> None:
    ns = fresh_namespace()
    want: dict[str, str] = {}  # run id -> workflow
    for w in range(3):
        name = f"wf-{w}"
        await create(client, ns, name)
        for _ in range(8):
            want[await client.workflow.start(name, "{}", WorkflowStartOptions(namespace=ns))] = name

    runs = await list_all(client, ns, None)
    ids = [r.run_id for r in runs]
    dups = sorted({i for i in ids if ids.count(i) > 1})
    assert not dups, f"listed more than once: {dups}"
    assert {r.run_id: r.workflow for r in runs} == want
    assert_newest_first(runs)

    for name in ("wf-0", "wf-2"):
        runs = await list_all(client, ns, name)
        assert sorted(r.run_id for r in runs) == sorted(i for i, w in want.items() if w == name)
        assert_newest_first(runs)


@pytest.mark.parametrize("run_id", ["order-123", "run-1", str(uuid.uuid4())])
async def test_a_run_started_with_its_own_id_is_found_by_it(client: FloClient, run_id: str) -> None:
    ns = fresh_namespace()
    await create(client, ns, "wf-a")
    await create(client, ns, "wf-b")

    started = await client.workflow.start(
        "wf-a", "{}", WorkflowStartOptions(namespace=ns, run_id=run_id)
    )
    assert started == run_id

    status = await client.workflow.status(run_id, WorkflowStatusOptions(namespace=ns))
    assert (status["run_id"], status["workflow"]) == (run_id, "wf-a")

    history = await client.workflow.history(run_id, WorkflowHistoryOptions(namespace=ns))
    (events,) = struct.unpack_from("<I", history, 0)
    assert events >= 1

    with pytest.raises(ConflictError):
        await client.workflow.start("wf-b", "{}", WorkflowStartOptions(namespace=ns, run_id=run_id))

    await client.workflow.signal(run_id, "nudge", b"{}", WorkflowSignalOptions(namespace=ns))
    await client.workflow.cancel(run_id, "done", WorkflowCancelOptions(namespace=ns))
    status = await client.workflow.status(run_id, WorkflowStatusOptions(namespace=ns))
    assert status["status"] == "cancelled"

    runs = await list_all(client, ns, None)
    assert [(r.run_id, r.workflow) for r in runs] == [(run_id, "wf-a")]
