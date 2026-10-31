"""Lists that span several pages on a multi-shard server come back whole: every
name once, paging only by has_more and the cursor, never by a short page."""

import secrets
from collections.abc import Awaitable, Callable

from flo import (
    ActionListOptions,
    ActionRegisterOptions,
    FloClient,
    ProcessingCancelOptions,
    ProcessingListOptions,
    ProcessingSubmitOptions,
    PutOptions,
    ScanOptions,
    WorkflowCreateOptions,
    WorkflowListDefinitionsOptions,
)

LIMIT = 3

# One page: its names, and the cursor for the next (None on the last).
Page = tuple[list[str], bytes | None]


def fresh_namespace() -> str:
    return f"py-paging-{secrets.token_hex(4)}"


async def page_all(fetch: Callable[[bytes | None], Awaitable[Page]]) -> list[str]:
    """Every name across the pages, in order."""
    names: list[str] = []
    cursor: bytes | None = None
    pages = 0
    while True:
        page, cursor = await fetch(cursor)
        pages += 1
        assert len(page) <= LIMIT, f"page {pages} holds {len(page)} names, limit {LIMIT}"
        names.extend(page)
        if cursor is None:
            return names
        assert pages < 1000, "the cursor never ran out"


def assert_exact(names: list[str], want: set[str]) -> None:
    dups = sorted({n for n in names if names.count(n) > 1})
    assert not dups, f"listed more than once: {dups}"
    assert set(names) == want, (
        f"missing {sorted(want - set(names))}, unexpected {sorted(set(names) - want)}"
    )


async def test_kv_scan_pages_every_key_once(client: FloClient) -> None:
    ns = fresh_namespace()
    want = {f"k-{i:03d}" for i in range(40)}
    for key in want:
        await client.kv.put(key, b"v", PutOptions(namespace=ns))

    async def fetch(cursor: bytes | None) -> Page:
        r = await client.kv.scan("", ScanOptions(namespace=ns, limit=LIMIT, cursor=cursor))
        return r.names, r.cursor

    names = await page_all(fetch)
    assert_exact(names, want)


async def test_action_list_pages_every_action_once(client: FloClient) -> None:
    ns = fresh_namespace()
    want = {f"act-{i:03d}" for i in range(30)}
    for name in want:
        await client.action.register(name, options=ActionRegisterOptions(namespace=ns))

    async def fetch(cursor: bytes | None) -> Page:
        r = await client.action.list(ActionListOptions(namespace=ns, limit=LIMIT, cursor=cursor))
        return r.names, r.cursor

    names = await page_all(fetch)
    assert_exact(names, want)


WORKFLOW_YAML = """\
kind: Workflow
name: {name}
version: "1.0.0"

start:
  run: "@actions/validate-expense"
  transitions:
    success: flo.Completed
    failure: flo.Failed
"""


async def test_workflow_list_definitions_pages_every_definition_once(
    client: FloClient,
) -> None:
    ns = fresh_namespace()
    want = {f"wf-{i:03d}" for i in range(30)}
    for name in want:
        await client.workflow.create(
            name, WORKFLOW_YAML.format(name=name), WorkflowCreateOptions(namespace=ns)
        )

    async def fetch(cursor: bytes | None) -> Page:
        r = await client.workflow.list_definitions(
            WorkflowListDefinitionsOptions(namespace=ns, limit=LIMIT, cursor=cursor)
        )
        return [e.name for e in r.definitions], r.cursor

    names = await page_all(fetch)
    assert_exact(names, want)


# A KV sink: outside "default" the server refuses a stream sink that hashes
# to another shard than the job, and twenty jobs would land on several.
PROCESSING_YAML = """\
kind: Processing
name: {name}
namespace: {ns}
sources:
  - stream:
      name: {name}-in
operators:
  - type: keyby
    name: by-id
    key_expression: $.id
sinks:
  - kv:
      namespace: {ns}
      key_prefix: {name}
batch_size: 100
"""


async def test_processing_list_pages_every_job_once(client: FloClient) -> None:
    ns = fresh_namespace()
    want = {f"job-{i:03d}" for i in range(20)}
    job_ids = []
    for name in want:
        job_ids.append(
            await client.processing.submit(
                PROCESSING_YAML.format(name=name, ns=ns), ProcessingSubmitOptions(namespace=ns)
            )
        )
    try:

        async def fetch(cursor: bytes | None) -> Page:
            r = await client.processing.list(
                ProcessingListOptions(namespace=ns, limit=LIMIT, cursor=cursor)
            )
            return [e.name for e in r.jobs], r.cursor

        names = await page_all(fetch)
        assert_exact(names, want)
    finally:
        for job_id in job_ids:
            await client.processing.cancel(job_id, ProcessingCancelOptions(namespace=ns))
