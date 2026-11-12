"""The action round trip, and the reads that show a shard's state, against a
live server: a fresh namespace per run, so a resend from an earlier run can't
be what answers."""

import dataclasses
import secrets
import time

import pytest

from flo import (
    ActionInvokeOptions,
    ActionListOptions,
    ActionRegisterOptions,
    ActionStatusOptions,
    ActionType,
    DlqListOptions,
    EnqueueOptions,
    FloClient,
    GroupMemberState,
    PeekOptions,
    RunStatus,
    StaleReportError,
    StreamAppendOptions,
    StreamGroupReadOptions,
    WorkerAwaitOptions,
    WorkerFailOptions,
)
from flo.exceptions import NotFoundError
from flo.types import TaskAssignment


def fresh_namespace() -> str:
    return f"sdk-live-{secrets.token_hex(4)}"


async def _await(client: FloClient, ns: str, key: bytes | None = None) -> TaskAssignment:
    got = await client.worker.await_task(
        "w1", ["echo"], WorkerAwaitOptions(namespace=ns, block_ms=5000, request_key=key)
    )
    assert got.task is not None, "no task within block_ms"
    return got.task


async def test_live_action_round_trip(client: FloClient) -> None:
    ns = fresh_namespace()

    # 1-2: register with a 5 s lease and 3 attempts, then invoke.
    await client.action.register(
        "echo",
        ActionType.USER,
        ActionRegisterOptions(namespace=ns, timeout_ms=5000, max_attempts=3),
    )
    invoked = await client.action.invoke("echo", b"ping", ActionInvokeOptions(namespace=ns))
    assert invoked.run_id

    # 3: the first delivery.
    task = await _await(client, ns, b"rk-1")
    assert task.run == invoked.run_id
    assert (task.namespace, task.deliveries, task.action, task.attempt) == (ns, 1, "echo", 1)
    assert (task.lease_ms, task.input) == (5000, b"ping")
    assert task.term > 0 and task.index > 0 and task.created_at_ms > 0

    # 4: the same request key gets the same delivery, not a second one.
    again = await _await(client, ns, b"rk-1")
    assert (again.run, again.deliveries, again.term, again.index) == (
        task.run,
        task.deliveries,
        task.term,
        task.index,
    )

    # 5: with more than half the lease left, a touch answers the expiry as it stands.
    expires = await client.worker.touch("w1", task)
    assert expires > int(time.time() * 1000)
    assert await client.worker.touch("w1", task) == expires

    # 6: a report naming another delivery is stale.
    with pytest.raises(StaleReportError) as stale:
        await client.worker.complete("w1", dataclasses.replace(task, deliveries=2), b"pong")
    assert str(stale.value).startswith("stale report:")

    # 7-8: complete, and the same complete again is not an error.
    await client.worker.complete("w1", task, b"pong")
    await client.worker.complete("w1", task, b"pong")

    # 9: a complete with another result is stale.
    with pytest.raises(StaleReportError) as other:
        await client.worker.complete("w1", task, b"other")
    assert str(other.value) == "stale report: the run was completed with another result"

    # 10: the run completed with its result and no named outcome.
    status = await client.action.status(task.run, ActionStatusOptions(namespace=ns))
    assert (status.status, status.output, status.outcome) == (RunStatus.COMPLETED, b"pong", None)

    # 11: a failed delivery comes back as the next attempt; an abandoned one
    # comes back without spending one.
    second = await client.action.invoke("echo", b"ping", ActionInvokeOptions(namespace=ns))
    first = await _await(client, ns)
    assert (first.run, first.deliveries, first.attempt) == (second.run_id, 1, 1)
    await client.worker.fail("w1", first, "boom", WorkerFailOptions(retry=True, delay_ms=0))
    retried = await _await(client, ns)
    assert (retried.run, retried.deliveries, retried.attempt) == (second.run_id, 2, 2)
    await client.worker.fail("w1", retried, "", WorkerFailOptions(abandoned=True))
    handed_back = await _await(client, ns)
    assert (handed_back.run, handed_back.deliveries, handed_back.attempt) == (second.run_id, 3, 2)
    await client.worker.complete("w1", handed_back, b"done")

    info = await client.action.info("echo", ns)
    assert (info.version, info.timeout_ms, info.max_attempts) == (1, 5000, 3)
    assert info.ready == 0 and not info.too_many_runs
    assert info.completed == 2


async def test_live_action_info_of_an_unknown_action_is_not_found(client: FloClient) -> None:
    # In a namespace that exists: a missing one is NoNamespaceError.
    ns = fresh_namespace()
    await client.action.register("echo", ActionType.USER, ActionRegisterOptions(namespace=ns))
    with pytest.raises(NotFoundError):
        await client.action.info("no-such-action", ns)


async def test_live_action_list_shows_each_actions_definition(client: FloClient) -> None:
    ns = fresh_namespace()
    await client.action.register(
        "resize",
        ActionType.USER,
        ActionRegisterOptions(namespace=ns, timeout_ms=60000, max_attempts=5, owner="media"),
    )
    # A page may hold fewer than its limit, none even, and still have more.
    entries = []
    cursor = None
    for _ in range(100):
        page = await client.action.list(ActionListOptions(namespace=ns, cursor=cursor))
        entries.extend(page.actions)
        if not page.has_more:
            break
        cursor = page.cursor
    [entry] = entries
    assert (entry.name, entry.owner, entry.timeout_ms, entry.max_attempts) == (
        "resize",
        "media",
        60000,
        5,
    )


async def test_live_queue_peek_shows_the_next_messages_without_taking_them(
    client: FloClient,
) -> None:
    ns = fresh_namespace()
    for body in (b"one", b"two", b"three"):
        await client.queue.enqueue("jobs", body, EnqueueOptions(namespace=ns))
    page = await client.queue.peek("jobs", 2, PeekOptions(namespace=ns))
    assert [m.payload for m in page.messages] == [b"one", b"two"]
    assert page.more is True
    assert [m.size for m in page.messages] == [3, 3]
    again = await client.queue.peek("jobs", 20, PeekOptions(namespace=ns))
    assert [m.payload for m in again.messages] == [b"one", b"two", b"three"]


async def test_live_an_empty_dlq_page(client: FloClient) -> None:
    ns = fresh_namespace()
    await client.queue.enqueue("jobs", b"x", EnqueueOptions(namespace=ns))
    page = await client.queue.dlq_list("jobs", DlqListOptions(namespace=ns, limit=10))
    assert (page.messages, page.total, page.has_more) == ([], 0, False)


async def test_live_stream_groups_and_their_members(client: FloClient) -> None:
    ns = fresh_namespace()
    await client.stream.append("events", b"e1", StreamAppendOptions(namespace=ns))
    # A member is a consumer holding records: the read makes c1 one.
    got = await client.stream.group_read(
        "events", "g1", "c1", StreamGroupReadOptions(namespace=ns, count=1)
    )
    assert len(got.records) == 1
    groups = await client.stream.group_list("events", ns)
    assert [g.name for g in groups.groups] == ["g1"]
    assert (groups.groups[0].members, groups.groups[0].pending) == (1, 1)
    members = await client.stream.group_members("events", "g1", ns)
    assert [(m.id, m.state) for m in members.members] == [("c1", GroupMemberState.ACTIVE)]
    with pytest.raises(NotFoundError):
        await client.stream.group_members("events", "no-such-group", ns)
