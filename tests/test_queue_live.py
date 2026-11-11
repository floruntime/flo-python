"""Queues are leased until completed, against a live server: a completed
message is gone, an expired lease is delivered again, and reports name the
dequeue's delivery."""

import asyncio
import dataclasses
import secrets

from flo import (
    DequeueOptions,
    EnqueueOptions,
    FloClient,
    PeekOptions,
    PurgeOptions,
    QueueFailOptions,
    QueueRefusalReason,
    QueueReportOptions,
)


def fresh_namespace() -> str:
    return f"sdk-live-{secrets.token_hex(4)}"


async def test_live_a_completed_message_is_gone_and_an_expired_one_comes_back(
    client: FloClient,
) -> None:
    ns = fresh_namespace()
    await client.queue.enqueue("jobs", b"one", EnqueueOptions(namespace=ns))

    # Dequeue and complete within the lease: the next dequeue gets nothing.
    lease = DequeueOptions(namespace=ns, consumer="c1", lease_ms=5000)
    got = await client.queue.dequeue("jobs", 10, lease)
    assert [(m.payload, m.deliveries) for m in got.messages] == [(b"one", 1)]
    assert got.term > 0 and got.index > 0 and got.expires_at_ms > 0
    report = await client.queue.complete("jobs", got, options=QueueReportOptions(namespace=ns))
    assert (report.applied, report.refused) == (1, [])
    assert (await client.queue.dequeue("jobs", 10, lease)).messages == []

    # A complete sent again finds it gone: done already.
    again = await client.queue.complete("jobs", got, options=QueueReportOptions(namespace=ns))
    assert [r.why for r in again.refused] == [QueueRefusalReason.GONE]
    assert again.lost == []

    # Let a lease run out: the message comes back with deliveries 2.
    await client.queue.enqueue("jobs", b"two", EnqueueOptions(namespace=ns))
    first = await client.queue.dequeue("jobs", 1, lease)
    [msg] = first.messages
    await asyncio.sleep(6.5)
    second = await client.queue.dequeue(
        "jobs", 1, DequeueOptions(namespace=ns, consumer="c1", block_ms=5000)
    )
    assert [(m.seq, m.payload, m.deliveries) for m in second.messages] == [(msg.seq, b"two", 2)]
    # The first delivery's complete is refused for that message; the second's applies.
    stale = await client.queue.complete("jobs", first, options=QueueReportOptions(namespace=ns))
    assert stale.applied == 0
    assert [(r.seq, r.why) for r in stale.refused] == [
        (msg.seq, QueueRefusalReason.DELIVERED_AGAIN)
    ]
    done = await client.queue.complete("jobs", second, options=QueueReportOptions(namespace=ns))
    assert done.applied == 1


async def test_live_touch_fail_ack_and_purge(client: FloClient) -> None:
    ns = fresh_namespace()
    opts = QueueReportOptions(namespace=ns)
    for body in (b"a", b"b", b"c"):
        await client.queue.enqueue("jobs", body, EnqueueOptions(namespace=ns))

    got = await client.queue.dequeue("jobs", 1, DequeueOptions(namespace=ns, consumer="c1"))
    touched = await client.queue.touch("jobs", got, options=opts)
    # More than half the lease left: the expiry as it stands.
    assert (touched.applied, touched.expires_at_ms) == (1, got.expires_at_ms)
    # Another consumer's report applies to nothing.
    other = await client.queue.complete(
        "jobs", dataclasses.replace(got, consumer="c2"), options=opts
    )
    assert [r.why for r in other.refused] == [QueueRefusalReason.OTHER_HOLDER]

    # Failed without a delay: ready again at once, delivered a second time.
    failed = await client.queue.fail("jobs", got, options=QueueFailOptions(namespace=ns, error="x"))
    assert failed.applied == 1
    page = await client.queue.peek("jobs", 10, PeekOptions(namespace=ns))
    assert sorted(m.payload for m in page.messages) == [b"a", b"b", b"c"]

    # With ack, taken and completed at once: nothing to report.
    acked = await client.queue.dequeue("jobs", 1, DequeueOptions(namespace=ns, ack=True))
    assert len(acked.messages) == 1 and acked.expires_at_ms == 0

    assert await client.queue.purge("jobs", PurgeOptions(namespace=ns)) == 2
    assert (await client.queue.peek("jobs", 10, PeekOptions(namespace=ns))).messages == []
