"""Flo Queue Operations

Queue operations for Flo client.
"""

import struct
from typing import TYPE_CHECKING

from .exceptions import IncompleteResponseError, ValidationError
from .kv_body import (
    MAX_PEEK,
    decode_dequeue_answer,
    decode_peek_answer,
    decode_queue_report_answer,
    decode_queue_touch_answer,
    encode_dequeue_body,
    encode_peek_body,
    encode_queue_complete_body,
    encode_queue_fail_body,
)
from .types import (
    DeadLetter,
    DequeueOptions,
    DequeueResult,
    DlqListOptions,
    DlqPage,
    EnqueueOptions,
    Message,
    OpCode,
    OptionTag,
    PeekOptions,
    PeekResult,
    PurgeOptions,
    QueueFailOptions,
    QueueReportOptions,
    QueueReportResult,
    QueueTouchResult,
)
from .wire import OptionsBuilder, parse_enqueue_response

# The longest an enqueued message waits before it's ready: a day.
MAX_ENQUEUE_DELAY_MS = 86_400_000
# The most dead letters one page lists.
MAX_DLQ_PAGE = 100

if TYPE_CHECKING:
    from .client import FloClient


class QueueOperations:
    """Queue operations mixin for FloClient."""

    def __init__(self, client: "FloClient"):
        self._client = client

    async def enqueue(
        self,
        queue: str,
        payload: bytes,
        options: EnqueueOptions | None = None,
    ) -> int:
        """Enqueue a message (at most 64 KiB) to a queue.

        Args:
            queue: Queue name.
            payload: Message payload.
            options: Priority (0-255, lower is taken first) and delay_ms (how
                long before it's ready, at most a day).

        Returns:
            Sequence number of the enqueued message.

        Raises:
            OverloadedError: The queue is full (its messages or held bytes,
                ready, leased and dead, are at its caps), or the shard's
                queues are. The client retries it as any overloaded refusal
                that didn't run, for about 350 ms, then raises it; consume or
                purge the queue first.

        Example:
            seq = await client.queue.enqueue("tasks", b'{"task": "process"}')
            seq = await client.queue.enqueue("tasks", payload, EnqueueOptions(priority=10))
        """
        opts = options or EnqueueOptions()
        namespace = self._client.get_namespace(opts.namespace)

        builder = OptionsBuilder()
        if opts.priority != 0:
            builder.add_u8(OptionTag.PRIORITY, opts.priority)
        if (
            not isinstance(opts.delay_ms, int)
            or isinstance(opts.delay_ms, bool)
            or not 0 <= opts.delay_ms <= MAX_ENQUEUE_DELAY_MS
        ):
            raise ValidationError(
                f"delay_ms must be 0..{MAX_ENQUEUE_DELAY_MS}, got {opts.delay_ms!r}"
            )
        if opts.delay_ms:
            builder.add_u64(OptionTag.DELAY_MS, opts.delay_ms)

        response = await self._client._send_and_check(
            OpCode.QUEUE_ENQUEUE,
            namespace,
            queue.encode("utf-8"),
            payload,
            builder.build(),
        )

        return parse_enqueue_response(response.data)

    async def dequeue(
        self,
        queue: str,
        count: int = 1,
        options: DequeueOptions | None = None,
    ) -> DequeueResult:
        """Take a queue's next ready messages (1..100), leased to the consumer.

        Delivery is at least once: complete each message within its lease
        (``complete``), fail it, or touch it to keep it; one whose lease runs
        out is ready again and delivered with ``deliveries`` one higher. A
        message failed or expired five times is dead. With
        ``DequeueOptions(ack=True)`` the messages are completed as they're
        taken.

        Example:
            result = await client.queue.dequeue("tasks", 10)
            for msg in result.messages:
                process(msg.payload)
            await client.queue.complete("tasks", result)

            # Long polling: wait up to 30 s for a message
            result = await client.queue.dequeue("tasks", 10, DequeueOptions(block_ms=30000))
        """
        opts = options or DequeueOptions()
        response = await self._client._send_and_check(
            OpCode.QUEUE_DEQUEUE,
            self._client.get_namespace(opts.namespace),
            queue.encode("utf-8"),
            encode_dequeue_body(opts.consumer, count, opts.lease_ms, opts.block_ms, opts.ack),
        )
        return decode_dequeue_answer(response.data, opts.consumer)

    async def complete(
        self,
        queue: str,
        delivery: DequeueResult,
        messages: list[Message] | None = None,
        options: QueueReportOptions | None = None,
    ) -> QueueReportResult:
        """Complete dequeued messages, all of the dequeue's unless
        ``messages`` names some: they leave the queue.

        A message the report doesn't apply to comes back in ``refused`` with
        why; the call itself succeeds. ``GONE`` means done (completed already,
        or not in this queue), so ``result.lost`` lists only the messages that
        go to another delivery. Resending a complete is safe.
        """
        opts = options or QueueReportOptions()
        response = await self._client._send_and_check(
            OpCode.QUEUE_COMPLETE,
            self._client.get_namespace(opts.namespace),
            queue.encode("utf-8"),
            encode_queue_complete_body(delivery, messages),
        )
        return decode_queue_report_answer("queue.complete", response.data)

    async def fail(
        self,
        queue: str,
        delivery: DequeueResult,
        messages: list[Message] | None = None,
        options: QueueFailOptions | None = None,
    ) -> QueueReportResult:
        """Fail dequeued messages, all of the dequeue's unless ``messages``
        names some: ready again after ``delay_ms``, or dead once their
        attempts are spent (or at once with ``dead``). Messages it doesn't
        apply to come back in ``refused``."""
        opts = options or QueueFailOptions()
        response = await self._client._send_and_check(
            OpCode.QUEUE_FAIL,
            self._client.get_namespace(opts.namespace),
            queue.encode("utf-8"),
            encode_queue_fail_body(
                delivery, messages, opts.delay_ms, opts.dead, opts.error.encode("utf-8")
            ),
        )
        return decode_queue_report_answer("queue.fail", response.data)

    async def touch(
        self,
        queue: str,
        delivery: DequeueResult,
        messages: list[Message] | None = None,
        options: QueueReportOptions | None = None,
    ) -> QueueTouchResult:
        """Keep dequeued messages: with half their lease or less left, it
        runs a full lease from now, up to 12 h from their dequeue; with more
        left, nothing changes. Touch at least every half lease."""
        opts = options or QueueReportOptions()
        response = await self._client._send_and_check(
            OpCode.QUEUE_TOUCH,
            self._client.get_namespace(opts.namespace),
            queue.encode("utf-8"),
            encode_queue_complete_body(delivery, messages),
        )
        return decode_queue_touch_answer(response.data)

    async def purge(self, queue: str, options: PurgeOptions | None = None) -> int:
        """Remove every message of a queue, ready, leased and dead. Returns
        how many it removed; a queue that doesn't exist answers 0."""
        opts = options or PurgeOptions()
        response = await self._client._send_and_check(
            OpCode.QUEUE_PURGE,
            self._client.get_namespace(opts.namespace),
            queue.encode("utf-8"),
            b"",
        )
        if len(response.data) != 4:
            raise IncompleteResponseError(f"queue.purge answer is {len(response.data)} bytes")
        return int(struct.unpack("<I", response.data)[0])

    async def dlq_list(
        self,
        queue: str,
        options: DlqListOptions | None = None,
    ) -> DlqPage:
        """List a page of the Dead Letter Queue, oldest first.

        A message lands here once failed or expired five times, or failed
        with ``dead``; it stays until the queue is purged.

        Args:
            queue: Queue name.
            options: The page's limit (1..100) and ``after``, the seq that
                ended the page before (``last_seq``); None starts at the oldest.

        Returns:
            DlqPage with the page's dead letters, how many there are in all,
            and whether more follow.

        Example:
            page = await client.queue.dlq_list("tasks")
            while True:
                for msg in page.messages:
                    print(f"Failed message {msg.seq}: {msg.payload}")
                if not page.has_more:
                    break
                page = await client.queue.dlq_list(
                    "tasks", DlqListOptions(after=page.last_seq)
                )
        """
        opts = options or DlqListOptions()
        namespace = self._client.get_namespace(opts.namespace)
        if not 1 <= opts.limit <= MAX_DLQ_PAGE:
            raise ValidationError(f"dlq list limit must be 1..{MAX_DLQ_PAGE}, got {opts.limit}")

        # The page goes in the value: [limit:u32][after:u64]?
        value = struct.pack("<I", opts.limit)
        if opts.after is not None:
            value += struct.pack("<Q", opts.after)
        response = await self._client._send_and_check(
            OpCode.QUEUE_DLQ_LIST,
            namespace,
            queue.encode("utf-8"),
            value,
        )

        return parse_dlq_page(response.data)

    async def peek(
        self,
        queue: str,
        limit: int = 20,
        options: PeekOptions | None = None,
    ) -> PeekResult:
        """The next ready messages (1..100), in the order a dequeue takes
        them, without taking them. Each shows up to 4 KiB of its payload.

        Example:
            result = await client.queue.peek("tasks", 5)
            for msg in result.messages:
                print(f"Message {msg.seq}: {msg.payload}")
        """
        if not 1 <= limit <= MAX_PEEK:
            raise ValidationError(f"peek limit must be 1..{MAX_PEEK}, got {limit}")
        opts = options or PeekOptions()
        response = await self._client._send_and_check(
            OpCode.QUEUE_PEEK,
            self._client.get_namespace(opts.namespace),
            queue.encode("utf-8"),
            encode_peek_body(limit),
        )
        return decode_peek_answer(response.data)


def parse_dlq_page(data: bytes) -> DlqPage:
    """A dead letter page: [count:u32]([seq:u64][size:u32][len:u32][payload]
    [enqueued_at_ms:i64][attempts:u32][priority:u8][moved_at_ms:i64])*
    [total:u64][has_more:u8][last_seq:u64]."""
    pos = 0

    def take(n: int) -> bytes:
        nonlocal pos
        if pos + n > len(data):
            raise IncompleteResponseError("queue.dlq_list answer is cut short")
        b = data[pos : pos + n]
        pos += n
        return b

    (count,) = struct.unpack("<I", take(4))
    if count > MAX_DLQ_PAGE:
        raise IncompleteResponseError(f"queue.dlq_list answer lists {count} dead letters")
    messages = []
    for _ in range(count):
        seq, size, n = struct.unpack("<QII", take(16))
        payload = take(n)
        enqueued_at_ms, attempts, priority, moved_at_ms = struct.unpack("<qIBq", take(21))
        messages.append(
            DeadLetter(
                seq=seq,
                size=size,
                payload=payload,
                enqueued_at_ms=enqueued_at_ms,
                attempts=attempts,
                priority=priority,
                moved_at_ms=moved_at_ms,
            )
        )
    total, has_more, last_seq = struct.unpack("<QBQ", take(17))
    if has_more > 1:
        raise IncompleteResponseError(f"queue.dlq_list answer has a has_more of {has_more}")
    if pos != len(data):
        raise IncompleteResponseError(
            f"queue.dlq_list answer has {len(data) - pos} bytes left over"
        )
    return DlqPage(messages=messages, total=total, has_more=bool(has_more), last_seq=last_seq)
