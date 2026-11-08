"""Flo Queue Operations

Queue operations for Flo client.
"""

import struct
from typing import TYPE_CHECKING

from .exceptions import IncompleteResponseError, ValidationError
from .kv_body import MAX_PEEK, decode_peek_answer, encode_peek_body
from .types import (
    AckOptions,
    DeadLetter,
    DequeueOptions,
    DequeueResult,
    DlqListOptions,
    DlqPage,
    DlqRequeueOptions,
    EnqueueOptions,
    NackOptions,
    OpCode,
    OptionTag,
    PeekOptions,
    PeekResult,
)
from .wire import OptionsBuilder, parse_dequeue_response, parse_enqueue_response, serialize_seqs

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
        """Enqueue a message to a queue.

        Args:
            queue: Queue name.
            payload: Message payload.
            options: Optional enqueue options (priority).

        Returns:
            Sequence number of the enqueued message.

        Example:
            # Simple enqueue
            seq = await client.queue.enqueue("tasks", b'{"task": "process"}')

            # With priority (0-255, lower is taken first; default 0)
            seq = await client.queue.enqueue("tasks", payload, EnqueueOptions(priority=10))
        """
        opts = options or EnqueueOptions()
        namespace = self._client.get_namespace(opts.namespace)

        # Build TLV options
        builder = OptionsBuilder()

        if opts.priority != 0:
            builder.add_u8(OptionTag.PRIORITY, opts.priority)

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
        count: int,
        options: DequeueOptions | None = None,
    ) -> DequeueResult:
        """Dequeue messages from a queue.

        Queues are currently at-most-once: the server acknowledges each
        message as it hands it out, so it is not redelivered in normal
        operation, even if the consumer crashes.

        Args:
            queue: Queue name.
            count: Maximum number of messages to dequeue.
            options: Optional dequeue options (block_ms).

        Returns:
            DequeueResult containing list of messages.

        Example:
            # Dequeue up to 10 messages
            result = await client.queue.dequeue("tasks", 10)
            for msg in result.messages:
                process(msg.payload)

            # With long polling (wait up to 30s for messages)
            result = await client.queue.dequeue(
                "tasks", 10,
                DequeueOptions(block_ms=30000)
            )
        """
        opts = options or DequeueOptions()
        namespace = self._client.get_namespace(opts.namespace)

        # Build TLV options
        builder = OptionsBuilder()
        builder.add_u32(OptionTag.COUNT, count)

        if opts.block_ms is not None:
            builder.add_u32(OptionTag.BLOCK_MS, opts.block_ms)

        response = await self._client._send_and_check(
            OpCode.QUEUE_DEQUEUE,
            namespace,
            queue.encode("utf-8"),
            b"",
            builder.build(),
        )

        return parse_dequeue_response(response.data)

    async def ack(
        self,
        queue: str,
        seqs: list[int],
        options: AckOptions | None = None,
    ) -> None:
        """Acknowledge messages.

        Queues are currently at-most-once: dequeue already acknowledges each
        message it hands out, so this has no effect on a dequeued message.

        Args:
            queue: Queue name.
            seqs: Sequence numbers of messages to acknowledge.
            options: Optional ack options.
        """
        if not seqs:
            return

        opts = options or AckOptions()
        namespace = self._client.get_namespace(opts.namespace)

        value = serialize_seqs(seqs)

        await self._client._send_and_check(
            OpCode.QUEUE_COMPLETE,
            namespace,
            queue.encode("utf-8"),
            value,
        )

    async def nack(
        self,
        queue: str,
        seqs: list[int],
        options: NackOptions | None = None,
    ) -> None:
        """Negatively acknowledge messages.

        Queues are currently at-most-once: dequeue already acknowledges each
        message it hands out, so this has no effect on a dequeued message and
        does not retry it.

        Args:
            queue: Queue name.
            seqs: Sequence numbers of messages to nack.
            options: Optional nack options.
        """
        if not seqs:
            return

        opts = options or NackOptions()
        namespace = self._client.get_namespace(opts.namespace)

        value = serialize_seqs(seqs)

        await self._client._send_and_check(
            OpCode.QUEUE_FAIL,
            namespace,
            queue.encode("utf-8"),
            value,
        )

    async def dlq_list(
        self,
        queue: str,
        options: DlqListOptions | None = None,
    ) -> DlqPage:
        """List a page of the Dead Letter Queue, oldest first.

        Queues are currently at-most-once, so messages don't reach the DLQ in
        normal use.

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

    async def dlq_requeue(
        self,
        queue: str,
        seqs: list[int],
        options: DlqRequeueOptions | None = None,
    ) -> None:
        """Move messages from DLQ back to the main queue.

        Queues are currently at-most-once, so messages don't reach the DLQ in
        normal use.

        Args:
            queue: Queue name.
            seqs: Sequence numbers of DLQ messages to requeue.
            options: Optional requeue options.

        Example:
            # Requeue all DLQ messages
            result = await client.queue.dlq_list("tasks")
            seqs = [msg.seq for msg in result.messages]
            await client.queue.dlq_requeue("tasks", seqs)
        """
        if not seqs:
            return

        opts = options or DlqRequeueOptions()
        namespace = self._client.get_namespace(opts.namespace)

        value = serialize_seqs(seqs)

        await self._client._send_and_check(
            OpCode.QUEUE_DLQ_REQUEUE,
            namespace,
            queue.encode("utf-8"),
            value,
        )

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
