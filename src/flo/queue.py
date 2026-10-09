"""Flo Queue Operations

Queue operations for Flo client.
"""

from typing import TYPE_CHECKING

from .types import (
    AckOptions,
    DequeueOptions,
    DequeueResult,
    DlqListOptions,
    DlqRequeueOptions,
    EnqueueOptions,
    NackOptions,
    OpCode,
    OptionTag,
    PeekOptions,
)
from .wire import OptionsBuilder, parse_dequeue_response, parse_enqueue_response, serialize_seqs

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
        message as it hands it out, so it is never redelivered.

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
    ) -> DequeueResult:
        """List messages in the Dead Letter Queue.

        Queues are currently at-most-once, so messages don't reach the DLQ in
        normal use.

        Args:
            queue: Queue name.
            options: Optional DLQ list options (limit).

        Returns:
            DequeueResult containing list of DLQ messages.

        Example:
            result = await client.queue.dlq_list("tasks", DlqListOptions(limit=100))
            for msg in result.messages:
                print(f"Failed message {msg.seq}: {msg.payload}")
        """
        opts = options or DlqListOptions()
        namespace = self._client.get_namespace(opts.namespace)

        # Build TLV options
        builder = OptionsBuilder()
        builder.add_u32(OptionTag.LIMIT, opts.limit)

        response = await self._client._send_and_check(
            OpCode.QUEUE_DLQ_LIST,
            namespace,
            queue.encode("utf-8"),
            b"",
            builder.build(),
        )

        return parse_dequeue_response(response.data)

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
        count: int,
        options: PeekOptions | None = None,
    ) -> DequeueResult:
        """Peek at messages without creating leases.

        Unlike dequeue, peek does not make messages invisible to other consumers.
        Use this to inspect queue contents without affecting message visibility.

        Args:
            queue: Queue name.
            count: Maximum number of messages to peek.
            options: Optional peek options.

        Returns:
            DequeueResult containing list of messages.

        Example:
            # Peek at next 5 messages without consuming them
            result = await client.queue.peek("tasks", 5)
            for msg in result.messages:
                print(f"Message {msg.seq}: {msg.payload}")
        """
        opts = options or PeekOptions()
        namespace = self._client.get_namespace(opts.namespace)

        # Build TLV options
        builder = OptionsBuilder()
        builder.add_u32(OptionTag.COUNT, count)

        response = await self._client._send_and_check(
            OpCode.QUEUE_PEEK,
            namespace,
            queue.encode("utf-8"),
            b"",
            builder.build(),
        )

        return parse_dequeue_response(response.data)
