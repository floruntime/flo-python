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
    TouchOptions,
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
            options: Optional enqueue options (priority, delay, dedup_key).

        Returns:
            Sequence number of the enqueued message.

        Example:
            # Simple enqueue
            seq = await client.queue.enqueue("tasks", b'{"task": "process"}')

            # With priority (higher = more urgent)
            seq = await client.queue.enqueue("tasks", payload, EnqueueOptions(priority=10))

            # With delay
            seq = await client.queue.enqueue("tasks", payload, EnqueueOptions(delay_ms=60000))

            # With deduplication
            seq = await client.queue.enqueue("tasks", payload, EnqueueOptions(dedup_key="task-123"))
        """
        opts = options or EnqueueOptions()
        namespace = self._client.get_namespace(opts.namespace)

        # Build TLV options
        builder = OptionsBuilder()

        if opts.priority != 0:
            builder.add_u8(OptionTag.PRIORITY, opts.priority)

        if opts.delay_ms is not None:
            builder.add_u64(OptionTag.DELAY_MS, opts.delay_ms)

        if opts.dedup_key is not None:
            builder.add_bytes(OptionTag.DEDUP_KEY, opts.dedup_key.encode("utf-8"))

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

        Args:
            queue: Queue name.
            count: Maximum number of messages to dequeue.
            options: Optional dequeue options (visibility_timeout, block_ms).

        Returns:
            DequeueResult containing list of messages.

        Example:
            # Dequeue up to 10 messages
            result = await client.queue.dequeue("tasks", 10)
            for msg in result.messages:
                process(msg.payload)
                await client.queue.ack("tasks", [msg.seq])

            # With long polling (wait up to 30s for messages)
            result = await client.queue.dequeue(
                "tasks", 10,
                DequeueOptions(block_ms=30000)
            )

            # With custom visibility timeout
            result = await client.queue.dequeue(
                "tasks", 10,
                DequeueOptions(visibility_timeout_ms=60000)
            )
        """
        opts = options or DequeueOptions()
        namespace = self._client.get_namespace(opts.namespace)

        # Build TLV options
        builder = OptionsBuilder()
        builder.add_u32(OptionTag.COUNT, count)

        if opts.visibility_timeout_ms is not None:
            builder.add_u32(OptionTag.VISIBILITY_TIMEOUT_MS, opts.visibility_timeout_ms)

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
        """Acknowledge messages as successfully processed.

        Args:
            queue: Queue name.
            seqs: Sequence numbers of messages to acknowledge.
            options: Optional ack options.

        Example:
            result = await client.queue.dequeue("tasks", 10)
            for msg in result.messages:
                try:
                    process(msg.payload)
                    await client.queue.ack("tasks", [msg.seq])
                except Exception:
                    await client.queue.nack("tasks", [msg.seq])
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
        """Negative acknowledge messages (retry or send to DLQ).

        Args:
            queue: Queue name.
            seqs: Sequence numbers of messages to nack.
            options: Optional nack options (to_dlq).

        Example:
            # Retry the message
            await client.queue.nack("tasks", [msg.seq])

            # Send to DLQ (don't retry)
            await client.queue.nack("tasks", [msg.seq], NackOptions(to_dlq=True))
        """
        if not seqs:
            return

        opts = options or NackOptions()
        namespace = self._client.get_namespace(opts.namespace)

        # Build TLV options
        builder = OptionsBuilder()

        if opts.to_dlq:
            builder.add_u8(OptionTag.SEND_TO_DLQ, 1)

        value = serialize_seqs(seqs)

        await self._client._send_and_check(
            OpCode.QUEUE_FAIL,
            namespace,
            queue.encode("utf-8"),
            value,
            builder.build(),
        )

    async def dlq_list(
        self,
        queue: str,
        options: DlqListOptions | None = None,
    ) -> DequeueResult:
        """List messages in the Dead Letter Queue.

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

    async def touch(
        self,
        queue: str,
        seqs: list[int],
        options: TouchOptions | None = None,
    ) -> None:
        """Renew lease on messages to prevent visibility timeout.

        Call this periodically during long-running processing to prevent
        the message from becoming visible again before you're done.

        Args:
            queue: Queue name.
            seqs: Sequence numbers of messages to touch.
            options: Optional touch options.

        Example:
            result = await client.queue.dequeue("tasks", 1)
            msg = result.messages[0]

            # Long running task - renew lease periodically
            for chunk in process_in_chunks(msg.payload):
                await process_chunk(chunk)
                await client.queue.touch("tasks", [msg.seq])

            await client.queue.ack("tasks", [msg.seq])
        """
        if not seqs:
            return

        opts = options or TouchOptions()
        namespace = self._client.get_namespace(opts.namespace)

        value = serialize_seqs(seqs)

        await self._client._send_and_check(
            OpCode.QUEUE_TOUCH,
            namespace,
            queue.encode("utf-8"),
            value,
        )
