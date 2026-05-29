"""Flo Stream Operations

Stream (append-only log) operations for Flo client.
Uses StreamID-native positioning (timestamp_ms + sequence).
"""

from typing import TYPE_CHECKING

from .types import (
    OpCode,
    OptionTag,
    PendingEntry,
    StreamAppendOptions,
    StreamAppendResult,
    StreamClaimResult,
    StreamGroupAckOptions,
    StreamGroupJoinOptions,
    StreamGroupNackOptions,
    StreamGroupReadOptions,
    StreamID,
    StreamInfo,
    StreamInfoOptions,
    StreamReadOptions,
    StreamReadResult,
    StreamTrimOptions,
)
from .wire import (
    OptionsBuilder,
    build_stream_batch_value,
    parse_pending_entries,
    parse_stream_append_response,
    parse_stream_info_response,
    parse_stream_read_response,
    serialize_group_ack_value,
    serialize_group_claim_value,
    serialize_group_pending_value,
    serialize_group_value,
)

# u64 max sentinel: group_claim returns StreamID.MAX when the PEL is fully
# scanned (mirrors Go's math.MaxUint64 check).
_U64_MAX = 0xFFFFFFFFFFFFFFFF

if TYPE_CHECKING:
    from .client import FloClient


class StreamOperations:
    """Stream operations mixin for FloClient."""

    def __init__(self, client: "FloClient"):
        self._client = client

    async def append(
        self,
        stream: str,
        payload: bytes,
        options: StreamAppendOptions | None = None,
    ) -> StreamAppendResult:
        """Append a record to a stream.

        Args:
            stream: Stream name.
            payload: Record payload.
            options: Optional append options.

        Returns:
            StreamAppendResult with id (StreamID).

        Example:
            result = await client.stream.append("events", b'{"event": "click"}')
            print(f"Appended: id={result.id}")
        """
        opts = options or StreamAppendOptions()
        namespace = self._client.get_namespace(opts.namespace)
        value = build_stream_batch_value(payload, opts.headers)

        response = await self._client._send_and_check(
            OpCode.STREAM_APPEND,
            namespace,
            stream.encode("utf-8"),
            value,
            allow_not_found=True,
        )

        return parse_stream_append_response(response.data)

    async def read(
        self,
        stream: str,
        options: StreamReadOptions | None = None,
    ) -> StreamReadResult:
        """Read records from a stream.

        Uses StreamID-native positioning (timestamp_ms + sequence).

        Args:
            stream: Stream name.
            options: Optional read options (start, end, tail, partition, count, block_ms).

        Returns:
            StreamReadResult with list of records.

        Example:
            # Read from beginning
            result = await client.stream.read("events")

            # Read from tail (latest)
            result = await client.stream.read("events", StreamReadOptions(tail=True, count=10))

            # Read from specific StreamID
            from flo.types import StreamID
            result = await client.stream.read("events", StreamReadOptions(
                start=StreamID(timestamp_ms=0, sequence=100), count=10
            ))

            # Blocking read (long polling)
            result = await client.stream.read("events", StreamReadOptions(
                start=StreamID(timestamp_ms=0, sequence=100), block_ms=30000
            ))
        """
        opts = options or StreamReadOptions()
        namespace = self._client.get_namespace(opts.namespace)

        # Build TLV options
        builder = OptionsBuilder()

        # Tail mode flag (mutually exclusive with start)
        if opts.tail:
            builder.add_flag(OptionTag.STREAM_TAIL)

        # Start StreamID (16 bytes)
        if opts.start is not None:
            builder.add_bytes(OptionTag.STREAM_START, opts.start.to_bytes())

        # End StreamID (16 bytes)
        if opts.end is not None:
            builder.add_bytes(OptionTag.STREAM_END, opts.end.to_bytes())

        # Explicit partition
        if opts.partition is not None:
            builder.add_u32(OptionTag.PARTITION, opts.partition)

        if opts.count is not None:
            builder.add_u32(OptionTag.COUNT, opts.count)

        if opts.block_ms is not None:
            builder.add_u32(OptionTag.BLOCK_MS, opts.block_ms)

        response = await self._client._send_and_check(
            OpCode.STREAM_READ,
            namespace,
            stream.encode("utf-8"),
            b"",
            builder.build(),
            allow_not_found=True,
        )

        return parse_stream_read_response(response.data)

    async def info(
        self,
        stream: str,
        options: StreamInfoOptions | None = None,
    ) -> StreamInfo:
        """Get stream metadata.

        Args:
            stream: Stream name.
            options: Optional info options.

        Returns:
            StreamInfo with first_id, last_id, count, bytes_size.

        Example:
            info = await client.stream.info("events")
            print(f"Stream has {info.count} records ({info.bytes_size} bytes)")
        """
        opts = options or StreamInfoOptions()
        namespace = self._client.get_namespace(opts.namespace)

        response = await self._client._send_and_check(
            OpCode.STREAM_INFO,
            namespace,
            stream.encode("utf-8"),
            b"",
            allow_not_found=True,
        )

        return parse_stream_info_response(response.data)

    async def trim(
        self,
        stream: str,
        options: StreamTrimOptions | None = None,
    ) -> None:
        """Trim a stream based on retention policy.

        Args:
            stream: Stream name.
            options: Trim options (max_len, max_age_seconds, max_bytes, dry_run).

        Example:
            # Keep only last 1000 records
            await client.stream.trim("events", StreamTrimOptions(max_len=1000))

            # Delete records older than 1 day
            await client.stream.trim("events", StreamTrimOptions(max_age_seconds=86400))

            # Preview what would be deleted
            await client.stream.trim("events", StreamTrimOptions(max_len=1000, dry_run=True))
        """
        opts = options or StreamTrimOptions()
        namespace = self._client.get_namespace(opts.namespace)

        builder = OptionsBuilder()

        if opts.max_len is not None:
            builder.add_u64(OptionTag.RETENTION_COUNT, opts.max_len)

        if opts.max_age_seconds is not None:
            builder.add_u64(OptionTag.RETENTION_AGE, opts.max_age_seconds)

        if opts.max_bytes is not None:
            builder.add_u64(OptionTag.RETENTION_BYTES, opts.max_bytes)

        if opts.dry_run:
            builder.add_flag(OptionTag.DRY_RUN)

        await self._client._send_and_check(
            OpCode.STREAM_TRIM,
            namespace,
            stream.encode("utf-8"),
            b"",
            builder.build(),
            allow_not_found=True,
        )

    async def group_join(
        self,
        stream: str,
        group: str,
        consumer: str,
        options: StreamGroupJoinOptions | None = None,
    ) -> None:
        """Join a consumer group.

        Args:
            stream: Stream name.
            group: Consumer group name.
            consumer: Consumer ID (unique within the group).
            options: Optional join options.

        Example:
            await client.stream.group_join("events", "processors", "worker-1")
        """
        opts = options or StreamGroupJoinOptions()
        namespace = self._client.get_namespace(opts.namespace)

        value = serialize_group_value(group, consumer)

        await self._client._send_and_check(
            OpCode.STREAM_GROUP_JOIN,
            namespace,
            stream.encode("utf-8"),
            value,
            allow_not_found=True,
        )

    async def group_leave(
        self,
        stream: str,
        group: str,
        consumer: str,
        options: StreamGroupJoinOptions | None = None,
    ) -> None:
        """Leave a consumer group.

        Args:
            stream: Stream name.
            group: Consumer group name.
            consumer: Consumer ID.
            options: Optional leave options.

        Example:
            await client.stream.group_leave("events", "processors", "worker-1")
        """
        opts = options or StreamGroupJoinOptions()
        namespace = self._client.get_namespace(opts.namespace)

        value = serialize_group_value(group, consumer)

        await self._client._send_and_check(
            OpCode.STREAM_GROUP_LEAVE,
            namespace,
            stream.encode("utf-8"),
            value,
            allow_not_found=True,
        )

    async def group_read(
        self,
        stream: str,
        group: str,
        consumer: str,
        options: StreamGroupReadOptions | None = None,
    ) -> StreamReadResult:
        """Read new records from a consumer group, advancing last_delivered_id.

        Records are distributed among consumers in the group; each record is
        delivered to only one consumer and added to that consumer's Pending
        Entry List (PEL) until acked. Unacknowledged records are redelivered.

        Crash recovery: ``group_read`` alone is NOT sufficient. It only returns
        records past ``last_delivered_id``, so a record delivered-but-unacked at
        crash time is never re-surfaced by a later ``group_read``. To re-process
        in-flight work after a reconnect, drain the PEL with :meth:`group_claim`
        (StreamWorker does this automatically — see
        ``redeliver_pending_on_reconnect``).

        Args:
            stream: Stream name.
            group: Consumer group name.
            consumer: Consumer ID.
            options: Optional read options (count, block_ms).

        Returns:
            StreamReadResult with list of records.

        Example:
            result = await client.stream.group_read("events", "processors", "worker-1")
            for record in result.records:
                process(record.payload)
                await client.stream.group_ack("events", "processors", [record.id])
        """
        opts = options or StreamGroupReadOptions()
        namespace = self._client.get_namespace(opts.namespace)

        # Build TLV options
        builder = OptionsBuilder()

        if opts.count is not None:
            builder.add_u32(OptionTag.COUNT, opts.count)

        if opts.block_ms is not None:
            builder.add_u32(OptionTag.BLOCK_MS, opts.block_ms)

        value = serialize_group_value(group, consumer)

        response = await self._client._send_and_check(
            OpCode.STREAM_GROUP_READ,
            namespace,
            stream.encode("utf-8"),
            value,
            builder.build(),
            allow_not_found=True,
        )

        return parse_stream_read_response(response.data)

    async def group_pending(
        self,
        stream: str,
        group: str,
        consumer: str = "",
        options: StreamGroupReadOptions | None = None,
    ) -> list[PendingEntry]:
        """List a consumer group's pending (delivered-but-unacked) entries.

        If ``consumer`` is non-empty, only that consumer's entries are returned;
        otherwise the whole group's PEL is returned. (FLO-102)

        Returns:
            A list of :class:`PendingEntry`.
        """
        opts = options or StreamGroupReadOptions()
        namespace = self._client.get_namespace(opts.namespace)

        value = serialize_group_pending_value(group, consumer)

        response = await self._client._send_and_check(
            OpCode.STREAM_GROUP_PENDING,
            namespace,
            stream.encode("utf-8"),
            value,
            allow_not_found=True,
        )

        return parse_pending_entries(response.data)

    async def group_claim(
        self,
        stream: str,
        group: str,
        consumer: str,
        min_idle_ms: int,
        start_id: StreamID,
        count: int,
        options: StreamGroupReadOptions | None = None,
    ) -> StreamClaimResult:
        """Claim a page of a consumer group's pending entries for ``consumer``.

        Scans the PEL in StreamID order from ``start_id`` and takes up to
        ``count`` entries idle for at least ``min_idle_ms``. Returns the claimed
        records (payload + headers) plus a cursor for the next page. (FLO-102)

        * Drain own pending (reconnect): ``min_idle_ms=0``, ``start_id=StreamID()``.
        * Steal from idle consumers (rebalance): ``min_idle_ms > 0``.

        Loop until ``result.done`` to fully drain::

            cursor = StreamID()
            while True:
                r = await client.stream.group_claim(
                    stream, group, consumer, 0, cursor, 100)
                for rec in r.records:
                    process(rec)
                if r.done or not r.records:
                    break
                cursor = r.next_cursor
        """
        opts = options or StreamGroupReadOptions()
        namespace = self._client.get_namespace(opts.namespace)

        value = serialize_group_claim_value(group, consumer, min_idle_ms, start_id, count)

        response = await self._client._send_and_check(
            OpCode.STREAM_GROUP_CLAIM,
            namespace,
            stream.encode("utf-8"),
            value,
            allow_not_found=True,
        )

        # Response = <records blob> + [next_ts:u64][next_seq:u64] trailer.
        data = response.data or b""
        if len(data) < 16:
            return StreamClaimResult(records=[], next_cursor=StreamID(), done=True)

        cursor_off = len(data) - 16
        next_ts = int.from_bytes(data[cursor_off : cursor_off + 8], "little")
        next_seq = int.from_bytes(data[cursor_off + 8 : cursor_off + 16], "little")

        read = parse_stream_read_response(data[:cursor_off])

        # StreamID.MAX (max, max) is the "fully scanned" sentinel.
        done = next_ts == _U64_MAX and next_seq == _U64_MAX
        return StreamClaimResult(
            records=read.records,
            next_cursor=StreamID(timestamp_ms=next_ts, sequence=next_seq),
            done=done,
        )

    async def group_ack(
        self,
        stream: str,
        group: str,
        ids: list[StreamID],
        options: StreamGroupAckOptions | None = None,
    ) -> None:
        """Acknowledge records in a consumer group.

        Args:
            stream: Stream name.
            group: Consumer group name.
            ids: StreamIDs of records to acknowledge.
            options: Optional ack options (including consumer name).

        Example:
            result = await client.stream.group_read("events", "processors", "worker-1")
            for record in result.records:
                try:
                    process(record.payload)
                    await client.stream.group_ack("events", "processors", [record.id],
                        StreamGroupAckOptions(consumer="worker-1"))
                except Exception:
                    pass  # Record will be redelivered
        """
        if not ids:
            return

        opts = options or StreamGroupAckOptions()
        namespace = self._client.get_namespace(opts.namespace)

        value = serialize_group_ack_value(group, ids, consumer=opts.consumer)

        await self._client._send_and_check(
            OpCode.STREAM_GROUP_ACK,
            namespace,
            stream.encode("utf-8"),
            value,
            allow_not_found=True,
        )

    async def group_nack(
        self,
        stream: str,
        group: str,
        ids: list[StreamID],
        options: StreamGroupNackOptions | None = None,
    ) -> None:
        """Negatively acknowledge records in a consumer group for redelivery.

        Args:
            stream: Stream name.
            group: Consumer group name.
            ids: StreamIDs of records to negatively acknowledge.
            options: Optional nack options (consumer name, redelivery delay).

        Example:
            result = await client.stream.group_read("events", "processors", "worker-1")
            for record in result.records:
                try:
                    process(record.payload)
                except Exception:
                    await client.stream.group_nack("events", "processors", [record.id],
                        StreamGroupNackOptions(consumer="worker-1", redelivery_delay_ms=5000))
        """
        if not ids:
            return

        opts = options or StreamGroupNackOptions()
        namespace = self._client.get_namespace(opts.namespace)

        value = serialize_group_ack_value(group, ids, consumer=opts.consumer)

        builder = OptionsBuilder()
        if opts.redelivery_delay_ms is not None:
            builder.add_u32(OptionTag.REDELIVERY_DELAY_MS, opts.redelivery_delay_ms)

        await self._client._send_and_check(
            OpCode.STREAM_GROUP_NACK,
            namespace,
            stream.encode("utf-8"),
            value,
            builder.build(),
            allow_not_found=True,
        )
