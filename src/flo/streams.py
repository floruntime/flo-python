"""Flo Stream Operations

Stream (append-only log) operations for Flo client.
Uses StreamID-native positioning (timestamp_ms + sequence).
"""

from typing import TYPE_CHECKING, Optional

from .types import (
    OpCode,
    OptionTag,
    StreamAppendOptions,
    StreamAppendResult,
    StreamGroupAckOptions,
    StreamGroupJoinOptions,
    StreamGroupReadOptions,
    StreamInfo,
    StreamInfoOptions,
    StreamReadOptions,
    StreamReadResult,
    StreamTrimOptions,
)
from .wire import (
    OptionsBuilder,
    parse_stream_append_response,
    parse_stream_info_response,
    parse_stream_read_response,
    serialize_group_ack_value,
    serialize_group_value,
)

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
        options: Optional[StreamAppendOptions] = None,
    ) -> StreamAppendResult:
        """Append a record to a stream.

        Args:
            stream: Stream name.
            payload: Record payload.
            options: Optional append options.

        Returns:
            StreamAppendResult with first_offset, last_offset, count.

        Example:
            result = await client.stream.append("events", b'{"event": "click"}')
            print(f"Appended at offset {result.first_offset}")
        """
        opts = options or StreamAppendOptions()
        namespace = self._client.get_namespace(opts.namespace)

        response = await self._client._send_and_check(
            OpCode.STREAM_APPEND,
            namespace,
            stream.encode("utf-8"),
            payload,
            allow_not_found=True,
        )

        return parse_stream_append_response(response.data)

    async def read(
        self,
        stream: str,
        options: Optional[StreamReadOptions] = None,
    ) -> StreamReadResult:
        """Read records from a stream.

        Uses StreamID-native positioning (timestamp_ms + sequence).

        Args:
            stream: Stream name.
            options: Optional read options (start, end, tail, partition, count, block_ms).

        Returns:
            StreamReadResult with list of records and next_offset.

        Example:
            # Read from beginning
            result = await client.stream.read("events")

            # Read from tail (latest)
            result = await client.stream.read("events", StreamReadOptions(tail=True, count=10))

            # Read from specific StreamID
            from flo.types import StreamID
            result = await client.stream.read("events", StreamReadOptions(
                start=StreamID.from_sequence(100), count=10
            ))

            # Blocking read (long polling)
            result = await client.stream.read("events", StreamReadOptions(
                start=StreamID.from_sequence(100), block_ms=30000
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
        options: Optional[StreamInfoOptions] = None,
    ) -> StreamInfo:
        """Get stream metadata.

        Args:
            stream: Stream name.
            options: Optional info options.

        Returns:
            StreamInfo with first_seq, last_seq, count, bytes_size.

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
        options: Optional[StreamTrimOptions] = None,
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
        options: Optional[StreamGroupJoinOptions] = None,
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
        options: Optional[StreamGroupJoinOptions] = None,
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
        options: Optional[StreamGroupReadOptions] = None,
    ) -> StreamReadResult:
        """Read records from a consumer group.

        Records are distributed among consumers in the group. Each record
        is delivered to only one consumer. Unacknowledged records will be
        redelivered.

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
                await client.stream.group_ack("events", "processors", [record.seq])
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

    async def group_ack(
        self,
        stream: str,
        group: str,
        seqs: list[int],
        options: Optional[StreamGroupAckOptions] = None,
    ) -> None:
        """Acknowledge records in a consumer group.

        Args:
            stream: Stream name.
            group: Consumer group name.
            seqs: Sequence numbers of records to acknowledge.
            options: Optional ack options.

        Example:
            result = await client.stream.group_read("events", "processors", "worker-1")
            for record in result.records:
                try:
                    process(record.payload)
                    await client.stream.group_ack("events", "processors", [record.seq])
                except Exception:
                    pass  # Record will be redelivered
        """
        if not seqs:
            return

        opts = options or StreamGroupAckOptions()
        namespace = self._client.get_namespace(opts.namespace)

        value = serialize_group_ack_value(group, seqs)

        await self._client._send_and_check(
            OpCode.STREAM_GROUP_ACK,
            namespace,
            stream.encode("utf-8"),
            value,
            allow_not_found=True,
        )
