"""Flo Stream Operations

Stream (append-only log) operations for Flo client.
Uses StreamID-native positioning (timestamp_ms + sequence).
"""

import struct
from typing import TYPE_CHECKING

from .exceptions import IncompleteResponseError, ValidationError
from .kv_body import (
    decode_group_list_answer,
    decode_group_members_answer,
    encode_group_members_body,
)
from .types import (
    GroupReadResult,
    GroupRecord,
    GroupReportResult,
    GroupTouchResult,
    OpCode,
    OptionTag,
    PendingEntry,
    StreamAppendOptions,
    StreamAppendResult,
    StreamDeleteOptions,
    StreamGroupAckOptions,
    StreamGroupCreateOptions,
    StreamGroupInfo,
    StreamGroupListResult,
    StreamGroupMembersResult,
    StreamGroupNackOptions,
    StreamGroupReadOptions,
    StreamInfo,
    StreamInfoOptions,
    StreamReadOptions,
    StreamReadResult,
    StreamTrimOptions,
    StreamTrimResult,
)
from .wire import (
    MAX_GROUP_BATCH,
    MAX_GROUP_LEASE_MS,
    MAX_GROUP_NACK_DELAY_MS,
    MAX_GROUP_NACK_ERROR,
    MIN_GROUP_LEASE_MS,
    OptionsBuilder,
    build_stream_batch_value,
    parse_group_info_response,
    parse_group_read_response,
    parse_group_report_response,
    parse_group_touch_response,
    parse_pending_entries,
    parse_stream_append_response,
    parse_stream_info_response,
    parse_stream_read_response,
    serialize_group_report_value,
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

            # Read the records after a StreamID (start is exclusive)
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

        Raises:
            NotFoundError: No such stream.
            NoNamespaceError: The namespace was never created.

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
        )

        return parse_stream_info_response(response.data)

    async def trim(
        self,
        stream: str,
        options: StreamTrimOptions | None = None,
    ) -> StreamTrimResult:
        """Remove a stream's oldest records by exactly one bound.

        Set one of ``before``, ``max_len`` or ``max_age_seconds``; the server
        refuses none or several. Trim cuts whole append batches, so ``max_len``
        may keep a few more records than asked. With ``dry_run`` nothing is
        removed and the result says what would be.

        Example:
            # Keep only the newest 1000 records
            await client.stream.trim("events", StreamTrimOptions(max_len=1000))

            # Count what that would remove, removing nothing
            await client.stream.trim("events", StreamTrimOptions(max_len=1000, dry_run=True))
        """
        opts = options or StreamTrimOptions()
        namespace = self._client.get_namespace(opts.namespace)

        builder = OptionsBuilder()

        if opts.before is not None:
            builder.add_bytes(OptionTag.STREAM_START, opts.before.to_bytes())

        if opts.max_len is not None:
            builder.add_u64(OptionTag.LIMIT, opts.max_len)

        if opts.max_age_seconds is not None:
            builder.add_u64(OptionTag.MAX_AGE_SECONDS, opts.max_age_seconds)

        if opts.dry_run:
            builder.add_flag(OptionTag.DRY_RUN)

        response = await self._client._send_and_check(
            OpCode.STREAM_TRIM,
            namespace,
            stream.encode("utf-8"),
            b"",
            builder.build(),
        )
        if len(response.data) != 16:
            raise ValueError(f"stream trim: response is {len(response.data)} bytes, want 16")
        removed, first_seq = struct.unpack("<QQ", response.data)
        return StreamTrimResult(removed=removed, first_seq=first_seq)

    async def delete(
        self,
        stream: str,
        options: StreamDeleteOptions | None = None,
    ) -> None:
        """Delete a stream's records and metadata.

        Deleting a stream that doesn't exist succeeds. A stream that still has
        records is refused with ``ConflictError`` unless ``force`` is set.

        Example:
            await client.stream.delete("events", StreamDeleteOptions(force=True))
        """
        opts = options or StreamDeleteOptions()
        namespace = self._client.get_namespace(opts.namespace)

        builder = OptionsBuilder()
        if opts.force:
            builder.add_flag(OptionTag.FORCE)

        await self._client._send_and_check(
            OpCode.STREAM_DELETE,
            namespace,
            stream.encode("utf-8"),
            b"",
            builder.build(),
        )

    async def group_create(
        self,
        stream: str,
        group: str,
        options: StreamGroupCreateOptions | None = None,
    ) -> bool:
        """Create a consumer group with its lease (``ack_timeout_ms``) and
        attempts (``max_deliver``), which can't change once it exists.

        A group read creates a group that doesn't exist yet at the server's
        defaults (a 30 s lease, 5 attempts); create it first for others.

        Returns:
            True if this made the group, False if it existed with the same
            settings.

        Raises:
            ConflictError: The group exists with other settings; the message
                names them.
        """
        opts = options or StreamGroupCreateOptions()
        builder = OptionsBuilder()
        if opts.ack_timeout_ms is not None:
            if not MIN_GROUP_LEASE_MS <= opts.ack_timeout_ms <= MAX_GROUP_LEASE_MS:
                raise ValidationError(
                    f"ack_timeout_ms must be {MIN_GROUP_LEASE_MS}..{MAX_GROUP_LEASE_MS}, "
                    f"got {opts.ack_timeout_ms}"
                )
            builder.add_u32(OptionTag.ACK_TIMEOUT_MS, opts.ack_timeout_ms)
        if opts.max_deliver is not None:
            if not 1 <= opts.max_deliver <= 255:
                raise ValidationError(f"max_deliver must be 1..255, got {opts.max_deliver}")
            builder.add_u8(OptionTag.MAX_DELIVER, opts.max_deliver)
        response = await self._client._send_and_check(
            OpCode.STREAM_GROUP_CREATE,
            self._client.get_namespace(opts.namespace),
            stream.encode("utf-8"),
            serialize_group_value(group),
            builder.build(),
        )
        if len(response.data) != 1:
            raise IncompleteResponseError(
                f"stream group create answer is {len(response.data)} bytes"
            )
        return response.data[0] == 1

    async def group_delete(self, stream: str, group: str, namespace: str | None = None) -> int:
        """Delete a consumer group and every record it holds. Returns how
        many records it dropped.

        Raises:
            NotFoundError: The stream has no such group.
        """
        response = await self._client._send_and_check(
            OpCode.STREAM_GROUP_DELETE,
            self._client.get_namespace(namespace),
            stream.encode("utf-8"),
            serialize_group_value(group),
        )
        if len(response.data) != 8:
            raise IncompleteResponseError(
                f"stream group delete answer is {len(response.data)} bytes"
            )
        return int(struct.unpack("<Q", response.data)[0])

    async def group_read(
        self,
        stream: str,
        group: str,
        consumer: str,
        options: StreamGroupReadOptions | None = None,
    ) -> GroupReadResult:
        """Lease up to ``count`` (1..100) of a group's records to ``consumer``.

        A read takes the group's ready records first (those whose lease ran
        out or that were nacked, once any delay passes), then the stream's
        records past the group's cursor. Each is leased until the group's
        lease runs out: ack it, nack it, or touch it to keep it. One whose
        lease runs out is read again with ``deliveries`` one higher, so
        delivery is at least once. A record nacked or expired ``max_deliver``
        times (5 by default) is dead and isn't read again.

        The first read of a group that doesn't exist creates it at the
        server's defaults (a 30 s lease, 5 attempts); see :meth:`group_create`.

        Example:
            got = await client.stream.group_read("events", "processors", "worker-1")
            for record in got.records:
                process(record.payload)
            await client.stream.group_ack("events", "processors", got)
        """
        opts = options or StreamGroupReadOptions()
        builder = OptionsBuilder()
        if opts.count is not None:
            if not 1 <= opts.count <= MAX_GROUP_BATCH:
                raise ValidationError(
                    f"a group read takes 1..{MAX_GROUP_BATCH} records, got {opts.count}"
                )
            builder.add_u32(OptionTag.COUNT, opts.count)
        if opts.block_ms is not None:
            builder.add_u32(OptionTag.BLOCK_MS, opts.block_ms)
        response = await self._client._send_and_check(
            OpCode.STREAM_GROUP_READ,
            self._client.get_namespace(opts.namespace),
            stream.encode("utf-8"),
            serialize_group_value(group, consumer),
            builder.build(),
        )
        # The server reads an empty consumer as "default", and so do reports.
        return parse_group_read_response(response.data, consumer)

    async def group_ack(
        self,
        stream: str,
        group: str,
        delivery: GroupReadResult,
        records: list[GroupRecord] | None = None,
        options: StreamGroupAckOptions | None = None,
    ) -> GroupReportResult:
        """Ack a group read's records, all of them unless ``records`` names
        some: they leave the group.

        A record the ack doesn't apply to comes back in ``refused`` with why;
        the call itself succeeds. ``GONE`` means done (acked already, or not in
        the group), so ``result.lost`` lists only the records that go to
        another delivery: their lease ran out, or another consumer holds them.
        Resending an ack is safe.
        """
        return await self._report(
            OpCode.STREAM_GROUP_ACK, "stream group ack", stream, group, delivery, records, options
        )

    async def group_nack(
        self,
        stream: str,
        group: str,
        delivery: GroupReadResult,
        records: list[GroupRecord] | None = None,
        options: StreamGroupNackOptions | None = None,
    ) -> GroupReportResult:
        """Nack a group read's records, all of them unless ``records`` names
        some: each is ready again after ``delay_ms``, or dead once nacked or
        expired ``max_deliver`` times (at once with ``dead``). Records it
        doesn't apply to come back in ``refused``."""
        opts = options or StreamGroupNackOptions()
        builder = OptionsBuilder()
        if not 0 <= opts.delay_ms <= MAX_GROUP_NACK_DELAY_MS:
            raise ValidationError(
                f"delay_ms must be 0..{MAX_GROUP_NACK_DELAY_MS}, got {opts.delay_ms}"
            )
        if opts.delay_ms:
            builder.add_u64(OptionTag.DELAY_MS, opts.delay_ms)
        if opts.dead:
            builder.add_flag(OptionTag.DEAD)
        if opts.error:
            error = opts.error.encode("utf-8")
            if len(error) > MAX_GROUP_NACK_ERROR:
                raise ValidationError(
                    f"a nack's error is at most {MAX_GROUP_NACK_ERROR} bytes, got {len(error)}"
                )
            builder.add_bytes(OptionTag.ERROR_TEXT, error)
        return await self._report(
            OpCode.STREAM_GROUP_NACK,
            "stream group nack",
            stream,
            group,
            delivery,
            records,
            opts,
            builder.build(),
        )

    async def group_touch(
        self,
        stream: str,
        group: str,
        delivery: GroupReadResult,
        records: list[GroupRecord] | None = None,
        options: StreamGroupAckOptions | None = None,
    ) -> GroupTouchResult:
        """Keep a group read's records: with half their lease or less left,
        it runs a full lease from now, up to 12 h from their read; with more
        left, nothing changes. Touch at least every half lease."""
        opts = options or StreamGroupAckOptions()
        response = await self._client._send_and_check(
            OpCode.STREAM_GROUP_TOUCH,
            self._client.get_namespace(opts.namespace),
            stream.encode("utf-8"),
            serialize_group_report_value(group, delivery, records),
        )
        return parse_group_touch_response(response.data)

    async def _report(
        self,
        op: OpCode,
        name: str,
        stream: str,
        group: str,
        delivery: GroupReadResult,
        records: list[GroupRecord] | None,
        options: StreamGroupAckOptions | StreamGroupNackOptions | None,
        tlv: bytes = b"",
    ) -> GroupReportResult:
        namespace = options.namespace if options is not None else None
        response = await self._client._send_and_check(
            op,
            self._client.get_namespace(namespace),
            stream.encode("utf-8"),
            serialize_group_report_value(group, delivery, records),
            tlv,
        )
        return parse_group_report_response(name, response.data)

    async def group_info(
        self, stream: str, group: str, namespace: str | None = None
    ) -> StreamGroupInfo:
        """A consumer group's counts: its records ready, leased and dead, and
        the consumers holding some.

        Raises:
            NotFoundError: The stream has no such group.
        """
        response = await self._client._send_and_check(
            OpCode.STREAM_GROUP_INFO,
            self._client.get_namespace(namespace),
            stream.encode("utf-8"),
            serialize_group_value(group),
        )
        return parse_group_info_response(response.data)

    async def group_pending(
        self,
        stream: str,
        group: str,
        consumer: str = "",
        namespace: str | None = None,
    ) -> list[PendingEntry]:
        """Up to 100 of a group's records leased now, ``consumer``'s alone
        when it is given.

        Raises:
            NotFoundError: The stream has no such group.
        """
        response = await self._client._send_and_check(
            OpCode.STREAM_GROUP_PENDING,
            self._client.get_namespace(namespace),
            stream.encode("utf-8"),
            serialize_group_value(group, consumer or None),
        )
        return parse_pending_entries(response.data)

    async def group_list(self, stream: str, namespace: str | None = None) -> StreamGroupListResult:
        """A stream's consumer groups: the consumers holding records, the
        records ready and leased, settings and the last record read."""
        response = await self._client._send_and_check(
            OpCode.STREAM_GROUP_LIST,
            self._client.get_namespace(namespace),
            stream.encode("utf-8"),
            b"",
        )
        return decode_group_list_answer(response.data)

    async def group_members(
        self, stream: str, group: str, namespace: str | None = None
    ) -> StreamGroupMembersResult:
        """A consumer group's members: the consumers holding records now, each
        with how many, when it first took one still held, and when it last
        took one. A consumer holding nothing isn't a member.

        Raises:
            NotFoundError: The stream has no such group.
        """
        response = await self._client._send_and_check(
            OpCode.STREAM_GROUP_MEMBERS,
            self._client.get_namespace(namespace),
            stream.encode("utf-8"),
            encode_group_members_body(group),
        )
        return decode_group_members_answer(response.data)
