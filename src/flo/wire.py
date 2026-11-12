"""Flo Wire Protocol

Binary serialization/deserialization for the Flo protocol.
Header: 32 bytes, little-endian, CRC32C validated.
"""

import json
import struct
from dataclasses import dataclass
from typing import Any

import google_crc32c

from .exceptions import (
    BlockTooLongError,
    IncompleteResponseError,
    InvalidChecksumError,
    InvalidMagicError,
    KeyTooLargeError,
    NamespaceTooLargeError,
    ProtocolError,
    TableMismatchError,
    ValidationError,
    ValueTooLargeError,
)
from .kv_body import is_table_op
from .types import (
    HEADER_SIZE,
    MAGIC,
    MAX_ANSWER_BYTES,
    MAX_BLOCK_MS,
    MAX_KEY_SIZE,
    MAX_NAMESPACE_SIZE,
    MAX_VALUE_SIZE,
    RESPONSE_STALE,
    TABLE_HASH,
    VERSION,
    GroupReadResult,
    GroupRecord,
    GroupRefusal,
    GroupReportResult,
    GroupTouchResult,
    OpCode,
    OptionTag,
    PendingEntry,
    QueueRefusalReason,
    StatusCode,
    StorageTier,
    StreamAppendResult,
    StreamGroupInfo,
    StreamID,
    StreamInfo,
    StreamReadResult,
    StreamRecord,
)

# Request header (32 bytes):
#   magic(u32) + payload_len(u32) + request_id(u64)
#   + crc32c(u32) + op_code(u16) + version(u8) + flags(u8) + table_hash(u64)
REQUEST_HEADER_FORMAT = "<IIQIHBBQ"

# Response header (32 bytes):
#   magic(u32) + data_len(u32) + request_id(u64)
#   + crc32c(u32) + version(u8) + status(u8) + flags(u8) + _pad(u8) + table_hash(u64)
RESPONSE_HEADER_FORMAT = "<IIQIBBBBQ"


# =============================================================================
# TLV Options
# =============================================================================


def _validate_block_ms(block_ms: int) -> None:
    """Refuse a block_ms the server would refuse or u32 cannot encode, before the round trip."""
    if not isinstance(block_ms, int) or isinstance(block_ms, bool):
        raise ValidationError(f"block_ms must be an int, got {block_ms!r}")
    if block_ms < 0:
        raise ValidationError(f"block_ms must not be negative, got {block_ms}")
    if block_ms > MAX_BLOCK_MS:
        raise BlockTooLongError(
            f"a blocking wait (block_ms) is at most {MAX_BLOCK_MS} ms (5 minutes), got {block_ms}"
        )


class OptionsBuilder:
    """Helper for building TLV-encoded options."""

    def __init__(self) -> None:
        self._buffer = bytearray()

    def add_u8(self, tag: OptionTag, value: int) -> "OptionsBuilder":
        """Add a u8 option."""
        self._buffer.extend(bytes([tag, 1, value & 0xFF]))
        return self

    def add_u32(self, tag: OptionTag, value: int) -> "OptionsBuilder":
        """Add a u32 option."""
        if tag == OptionTag.BLOCK_MS:
            _validate_block_ms(value)
        self._buffer.extend(bytes([tag, 4]))
        self._buffer.extend(struct.pack("<I", value))
        return self

    def add_u64(self, tag: OptionTag, value: int) -> "OptionsBuilder":
        """Add a u64 option."""
        self._buffer.extend(bytes([tag, 8]))
        self._buffer.extend(struct.pack("<Q", value))
        return self

    def add_bytes(self, tag: OptionTag, value: bytes) -> "OptionsBuilder":
        """Add a bytes option."""
        if len(value) > 255:
            raise ValueError("Option value too large (max 255 bytes)")
        self._buffer.extend(bytes([tag, len(value)]))
        self._buffer.extend(value)
        return self

    def add_flag(self, tag: OptionTag) -> "OptionsBuilder":
        """Add a flag option (presence indicates true)."""
        self._buffer.extend(bytes([tag, 0]))
        return self

    def build(self) -> bytes:
        """Get the built options as bytes."""
        return bytes(self._buffer)


@dataclass
class Option:
    """A single TLV option."""

    tag: OptionTag
    data: bytes

    def as_u8(self) -> int | None:
        """Get option value as u8."""
        if len(self.data) != 1:
            return None
        return self.data[0]

    def as_u32(self) -> int | None:
        """Get option value as u32."""
        if len(self.data) != 4:
            return None
        result: int = struct.unpack("<I", self.data)[0]
        return result

    def as_u64(self) -> int | None:
        """Get option value as u64."""
        if len(self.data) != 8:
            return None
        result: int = struct.unpack("<Q", self.data)[0]
        return result

    def as_string(self) -> str:
        """Get option value as UTF-8 string."""
        return self.data.decode("utf-8")

    def is_flag(self) -> bool:
        """Check if this is a flag option."""
        return len(self.data) == 0


class OptionsIterator:
    """Iterator for parsing TLV options."""

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._offset = 0

    def __iter__(self) -> "OptionsIterator":
        return self

    def __next__(self) -> Option:
        if self._offset + 2 > len(self._data):
            raise StopIteration

        tag = OptionTag(self._data[self._offset])
        length = self._data[self._offset + 1]

        if self._offset + 2 + length > len(self._data):
            raise StopIteration

        data = self._data[self._offset + 2 : self._offset + 2 + length]
        self._offset += 2 + length

        return Option(tag=tag, data=data)

    def find(self, tag: OptionTag) -> Option | None:
        """Find a specific option by tag."""
        self._offset = 0
        for opt in self:
            if opt.tag == tag:
                return opt
        return None


# =============================================================================
# Request Serialization
# =============================================================================


def crc32c(data: bytes, crc: int = 0) -> int:
    """CRC32C (Castagnoli); pass a previous result as ``crc`` to continue it."""
    return int(google_crc32c.extend(crc, data))


def compute_crc32c(header_bytes: bytes, payload: bytes) -> int:
    """A frame's checksum: CRC32C over header bytes 0-15 and 20-31 (all but
    the checksum field itself), then the payload."""
    return crc32c(payload, crc32c(header_bytes[20:32], crc32c(header_bytes[0:16])))


def serialize_request(
    request_id: int,
    op_code: OpCode,
    namespace: bytes,
    key: bytes,
    value: bytes,
    options: bytes = b"",
) -> bytes:
    """Serialize a request into wire format.

    Args:
        request_id: Unique request identifier.
        op_code: Operation code.
        namespace: Namespace bytes (UTF-8 encoded).
        key: Key bytes.
        value: Value bytes.
        options: TLV-encoded options.

    Returns:
        Serialized request bytes.

    Raises:
        NamespaceTooLargeError: If namespace exceeds 255 bytes.
        KeyTooLargeError: If key exceeds 64 KB.
        ValueTooLargeError: If value exceeds 16 MB.
    """
    # Validate sizes
    if len(namespace) > MAX_NAMESPACE_SIZE:
        raise NamespaceTooLargeError(
            f"Namespace too large: {len(namespace)} > {MAX_NAMESPACE_SIZE}"
        )
    if len(key) > MAX_KEY_SIZE:
        raise KeyTooLargeError(f"Key too large: {len(key)} > {MAX_KEY_SIZE}")
    if len(value) > MAX_VALUE_SIZE:
        raise ValueTooLargeError(f"Value too large: {len(value)} > {MAX_VALUE_SIZE}")

    # Build payload
    payload = bytearray()

    # Namespace: [len:u16][data]
    payload.extend(struct.pack("<H", len(namespace)))
    payload.extend(namespace)

    # Key: [len:u16][data]
    payload.extend(struct.pack("<H", len(key)))
    payload.extend(key)

    if is_table_op(op_code):
        # A table op's body, coded by its schema, is `value`; it takes no options.
        if options:
            raise ValidationError(f"op {op_code} takes a body, not options")
        payload.extend(value)
    else:
        # Value: [len:u32][data]
        payload.extend(struct.pack("<I", len(value)))
        payload.extend(value)

        # Options: [len:u16][data]
        payload.extend(struct.pack("<H", len(options)))
        payload.extend(options)

    payload_bytes = bytes(payload)

    header_without_crc = struct.pack(
        REQUEST_HEADER_FORMAT,
        MAGIC,
        len(payload_bytes),
        request_id,
        0,  # checksum placeholder
        op_code,
        VERSION,
        0,  # flags
        TABLE_HASH,
    )

    crc = compute_crc32c(header_without_crc, payload_bytes)
    header = struct.pack(
        REQUEST_HEADER_FORMAT,
        MAGIC,
        len(payload_bytes),
        request_id,
        crc,
        op_code,
        VERSION,
        0,  # flags
        TABLE_HASH,
    )

    return header + payload_bytes


# =============================================================================
# Response Parsing
# =============================================================================


@dataclass
class RawResponse:
    """Raw response from server."""

    status: StatusCode
    data: bytes
    request_id: int
    # The RESPONSE_STALE flag: a halted or read-blocked shard answered this
    # read from its state just before the entry it couldn't apply. Not an
    # error.
    stale: bool = False

    def is_ok(self) -> bool:
        """Check if response status is OK."""
        return self.status == StatusCode.OK

    def is_not_found(self) -> bool:
        """Check if response status is NOT_FOUND."""
        return self.status == StatusCode.NOT_FOUND


def parse_response_header(data: bytes) -> tuple[StatusCode, int, int, int, bool]:
    """Parse response header.

    Args:
        data: Raw response data (must be at least HEADER_SIZE bytes).

    Returns:
        Tuple of (status, data_len, request_id, crc32c, stale).

    Raises:
        IncompleteResponseError: If data is too short, or a flag bit is set
            that this SDK doesn't know.
        ProtocolError: If the answer says it is longer than a server sends.
        InvalidMagicError: If magic number is invalid.
        TableMismatchError: If the server was built from another protocol
            version or op table; its body can't be read.
    """
    if len(data) < HEADER_SIZE:
        raise IncompleteResponseError(f"Response too short: {len(data)} < {HEADER_SIZE}")

    magic, data_len, request_id, crc, version, status, flags, _pad, table_hash = struct.unpack(
        RESPONSE_HEADER_FORMAT, data[:HEADER_SIZE]
    )

    if magic != MAGIC:
        raise InvalidMagicError(f"Invalid magic: 0x{magic:08X} != 0x{MAGIC:08X}")

    if version != VERSION or table_hash != TABLE_HASH:
        raise TableMismatchError(version, table_hash)

    if data_len > MAX_ANSWER_BYTES - HEADER_SIZE:
        raise ProtocolError(
            f"Answer of {data_len} bytes is past the largest a server sends "
            f"({MAX_ANSWER_BYTES - HEADER_SIZE})"
        )

    if flags & ~RESPONSE_STALE:
        raise IncompleteResponseError(f"Response has unknown flags 0x{flags:02X}")

    return StatusCode(status), data_len, request_id, crc, bool(flags & RESPONSE_STALE)


def parse_response(data: bytes) -> RawResponse:
    """Parse complete response.

    Args:
        data: Raw response data including header and payload.

    Returns:
        RawResponse with status, data, and request_id.

    Raises:
        IncompleteResponseError: If response is incomplete.
        InvalidChecksumError: If the CRC32C check fails.
    """
    status, data_len, request_id, expected_crc, stale = parse_response_header(data)

    expected_size = HEADER_SIZE + data_len
    if len(data) < expected_size:
        raise IncompleteResponseError(f"Response incomplete: {len(data)} < {expected_size}")

    response_data = data[HEADER_SIZE : HEADER_SIZE + data_len]

    # Verify CRC32C
    computed_crc = compute_crc32c(data[:HEADER_SIZE], response_data)
    if computed_crc != expected_crc:
        raise InvalidChecksumError(f"CRC32C mismatch: 0x{computed_crc:08X} != 0x{expected_crc:08X}")

    return RawResponse(status=status, data=response_data, request_id=request_id, stale=stale)


# =============================================================================
# Response Data Parsing
# =============================================================================


def parse_enqueue_response(data: bytes) -> int:
    """Parse enqueue response data.

    Format: [seq:u64]
    """
    if len(data) < 8:
        raise IncompleteResponseError("Enqueue response too short")

    result: int = struct.unpack("<Q", data[:8])[0]
    return result


# =============================================================================
# Stream Response Parsing
# =============================================================================


def parse_stream_append_response(data: bytes) -> StreamAppendResult:
    """Parse stream append response data.

    Format: [sequence:u64][timestamp_ms:i64]
    """
    if len(data) < 16:
        raise IncompleteResponseError("Stream append response too short")

    sequence = struct.unpack("<Q", data[0:8])[0]
    timestamp_ms = struct.unpack("<q", data[8:16])[0]

    return StreamAppendResult(id=StreamID(timestamp_ms=timestamp_ms, sequence=sequence))


class _Reader:
    """Reads a little-endian answer field by field; one cut short raises."""

    def __init__(self, op: str, data: bytes) -> None:
        self.op = op
        self.data = data
        self.pos = 0

    def take(self, n: int, what: str) -> bytes:
        if self.pos + n > len(self.data):
            raise IncompleteResponseError(f"{self.op}: missing {what}")
        b = self.data[self.pos : self.pos + n]
        self.pos += n
        return b

    def unpack(self, fmt: str, what: str) -> tuple[Any, ...]:
        return struct.unpack(fmt, self.take(struct.calcsize(fmt), what))

    def u32(self, what: str) -> int:
        return int(self.unpack("<I", what)[0])

    def done(self) -> None:
        if self.pos != len(self.data):
            raise IncompleteResponseError(f"{self.op}: {len(self.data) - self.pos} bytes left over")


def _stream_record(r: _Reader) -> StreamRecord:
    """One record as a stream read gives it: [sequence:u64][timestamp_ms:i64]
    [tier:u8][partition:u32][key_present:u8]([key_len:u32][key])?
    [payload_len:u32][payload][header_count:u32]([len:u32][k][len:u32][v])*."""
    sequence, timestamp_ms, tier, _partition, key_present = r.unpack("<QqBIB", "record header")
    stream_name = ""
    if key_present != 0:
        stream_name = r.take(r.u32("key length"), "key").decode("utf-8")
    payload = r.take(r.u32("payload length"), "payload")
    header_count = r.u32("header count")
    headers = None
    if header_count > 0:
        headers = {}
        for _ in range(header_count):
            hkey = r.take(r.u32("header key length"), "header key").decode("utf-8")
            headers[hkey] = r.take(r.u32("header value length"), "header value").decode("utf-8")
    return StreamRecord(
        id=StreamID(timestamp_ms=timestamp_ms, sequence=sequence),
        tier=StorageTier(tier),
        stream=stream_name,
        payload=payload,
        headers=headers,
    )


def parse_stream_read_response(data: bytes) -> StreamReadResult:
    """Parse stream read response data: [count:u32] then each record.

    An answer that ends before its count of records is refused, not returned
    short.
    """
    if len(data) < 4:
        # Empty response is valid (no records)
        return StreamReadResult(records=[])
    r = _Reader("stream read", data)
    count = r.u32("count")
    return StreamReadResult(records=[_stream_record(r) for _ in range(count)])


def parse_stream_info_response(data: bytes) -> StreamInfo:
    """Parse stream info response data.

    Format: [first_ts:u64][first_seq:u64][last_ts:u64][last_seq:u64]
            [count:u64][bytes:u64][partition_count:u32]
    """
    if len(data) < 52:
        raise IncompleteResponseError("Stream info response too short")

    first_ts = struct.unpack("<Q", data[0:8])[0]
    first_seq = struct.unpack("<Q", data[8:16])[0]
    last_ts = struct.unpack("<Q", data[16:24])[0]
    last_seq = struct.unpack("<Q", data[24:32])[0]
    count = struct.unpack("<Q", data[32:40])[0]
    bytes_size = struct.unpack("<Q", data[40:48])[0]
    partition_count = struct.unpack("<I", data[48:52])[0]

    return StreamInfo(
        first_id=StreamID(timestamp_ms=first_ts, sequence=first_seq),
        last_id=StreamID(timestamp_ms=last_ts, sequence=last_seq),
        count=count,
        bytes_size=bytes_size,
        partition_count=partition_count,
    )


def build_stream_batch_value(
    payload: bytes,
    headers: dict[str, str] | None = None,
) -> bytes:
    """Frame a single record for stream_append (batch of one).

    Wire format:
    [record_count:u32=1][payload_len:u32][payload]
    [header_count:u16]([key_len:u16][key][val_len:u16][val])*
    """
    hdrs = headers or {}
    parts: list[bytes] = [
        struct.pack("<I", 1),
        struct.pack("<I", len(payload)),
        payload,
        struct.pack("<H", len(hdrs)),
    ]
    for key, val in hdrs.items():
        key_bytes = key.encode("utf-8")
        val_bytes = val.encode("utf-8")
        parts.append(struct.pack("<H", len(key_bytes)))
        parts.append(key_bytes)
        parts.append(struct.pack("<H", len(val_bytes)))
        parts.append(val_bytes)
    return b"".join(parts)


# The longest group name and consumer name the server takes.
MAX_GROUP_NAME = 256
MAX_GROUP_CONSUMER = 4096
# The most records a group read takes, and a report names.
MAX_GROUP_BATCH = 100
# A group's lease (ack timeout) bounds, and a nack's longest delay.
MIN_GROUP_LEASE_MS = 5_000
MAX_GROUP_LEASE_MS = 43_200_000
MAX_GROUP_NACK_DELAY_MS = 86_400_000
# A nack's error travels as an option, whose length is one byte.
MAX_GROUP_NACK_ERROR = 255


def _str16(what: str, s: str, most: int) -> bytes:
    b = s.encode("utf-8")
    if len(b) > most:
        raise ValidationError(f"{what} is {len(b)} bytes, at most {most}")
    return struct.pack("<H", len(b)) + b


def serialize_group_value(group: str, consumer: str | None = None) -> bytes:
    """A group op's value: [group_len:u16][group], then the consumer's
    [len:u16][consumer] when one is given."""
    if not group:
        raise ValidationError("a group name is required")
    out = _str16("group name", group, MAX_GROUP_NAME)
    if consumer is not None:
        out += _str16("consumer", consumer, MAX_GROUP_CONSUMER)
    return out


def serialize_group_report_value(
    group: str, delivery: GroupReadResult, records: list[GroupRecord] | None
) -> bytes:
    """An ack's, nack's or touch's value: the group, the read's consumer, its
    delivery (term, index), then each record's id and deliveries; all the
    read's records when ``records`` is None."""
    picked = delivery.records if records is None else records
    if not 1 <= len(picked) <= MAX_GROUP_BATCH:
        raise ValidationError(f"a report names 1..{MAX_GROUP_BATCH} records, got {len(picked)}")
    return (
        serialize_group_value(group, delivery.consumer)
        + struct.pack("<QQI", delivery.term, delivery.index, len(picked))
        + b"".join(
            struct.pack("<QQI", r.id.timestamp_ms, r.id.sequence, r.deliveries) for r in picked
        )
    )


def parse_group_read_response(data: bytes, consumer: str) -> GroupReadResult:
    """A group read's answer: [term:u64][index:u64][expires_at_ms:i64]
    [count:u32], then each record as a stream read gives it followed by its
    [deliveries:u32]."""
    r = _Reader("stream group read", data)
    term, index, expires_at_ms, count = r.unpack("<QQqI", "delivery")
    if count > MAX_GROUP_BATCH:
        raise IncompleteResponseError(f"stream group read answer has {count} records")
    records = []
    for _ in range(count):
        rec = _stream_record(r)
        records.append(
            GroupRecord(
                id=rec.id,
                tier=rec.tier,
                stream=rec.stream,
                payload=rec.payload,
                headers=rec.headers,
                deliveries=r.u32("deliveries"),
            )
        )
    r.done()
    return GroupReadResult(
        records=records, term=term, index=index, expires_at_ms=expires_at_ms, consumer=consumer
    )


def _group_refusals(r: _Reader) -> list[GroupRefusal]:
    count = r.u32("refused count")
    if count > MAX_GROUP_BATCH:
        raise IncompleteResponseError(f"{r.op} answer refuses {count} records")
    refused = []
    for _ in range(count):
        ms, seq, code = r.unpack("<QQB", "refused record")
        try:
            why = QueueRefusalReason(code)
        except ValueError:
            raise IncompleteResponseError(f"{r.op} answer has an unknown refusal {code}") from None
        refused.append(GroupRefusal(id=StreamID(timestamp_ms=ms, sequence=seq), why=why))
    r.done()
    return refused


def parse_group_report_response(op: str, data: bytes) -> GroupReportResult:
    """An ack's or nack's answer: [applied:u32][refused:u32] then each refused
    record's [ms:u64][seq:u64][why:u8]."""
    r = _Reader(op, data)
    applied = r.u32("applied")
    return GroupReportResult(applied=applied, refused=_group_refusals(r))


def parse_group_touch_response(data: bytes) -> GroupTouchResult:
    """A touch's answer: [expires_at_ms:i64], then as an ack's."""
    r = _Reader("stream group touch", data)
    (expires_at_ms,) = r.unpack("<q", "expires_at_ms")
    applied = r.u32("applied")
    return GroupTouchResult(
        expires_at_ms=expires_at_ms, applied=applied, refused=_group_refusals(r)
    )


def parse_group_info_response(data: bytes) -> StreamGroupInfo:
    """A group info's answer: [pending:u64][holders:u32][created_at_ns:u64]
    [ready:u64][leased:u64][dead:u64][trimmed_pending:u64]."""
    r = _Reader("stream group info", data)
    pending, holders, created_at_ns, ready, leased, dead, trimmed = r.unpack("<QIQQQQQ", "counts")
    r.done()
    return StreamGroupInfo(
        pending=pending,
        holders=holders,
        created_at_ns=created_at_ns,
        ready=ready,
        leased=leased,
        dead=dead,
        trimmed_pending=trimmed,
    )


def parse_pending_entries(data: bytes) -> list[PendingEntry]:
    """A group pending answer: [count:u32]([ts:u64][seq:u64]
    [delivery_count:u32][consumer_len:u16][consumer])*."""
    r = _Reader("stream group pending", data)
    count = r.u32("count")
    entries: list[PendingEntry] = []
    for _ in range(count):
        ts, seq, delivery_count, clen = r.unpack("<QQIH", "pending entry")
        consumer = r.take(clen, "consumer").decode("utf-8")
        entries.append(
            PendingEntry(
                id=StreamID(timestamp_ms=ts, sequence=seq),
                consumer=consumer,
                delivery_count=delivery_count,
            )
        )
    r.done()
    return entries


# =============================================================================
# Action/Worker Value Serialization
# =============================================================================


def serialize_action_register_value(
    action_type: int,
    timeout_ms: int,
    max_attempts: int,
    owner: str | None = None,
) -> bytes:
    """Serialize action register value.

    Format: [action_type:u8][timeout_ms:u32][max_attempts:u32][owner_len:u16][owner]
    """
    for name, value in (("timeout_ms", timeout_ms), ("max_attempts", max_attempts)):
        if not 0 <= value < 2**32:
            raise ValueError(f"{name} must be in [0, 2**32), not {value}")
    owner_bytes = (owner or "").encode("utf-8")
    if len(owner_bytes) > 0xFFFF:
        raise ValueError("owner is longer than 65535 bytes")
    return (
        struct.pack("<BIIH", action_type & 0xFF, timeout_ms, max_attempts, len(owner_bytes))
        + owner_bytes
    )


def serialize_action_invoke_value(
    input_data: bytes,
    labels: dict[str, Any] | None = None,
) -> bytes:
    """Serialize action invoke value.

    Format: [has_labels:u8]([labels_len:u16][labels...])?[input...]

    has_labels is 0 or 1. labels is a compact JSON object; only workers whose
    registered labels contain every key/value receive the run.

    Raises:
        ValueError: If the encoded labels exceed 65535 bytes.
    """
    if labels is None:
        return b"\x00" + input_data

    labels_bytes = json.dumps(labels, separators=(",", ":")).encode("utf-8")
    if len(labels_bytes) > 0xFFFF:
        raise ValueError(f"invoke labels are {len(labels_bytes)} bytes; the limit is 65535")
    return b"\x01" + struct.pack("<H", len(labels_bytes)) + labels_bytes + input_data


def serialize_action_list_value(limit: int = 100, cursor: bytes | None = None) -> bytes:
    """Serialize action list value.

    Format: [limit:u32][cursor...] (cursor omitted on the first page; limit 0
    is the server's default)
    """
    return struct.pack("<I", limit) + (cursor or b"")


def serialize_worker_register_value(
    task_types: list[str],
    *,
    worker_type: int = 0,
    max_concurrency: int = 10,
    metadata: str | None = None,
    machine_id: str | None = None,
) -> bytes:
    """Serialize worker register value.

    Server format:
      [type:u8][max_concurrency:u32][process_count:u16]
      ([name_len:u16][name][kind:u8])*
      [has_metadata:u8]([metadata_len:u16][metadata])?
      [has_machine_id:u8]([machine_id_len:u16][machine_id])?
    """
    result = bytearray()

    # worker type: 0=action, 1=stream
    result.append(worker_type & 0xFF)

    # max_concurrency
    result.extend(struct.pack("<I", max_concurrency))

    # process_count + process entries
    result.extend(struct.pack("<H", len(task_types)))
    for tt in task_types:
        tt_bytes = tt.encode("utf-8")
        result.extend(struct.pack("<H", len(tt_bytes)))
        result.extend(tt_bytes)
        result.append(worker_type & 0xFF)  # kind matches worker type

    # optional metadata
    if metadata:
        result.append(1)
        meta_bytes = metadata.encode("utf-8")
        result.extend(struct.pack("<H", len(meta_bytes)))
        result.extend(meta_bytes)
    else:
        result.append(0)

    # optional machine_id
    if machine_id:
        result.append(1)
        mid_bytes = machine_id.encode("utf-8")
        result.extend(struct.pack("<H", len(mid_bytes)))
        result.extend(mid_bytes)
    else:
        result.append(0)

    return bytes(result)
