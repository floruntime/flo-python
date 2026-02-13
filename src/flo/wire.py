"""Flo Wire Protocol

Binary serialization/deserialization for the Flo protocol.
Header: 24 bytes, little-endian, CRC32 validated.
"""

import struct
from binascii import crc32
from dataclasses import dataclass
from typing import Optional

from .exceptions import (
    IncompleteResponseError,
    InvalidChecksumError,
    InvalidMagicError,
    KeyTooLargeError,
    NamespaceTooLargeError,
    UnsupportedVersionError,
    ValueTooLargeError,
)
from .types import (
    HEADER_SIZE,
    MAGIC,
    MAX_KEY_SIZE,
    MAX_NAMESPACE_SIZE,
    MAX_VALUE_SIZE,
    VERSION,
    DequeueResult,
    KVEntry,
    Message,
    OpCode,
    OptionTag,
    ScanResult,
    StatusCode,
    StorageTier,
    StreamAppendResult,
    StreamInfo,
    StreamReadResult,
    StreamRecord,
    VersionEntry,
)

# Header format: magic(u32) + payload_len(u32) + request_id(u64) + crc32(u32) + version(u8) + op_code/status(u8) + flags(u8) + reserved(u8)
REQUEST_HEADER_FORMAT = "<IIQIBBBB"
RESPONSE_HEADER_FORMAT = "<IIQIBBBB"


# =============================================================================
# TLV Options
# =============================================================================


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

    def as_u8(self) -> Optional[int]:
        """Get option value as u8."""
        if len(self.data) != 1:
            return None
        return self.data[0]

    def as_u32(self) -> Optional[int]:
        """Get option value as u32."""
        if len(self.data) != 4:
            return None
        return struct.unpack("<I", self.data)[0]

    def as_u64(self) -> Optional[int]:
        """Get option value as u64."""
        if len(self.data) != 8:
            return None
        return struct.unpack("<Q", self.data)[0]

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

    def find(self, tag: OptionTag) -> Optional[Option]:
        """Find a specific option by tag."""
        self._offset = 0
        for opt in self:
            if opt.tag == tag:
                return opt
        return None


# =============================================================================
# Request Serialization
# =============================================================================


def compute_crc32(header_bytes: bytes, payload: bytes) -> int:
    """Compute CRC32 for header (excluding crc32 field) + payload.

    The CRC32 is computed over:
    - Header bytes 0-15 (magic, payload_length, request_id)
    - Header bytes 20-23 (version, op_code/status, flags, reserved)
    - Payload bytes
    """
    # Hash bytes 0-15 and 20-23 of header (skip crc32 field at 16-19)
    crc_data = header_bytes[0:16] + header_bytes[20:24] + payload
    return crc32(crc_data) & 0xFFFFFFFF


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
        raise NamespaceTooLargeError(f"Namespace too large: {len(namespace)} > {MAX_NAMESPACE_SIZE}")
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

    # Value: [len:u32][data]
    payload.extend(struct.pack("<I", len(value)))
    payload.extend(value)

    # Options: [len:u16][data]
    payload.extend(struct.pack("<H", len(options)))
    payload.extend(options)

    payload_bytes = bytes(payload)

    # Build header without CRC32
    header_without_crc = struct.pack(
        REQUEST_HEADER_FORMAT,
        MAGIC,
        len(payload_bytes),
        request_id,
        0,  # CRC32 placeholder
        VERSION,
        op_code,
        0,  # flags
        0,  # reserved
    )

    # Compute CRC32
    crc = compute_crc32(header_without_crc, payload_bytes)

    # Build final header with CRC32
    header = struct.pack(
        REQUEST_HEADER_FORMAT,
        MAGIC,
        len(payload_bytes),
        request_id,
        crc,
        VERSION,
        op_code,
        0,  # flags
        0,  # reserved
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

    def is_ok(self) -> bool:
        """Check if response status is OK."""
        return self.status == StatusCode.OK

    def is_not_found(self) -> bool:
        """Check if response status is NOT_FOUND."""
        return self.status == StatusCode.NOT_FOUND


def parse_response_header(data: bytes) -> tuple[StatusCode, int, int, int]:
    """Parse response header.

    Args:
        data: Raw response data (must be at least HEADER_SIZE bytes).

    Returns:
        Tuple of (status, data_len, request_id, crc32).

    Raises:
        IncompleteResponseError: If data is too short.
        InvalidMagicError: If magic number is invalid.
        UnsupportedVersionError: If protocol version is unsupported.
        InvalidChecksumError: If CRC32 validation fails.
    """
    if len(data) < HEADER_SIZE:
        raise IncompleteResponseError(f"Response too short: {len(data)} < {HEADER_SIZE}")

    magic, data_len, request_id, crc, version, status, flags, reserved = struct.unpack(
        RESPONSE_HEADER_FORMAT, data[:HEADER_SIZE]
    )

    if magic != MAGIC:
        raise InvalidMagicError(f"Invalid magic: 0x{magic:08X} != 0x{MAGIC:08X}")

    if version != VERSION:
        raise UnsupportedVersionError(f"Unsupported version: {version} != {VERSION}")

    return StatusCode(status), data_len, request_id, crc


def parse_response(data: bytes) -> RawResponse:
    """Parse complete response.

    Args:
        data: Raw response data including header and payload.

    Returns:
        RawResponse with status, data, and request_id.

    Raises:
        IncompleteResponseError: If response is incomplete.
        InvalidChecksumError: If CRC32 validation fails.
    """
    status, data_len, request_id, expected_crc = parse_response_header(data)

    expected_size = HEADER_SIZE + data_len
    if len(data) < expected_size:
        raise IncompleteResponseError(f"Response incomplete: {len(data)} < {expected_size}")

    response_data = data[HEADER_SIZE : HEADER_SIZE + data_len]

    # Verify CRC32
    computed_crc = compute_crc32(data[:HEADER_SIZE], response_data)
    if computed_crc != expected_crc:
        raise InvalidChecksumError(f"CRC32 mismatch: 0x{computed_crc:08X} != 0x{expected_crc:08X}")

    return RawResponse(status=status, data=response_data, request_id=request_id)


# =============================================================================
# Response Data Parsing
# =============================================================================


def parse_scan_response(data: bytes) -> ScanResult:
    """Parse scan response data.

    Format: [has_more:u8][cursor_len:u32][cursor:bytes][count:u32][entries...]
    Entry format: [key_len:u16][key][value_len:u32][value]
    """
    if len(data) < 9:
        raise IncompleteResponseError("Scan response too short")

    offset = 0

    # has_more
    has_more = data[offset] != 0
    offset += 1

    # cursor
    cursor_len = struct.unpack("<I", data[offset : offset + 4])[0]
    offset += 4

    if len(data) < offset + cursor_len:
        raise IncompleteResponseError("Scan response cursor incomplete")

    cursor = data[offset : offset + cursor_len] if cursor_len > 0 else None
    offset += cursor_len

    # count
    if len(data) < offset + 4:
        raise IncompleteResponseError("Scan response count incomplete")

    count = struct.unpack("<I", data[offset : offset + 4])[0]
    offset += 4

    # entries
    entries: list[KVEntry] = []
    for _ in range(count):
        # key
        if len(data) < offset + 2:
            raise IncompleteResponseError("Scan response key length incomplete")

        key_len = struct.unpack("<H", data[offset : offset + 2])[0]
        offset += 2

        if len(data) < offset + key_len:
            raise IncompleteResponseError("Scan response key incomplete")

        key = data[offset : offset + key_len]
        offset += key_len

        # value
        if len(data) < offset + 4:
            raise IncompleteResponseError("Scan response value length incomplete")

        value_len = struct.unpack("<I", data[offset : offset + 4])[0]
        offset += 4

        value: Optional[bytes] = None
        if value_len > 0:
            if len(data) < offset + value_len:
                raise IncompleteResponseError("Scan response value incomplete")
            value = data[offset : offset + value_len]
            offset += value_len

        entries.append(KVEntry(key=key, value=value))

    return ScanResult(entries=entries, cursor=cursor, has_more=has_more)


def parse_history_response(data: bytes) -> list[VersionEntry]:
    """Parse history response data.

    Format: [count:u32][entries...]
    Entry format: [version:u64][timestamp:i64][value_len:u32][value]
    """
    if len(data) < 4:
        raise IncompleteResponseError("History response too short")

    offset = 0

    count = struct.unpack("<I", data[offset : offset + 4])[0]
    offset += 4

    entries: list[VersionEntry] = []
    for _ in range(count):
        if len(data) < offset + 20:
            raise IncompleteResponseError("History response entry incomplete")

        version = struct.unpack("<Q", data[offset : offset + 8])[0]
        offset += 8

        timestamp = struct.unpack("<q", data[offset : offset + 8])[0]
        offset += 8

        value_len = struct.unpack("<I", data[offset : offset + 4])[0]
        offset += 4

        if len(data) < offset + value_len:
            raise IncompleteResponseError("History response value incomplete")

        value = data[offset : offset + value_len]
        offset += value_len

        entries.append(VersionEntry(version=version, timestamp=timestamp, value=value))

    return entries


def parse_dequeue_response(data: bytes) -> DequeueResult:
    """Parse dequeue response data.

    Format: [count:u32][messages...]
    Message format: [seq:u64][payload_len:u32][payload]
    """
    if len(data) < 4:
        raise IncompleteResponseError("Dequeue response too short")

    offset = 0

    count = struct.unpack("<I", data[offset : offset + 4])[0]
    offset += 4

    messages: list[Message] = []
    for _ in range(count):
        if len(data) < offset + 12:
            raise IncompleteResponseError("Dequeue response message incomplete")

        seq = struct.unpack("<Q", data[offset : offset + 8])[0]
        offset += 8

        payload_len = struct.unpack("<I", data[offset : offset + 4])[0]
        offset += 4

        if len(data) < offset + payload_len:
            raise IncompleteResponseError("Dequeue response payload incomplete")

        payload = data[offset : offset + payload_len]
        offset += payload_len

        messages.append(Message(seq=seq, payload=payload))

    return DequeueResult(messages=messages)


def parse_enqueue_response(data: bytes) -> int:
    """Parse enqueue response data.

    Format: [seq:u64]
    """
    if len(data) < 8:
        raise IncompleteResponseError("Enqueue response too short")

    return struct.unpack("<Q", data[:8])[0]


def serialize_seqs(seqs: list[int]) -> bytes:
    """Serialize sequence numbers for ack/nack.

    Format: [count:u32][seq:u64]*
    """
    result = bytearray()
    result.extend(struct.pack("<I", len(seqs)))
    for seq in seqs:
        result.extend(struct.pack("<Q", seq))
    return bytes(result)


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

    return StreamAppendResult(sequence=sequence, timestamp_ms=timestamp_ms)


def parse_stream_read_response(data: bytes) -> StreamReadResult:
    """Parse stream read response data.

    Wire format: [count:u32]([sequence:u64][timestamp_ms:i64][tier:u8][partition:u32]
                 [key_present:u8][key_len:u32]?[key]?[payload_len:u32][payload]
                 [header_count:u32])*
    """
    if len(data) < 4:
        # Empty response is valid (no records)
        return StreamReadResult(records=[], next_offset=0)

    pos = 0

    count = struct.unpack("<I", data[pos : pos + 4])[0]
    pos += 4

    records: list[StreamRecord] = []
    for _ in range(count):
        if pos >= len(data):
            break

        # Read sequence
        if pos + 8 > len(data):
            raise IncompleteResponseError("Stream record: missing sequence")
        sequence = struct.unpack("<Q", data[pos : pos + 8])[0]
        pos += 8

        # Read timestamp_ms
        if pos + 8 > len(data):
            raise IncompleteResponseError("Stream record: missing timestamp_ms")
        timestamp_ms = struct.unpack("<q", data[pos : pos + 8])[0]
        pos += 8

        # Read tier
        if pos + 1 > len(data):
            raise IncompleteResponseError("Stream record: missing tier")
        tier = StorageTier(data[pos])
        pos += 1

        # Skip partition
        if pos + 4 > len(data):
            raise IncompleteResponseError("Stream record: missing partition")
        pos += 4

        # Read key_present
        if pos + 1 > len(data):
            raise IncompleteResponseError("Stream record: missing key_present")
        key_present = data[pos]
        pos += 1

        # Skip key if present
        if key_present != 0:
            if pos + 4 > len(data):
                raise IncompleteResponseError("Stream record: missing key length")
            key_len = struct.unpack("<I", data[pos : pos + 4])[0]
            pos += 4
            if pos + key_len > len(data):
                raise IncompleteResponseError("Stream record: missing key data")
            pos += key_len

        # Read payload
        if pos + 4 > len(data):
            raise IncompleteResponseError("Stream record: missing payload length")
        payload_len = struct.unpack("<I", data[pos : pos + 4])[0]
        pos += 4

        if pos + payload_len > len(data):
            raise IncompleteResponseError("Stream record: missing payload data")
        payload = data[pos : pos + payload_len]
        pos += payload_len

        # Skip header_count (TODO: parse headers)
        if pos + 4 > len(data):
            raise IncompleteResponseError("Stream record: missing header count")
        pos += 4

        records.append(StreamRecord(
            sequence=sequence,
            timestamp_ms=timestamp_ms,
            tier=tier,
            payload=payload,
            headers=None,
        ))

    return StreamReadResult(records=records)


def parse_stream_info_response(data: bytes) -> StreamInfo:
    """Parse stream info response data.

    Format: [first_seq:u64][last_seq:u64][count:u64][bytes:u64]
    """
    if len(data) < 32:
        raise IncompleteResponseError("Stream info response too short")

    first_seq = struct.unpack("<Q", data[0:8])[0]
    last_seq = struct.unpack("<Q", data[8:16])[0]
    count = struct.unpack("<Q", data[16:24])[0]
    bytes_size = struct.unpack("<Q", data[24:32])[0]

    return StreamInfo(first_seq=first_seq, last_seq=last_seq, count=count, bytes_size=bytes_size)


def serialize_group_value(group: str, consumer: str) -> bytes:
    """Serialize group and consumer names for consumer group operations.

    Format: [group_len:u16][group][consumer_len:u16][consumer]
    """
    group_bytes = group.encode("utf-8")
    consumer_bytes = consumer.encode("utf-8")

    result = bytearray()
    result.extend(struct.pack("<H", len(group_bytes)))
    result.extend(group_bytes)
    result.extend(struct.pack("<H", len(consumer_bytes)))
    result.extend(consumer_bytes)
    return bytes(result)


def serialize_group_ack_value(group: str, seqs: list[int]) -> bytes:
    """Serialize group name and sequence numbers for group ack.

    Format: [group_len:u16][group][count:u32][seq:u64]*
    """
    group_bytes = group.encode("utf-8")

    result = bytearray()
    result.extend(struct.pack("<H", len(group_bytes)))
    result.extend(group_bytes)
    result.extend(struct.pack("<I", len(seqs)))
    for seq in seqs:
        result.extend(struct.pack("<Q", seq))
    return bytes(result)


# =============================================================================
# Action/Worker Value Serialization
# =============================================================================


def serialize_action_register_value(
    action_type: int,
    timeout_ms: int,
    max_retries: int,
    description: Optional[str] = None,
) -> bytes:
    """Serialize action register value.

    Format: [action_type:u8][timeout_ms:u32][max_retries:u32]
            [has_desc:u8][desc_len:u16]?[desc]?
            [has_wasm_module:u8]...(all optional fields as u8=0)
    """
    result = bytearray()

    # action_type
    result.append(action_type & 0xFF)

    # timeout_ms
    result.extend(struct.pack("<I", timeout_ms))

    # max_retries
    result.extend(struct.pack("<I", max_retries))

    # description (optional)
    if description:
        desc_bytes = description.encode("utf-8")
        result.append(1)  # has_desc
        result.extend(struct.pack("<H", len(desc_bytes)))
        result.extend(desc_bytes)
    else:
        result.append(0)

    # wasm_module (optional, not used)
    result.append(0)

    # wasm_entrypoint (optional, not used)
    result.append(0)

    # wasm_memory_limit (optional, not used)
    result.append(0)

    # trigger_stream (optional, not used)
    result.append(0)

    # trigger_group (optional, not used)
    result.append(0)

    return bytes(result)


def serialize_action_invoke_value(
    input_data: bytes,
    priority: int = 10,
    idempotency_key: Optional[str] = None,
) -> bytes:
    """Serialize action invoke value.

    Format: [priority:u8][delay_ms:i64][has_caller:u8]
            [has_idempotency_key:u8][key_len:u16]?[key]?[input...]
    """
    result = bytearray()

    # priority
    result.append(priority & 0xFF)

    # delay_ms (default 0)
    result.extend(struct.pack("<q", 0))

    # caller_id (optional, none)
    result.append(0)

    # idempotency_key (optional)
    if idempotency_key:
        key_bytes = idempotency_key.encode("utf-8")
        result.append(1)
        result.extend(struct.pack("<H", len(key_bytes)))
        result.extend(key_bytes)
    else:
        result.append(0)

    # input
    result.extend(input_data)

    return bytes(result)


def serialize_action_list_value(limit: int = 100) -> bytes:
    """Serialize action list value.

    Format: [limit:u32][cursor...] (cursor omitted if empty)
    """
    return struct.pack("<I", limit)


def serialize_worker_register_value(task_types: list[str]) -> bytes:
    """Serialize worker register value.

    Format: [count:u32][task_type_len:u16][task_type]...[has_caps:u8][caps]?
    """
    result = bytearray()

    # count
    result.extend(struct.pack("<I", len(task_types)))

    # task types
    for tt in task_types:
        tt_bytes = tt.encode("utf-8")
        result.extend(struct.pack("<H", len(tt_bytes)))
        result.extend(tt_bytes)

    # capabilities (optional, none)
    result.append(0)

    return bytes(result)


def serialize_worker_await_value(task_types: list[str]) -> bytes:
    """Serialize worker await value.

    Format: [count:u32][task_type_len:u16][task_type]...
    """
    result = bytearray()

    # count
    result.extend(struct.pack("<I", len(task_types)))

    # task types
    for tt in task_types:
        tt_bytes = tt.encode("utf-8")
        result.extend(struct.pack("<H", len(tt_bytes)))
        result.extend(tt_bytes)

    return bytes(result)


def serialize_worker_touch_value(task_id: str, extend_ms: int = 30000) -> bytes:
    """Serialize worker touch value.

    Format: [task_id_len:u16][task_id][extend_ms:u32]
    """
    task_id_bytes = task_id.encode("utf-8")

    result = bytearray()
    result.extend(struct.pack("<H", len(task_id_bytes)))
    result.extend(task_id_bytes)
    result.extend(struct.pack("<I", extend_ms))

    return bytes(result)


def serialize_worker_complete_value(task_id: str, result_data: bytes) -> bytes:
    """Serialize worker complete value.

    Format: [task_id_len:u16][task_id][result...]
    """
    task_id_bytes = task_id.encode("utf-8")

    result = bytearray()
    result.extend(struct.pack("<H", len(task_id_bytes)))
    result.extend(task_id_bytes)
    result.extend(result_data)

    return bytes(result)


def serialize_worker_fail_value(task_id: str, error_message: str) -> bytes:
    """Serialize worker fail value.

    Format: [task_id_len:u16][task_id][error_message...]
    Note: retry flag is handled via TLV options, not in payload (matches Go SDK).
    """
    task_id_bytes = task_id.encode("utf-8")
    error_bytes = error_message.encode("utf-8")

    result = bytearray()
    result.extend(struct.pack("<H", len(task_id_bytes)))
    result.extend(task_id_bytes)
    result.extend(error_bytes)

    return bytes(result)


def serialize_worker_list_value(limit: int = 100) -> bytes:
    """Serialize worker list value.

    Format: [limit:u32]
    """
    return struct.pack("<I", limit)


def parse_task_assignment(data: bytes) -> "TaskAssignment":
    """Parse task assignment from server response.

    Format: [task_id_len:u16][task_id][task_type_len:u16][task_type]
            [created_at:i64][attempt:u32][payload...]
    """
    from .types import TaskAssignment

    if len(data) < 10:
        raise ValueError("Incomplete task assignment response")

    pos = 0

    # task_id
    task_id_len = struct.unpack_from("<H", data, pos)[0]
    pos += 2
    if pos + task_id_len > len(data):
        raise ValueError("Incomplete task assignment: missing task_id")
    task_id = data[pos : pos + task_id_len].decode("utf-8")
    pos += task_id_len

    # task_type
    if pos + 2 > len(data):
        raise ValueError("Incomplete task assignment: missing task_type length")
    task_type_len = struct.unpack_from("<H", data, pos)[0]
    pos += 2
    if pos + task_type_len > len(data):
        raise ValueError("Incomplete task assignment: missing task_type")
    task_type = data[pos : pos + task_type_len].decode("utf-8")
    pos += task_type_len

    # created_at
    if pos + 8 > len(data):
        raise ValueError("Incomplete task assignment: missing created_at")
    created_at = struct.unpack_from("<q", data, pos)[0]
    pos += 8

    # attempt
    if pos + 4 > len(data):
        raise ValueError("Incomplete task assignment: missing attempt")
    attempt = struct.unpack_from("<I", data, pos)[0]
    pos += 4

    # payload (rest of data)
    payload = data[pos:]

    return TaskAssignment(
        task_id=task_id,
        task_type=task_type,
        payload=payload,
        created_at=created_at,
        attempt=attempt,
    )
