"""Wire protocol unit tests."""

import struct

import pytest

from flo.types import (
    HEADER_SIZE,
    MAGIC,
    VERSION,
    OpCode,
    OptionTag,
    StatusCode,
)
from flo.wire import (
    OptionsBuilder,
    OptionsIterator,
    compute_crc32,
    parse_dequeue_response,
    parse_enqueue_response,
    parse_response,
    parse_scan_response,
    serialize_request,
    serialize_seqs,
)


class TestOptionsBuilder:
    """Tests for TLV options building."""

    def test_add_u8(self) -> None:
        builder = OptionsBuilder()
        builder.add_u8(OptionTag.PRIORITY, 5)
        result = builder.build()

        assert result == bytes([0x10, 1, 5])

    def test_add_u32(self) -> None:
        builder = OptionsBuilder()
        builder.add_u32(OptionTag.LIMIT, 100)
        result = builder.build()

        assert result[0] == 0x05  # LIMIT tag
        assert result[1] == 4  # length
        assert struct.unpack("<I", result[2:])[0] == 100

    def test_add_u64(self) -> None:
        builder = OptionsBuilder()
        builder.add_u64(OptionTag.TTL_SECONDS, 3600)
        result = builder.build()

        assert result[0] == 0x01  # TTL_SECONDS tag
        assert result[1] == 8  # length
        assert struct.unpack("<Q", result[2:])[0] == 3600

    def test_add_bytes(self) -> None:
        builder = OptionsBuilder()
        builder.add_bytes(OptionTag.DEDUP_KEY, b"abc123")
        result = builder.build()

        assert result[0] == 0x13  # DEDUP_KEY tag
        assert result[1] == 6  # length
        assert result[2:] == b"abc123"

    def test_add_flag(self) -> None:
        builder = OptionsBuilder()
        builder.add_flag(OptionTag.IF_NOT_EXISTS)
        result = builder.build()

        assert result == bytes([0x03, 0])  # IF_NOT_EXISTS tag, length 0

    def test_multiple_options(self) -> None:
        builder = OptionsBuilder()
        builder.add_u64(OptionTag.TTL_SECONDS, 3600)
        builder.add_u8(OptionTag.PRIORITY, 5)
        builder.add_bytes(OptionTag.DEDUP_KEY, b"abc")
        result = builder.build()

        assert len(result) == 10 + 3 + 5  # u64(10) + u8(3) + bytes(2+3)

    def test_bytes_too_large(self) -> None:
        builder = OptionsBuilder()
        with pytest.raises(ValueError, match="too large"):
            builder.add_bytes(OptionTag.DEDUP_KEY, b"x" * 256)


class TestOptionsIterator:
    """Tests for TLV options parsing."""

    def test_iterate_options(self) -> None:
        # Build options
        builder = OptionsBuilder()
        builder.add_u64(OptionTag.TTL_SECONDS, 3600)
        builder.add_u8(OptionTag.PRIORITY, 5)
        data = builder.build()

        # Parse them
        options = list(OptionsIterator(data))
        assert len(options) == 2

        assert options[0].tag == OptionTag.TTL_SECONDS
        assert options[0].as_u64() == 3600

        assert options[1].tag == OptionTag.PRIORITY
        assert options[1].as_u8() == 5

    def test_find_option(self) -> None:
        builder = OptionsBuilder()
        builder.add_u64(OptionTag.TTL_SECONDS, 7200)
        builder.add_u8(OptionTag.PRIORITY, 10)
        data = builder.build()

        iter = OptionsIterator(data)

        # Find priority (not first)
        priority = iter.find(OptionTag.PRIORITY)
        assert priority is not None
        assert priority.as_u8() == 10

        # Find TTL
        ttl = iter.find(OptionTag.TTL_SECONDS)
        assert ttl is not None
        assert ttl.as_u64() == 7200

        # Find non-existent
        cas = iter.find(OptionTag.CAS_VERSION)
        assert cas is None

    def test_flag_option(self) -> None:
        builder = OptionsBuilder()
        builder.add_flag(OptionTag.IF_NOT_EXISTS)
        data = builder.build()

        iter = OptionsIterator(data)
        opt = iter.find(OptionTag.IF_NOT_EXISTS)
        assert opt is not None
        assert opt.is_flag()


class TestSerializeRequest:
    """Tests for request serialization."""

    def test_basic_request(self) -> None:
        request = serialize_request(
            request_id=42,
            op_code=OpCode.KV_GET,
            namespace=b"test",
            key=b"mykey",
            value=b"",
            options=b"",
        )

        assert len(request) > HEADER_SIZE

        # Check header
        magic, payload_len, request_id, crc, version, op_code, flags, reserved = struct.unpack(
            "<IIQIBBBB", request[:HEADER_SIZE]
        )

        assert magic == MAGIC
        assert version == VERSION
        assert request_id == 42
        assert op_code == OpCode.KV_GET
        assert flags == 0
        assert reserved == 0

    def test_request_with_value(self) -> None:
        request = serialize_request(
            request_id=1,
            op_code=OpCode.KV_PUT,
            namespace=b"ns",
            key=b"key",
            value=b"value",
            options=b"",
        )

        # Verify payload contains namespace, key, value
        payload = request[HEADER_SIZE:]

        offset = 0
        ns_len = struct.unpack("<H", payload[offset : offset + 2])[0]
        offset += 2
        assert payload[offset : offset + ns_len] == b"ns"
        offset += ns_len

        key_len = struct.unpack("<H", payload[offset : offset + 2])[0]
        offset += 2
        assert payload[offset : offset + key_len] == b"key"
        offset += key_len

        val_len = struct.unpack("<I", payload[offset : offset + 4])[0]
        offset += 4
        assert payload[offset : offset + val_len] == b"value"

    def test_request_with_options(self) -> None:
        builder = OptionsBuilder()
        builder.add_u64(OptionTag.TTL_SECONDS, 3600)
        options = builder.build()

        request = serialize_request(
            request_id=1,
            op_code=OpCode.KV_PUT,
            namespace=b"ns",
            key=b"key",
            value=b"val",
            options=options,
        )

        # Verify options are included in payload
        # Payload: [ns_len:u16][ns][key_len:u16][key][val_len:u32][val][opts_len:u16][opts]
        # Overhead = 2 + len("ns") + 2 + len("key") + 4 + len("val") + 2 = 18
        overhead = 2 + 2 + 2 + 3 + 4 + 3 + 2
        assert len(options) == len(request) - HEADER_SIZE - overhead

    def test_crc32_computation(self) -> None:
        request = serialize_request(
            request_id=1,
            op_code=OpCode.KV_GET,
            namespace=b"test",
            key=b"key",
            value=b"",
            options=b"",
        )

        # Extract CRC from header
        stored_crc = struct.unpack("<I", request[16:20])[0]

        # Recompute CRC
        payload = request[HEADER_SIZE:]
        computed_crc = compute_crc32(request[:HEADER_SIZE], payload)

        assert stored_crc == computed_crc


class TestParseResponse:
    """Tests for response parsing."""

    def _make_response(
        self,
        status: StatusCode,
        data: bytes,
        request_id: int = 1,
    ) -> bytes:
        """Helper to create a valid response."""
        header_without_crc = struct.pack(
            "<IIQIBBBB",
            MAGIC,
            len(data),
            request_id,
            0,  # CRC placeholder
            VERSION,
            status,
            0,  # flags
            0,  # reserved
        )

        crc = compute_crc32(header_without_crc, data)

        header = struct.pack(
            "<IIQIBBBB",
            MAGIC,
            len(data),
            request_id,
            crc,
            VERSION,
            status,
            0,
            0,
        )

        return header + data

    def test_parse_ok_response(self) -> None:
        response_data = self._make_response(StatusCode.OK, b"value")
        parsed = parse_response(response_data)

        assert parsed.status == StatusCode.OK
        assert parsed.data == b"value"
        assert parsed.is_ok()

    def test_parse_not_found_response(self) -> None:
        response_data = self._make_response(StatusCode.NOT_FOUND, b"")
        parsed = parse_response(response_data)

        assert parsed.status == StatusCode.NOT_FOUND
        assert parsed.is_not_found()

    def test_parse_empty_data(self) -> None:
        response_data = self._make_response(StatusCode.OK, b"")
        parsed = parse_response(response_data)

        assert parsed.data == b""


class TestParseScanResponse:
    """Tests for scan response parsing."""

    def test_parse_scan_response(self) -> None:
        # Build scan response:
        # [has_more:u8][cursor_len:u32][cursor][count:u32][entries...]
        data = bytearray()
        data.append(1)  # has_more = true
        data.extend(struct.pack("<I", 4))  # cursor_len
        data.extend(b"curs")  # cursor
        data.extend(struct.pack("<I", 2))  # count

        # Entry 1: key="key1", value="val1"
        data.extend(struct.pack("<H", 4))
        data.extend(b"key1")
        data.extend(struct.pack("<I", 4))
        data.extend(b"val1")

        # Entry 2: key="key2", value=""
        data.extend(struct.pack("<H", 4))
        data.extend(b"key2")
        data.extend(struct.pack("<I", 0))

        result = parse_scan_response(bytes(data))

        assert result.has_more is True
        assert result.cursor == b"curs"
        assert len(result.entries) == 2
        assert result.entries[0].key == b"key1"
        assert result.entries[0].value == b"val1"
        assert result.entries[1].key == b"key2"
        assert result.entries[1].value is None


class TestParseDequeueResponse:
    """Tests for dequeue response parsing."""

    def test_parse_dequeue_response(self) -> None:
        # Build dequeue response:
        # [count:u32][messages...]
        # Message: [seq:u64][payload_len:u32][payload]
        data = bytearray()
        data.extend(struct.pack("<I", 2))  # count

        # Message 1
        data.extend(struct.pack("<Q", 100))  # seq
        data.extend(struct.pack("<I", 5))  # payload_len
        data.extend(b"task1")

        # Message 2
        data.extend(struct.pack("<Q", 101))  # seq
        data.extend(struct.pack("<I", 5))  # payload_len
        data.extend(b"task2")

        result = parse_dequeue_response(bytes(data))

        assert len(result.messages) == 2
        assert result.messages[0].seq == 100
        assert result.messages[0].payload == b"task1"
        assert result.messages[1].seq == 101
        assert result.messages[1].payload == b"task2"


class TestParseEnqueueResponse:
    """Tests for enqueue response parsing."""

    def test_parse_enqueue_response(self) -> None:
        data = struct.pack("<Q", 12345)
        seq = parse_enqueue_response(data)
        assert seq == 12345


class TestSerializeSeqs:
    """Tests for sequence number serialization."""

    def test_serialize_seqs(self) -> None:
        seqs = [1, 2, 3]
        result = serialize_seqs(seqs)

        assert len(result) == 4 + 3 * 8  # count + 3 u64s

        count = struct.unpack("<I", result[0:4])[0]
        assert count == 3

        seq1 = struct.unpack("<Q", result[4:12])[0]
        seq2 = struct.unpack("<Q", result[12:20])[0]
        seq3 = struct.unpack("<Q", result[20:28])[0]

        assert seq1 == 1
        assert seq2 == 2
        assert seq3 == 3

    def test_serialize_empty_seqs(self) -> None:
        result = serialize_seqs([])
        assert result == struct.pack("<I", 0)
