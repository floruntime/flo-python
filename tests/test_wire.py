"""Wire protocol unit tests."""

import os
import struct

import google_crc32c
import pytest

from flo.exceptions import TableMismatchError, ValidationError
from flo.types import (
    HEADER_SIZE,
    MAGIC,
    TABLE_HASH,
    VERSION,
    OpCode,
    OptionTag,
    StatusCode,
    StreamID,
)
from flo.wire import (
    OptionsBuilder,
    OptionsIterator,
    build_stream_batch_value,
    compute_crc32c,
    crc32c,
    parse_enqueue_response,
    parse_pending_entries,
    parse_response,
    serialize_action_invoke_value,
    serialize_group_value,
    serialize_request,
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
        builder.add_u64(OptionTag.MAX_AGE_SECONDS, 3_600_000)
        result = builder.build()

        assert result[0] == 0x26  # MAX_AGE_SECONDS tag
        assert result[1] == 8  # length
        assert struct.unpack("<Q", result[2:])[0] == 3_600_000

    def test_add_bytes(self) -> None:
        builder = OptionsBuilder()
        builder.add_bytes(OptionTag.PARTITION_KEY, b"abc123")
        result = builder.build()

        assert result[0] == 0x25  # PARTITION_KEY tag
        assert result[1] == 6  # length
        assert result[2:] == b"abc123"

    def test_add_flag(self) -> None:
        builder = OptionsBuilder()
        builder.add_flag(OptionTag.DRY_RUN)
        result = builder.build()

        assert result == bytes([0x28, 0])  # DRY_RUN tag, length 0

    def test_multiple_options(self) -> None:
        builder = OptionsBuilder()
        builder.add_u64(OptionTag.MAX_AGE_SECONDS, 3_600_000)
        builder.add_u8(OptionTag.PRIORITY, 5)
        builder.add_bytes(OptionTag.PARTITION_KEY, b"abc")
        result = builder.build()

        assert len(result) == 10 + 3 + 5  # u64(10) + u8(3) + bytes(2+3)

    def test_bytes_too_large(self) -> None:
        builder = OptionsBuilder()
        with pytest.raises(ValueError, match="too large"):
            builder.add_bytes(OptionTag.PARTITION_KEY, b"x" * 256)


class TestOptionsIterator:
    """Tests for TLV options parsing."""

    def test_iterate_options(self) -> None:
        # Build options
        builder = OptionsBuilder()
        builder.add_u64(OptionTag.MAX_AGE_SECONDS, 3_600_000)
        builder.add_u8(OptionTag.PRIORITY, 5)
        data = builder.build()

        # Parse them
        options = list(OptionsIterator(data))
        assert len(options) == 2

        assert options[0].tag == OptionTag.MAX_AGE_SECONDS
        assert options[0].as_u64() == 3_600_000

        assert options[1].tag == OptionTag.PRIORITY
        assert options[1].as_u8() == 5

    def test_find_option(self) -> None:
        builder = OptionsBuilder()
        builder.add_u64(OptionTag.MAX_AGE_SECONDS, 7_200_000)
        builder.add_u8(OptionTag.PRIORITY, 10)
        data = builder.build()

        iter = OptionsIterator(data)

        # Find priority (not first)
        priority = iter.find(OptionTag.PRIORITY)
        assert priority is not None
        assert priority.as_u8() == 10

        # Find max age
        age = iter.find(OptionTag.MAX_AGE_SECONDS)
        assert age is not None
        assert age.as_u64() == 7_200_000

        # Find non-existent
        cas = iter.find(OptionTag.ACK_TIMEOUT_MS)
        assert cas is None

    def test_flag_option(self) -> None:
        builder = OptionsBuilder()
        builder.add_flag(OptionTag.DRY_RUN)
        data = builder.build()

        iter = OptionsIterator(data)
        opt = iter.find(OptionTag.DRY_RUN)
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
        magic, payload_len, request_id, crc, op_code, version, flags, table_hash = struct.unpack(
            "<IIQIHBBQ", request[:HEADER_SIZE]
        )

        assert magic == MAGIC
        assert version == VERSION
        assert request_id == 42
        assert op_code == OpCode.KV_GET
        assert flags == 0
        assert table_hash == 0xC688167B51FCC846

    def test_table_op_payload_is_namespace_key_body(self) -> None:
        request = serialize_request(
            request_id=1,
            op_code=OpCode.KV_PUT,
            namespace=b"ns",
            key=b"key",
            value=b"body",
            options=b"",
        )
        assert request[HEADER_SIZE:] == b"\x02\x00ns\x03\x00keybody"

    def test_table_op_takes_no_options(self) -> None:
        with pytest.raises(ValidationError):
            serialize_request(
                request_id=1,
                op_code=OpCode.KV_GET,
                namespace=b"",
                key=b"k",
                value=b"",
                options=b"\x01",
            )

    def test_request_with_options(self) -> None:
        builder = OptionsBuilder()
        builder.add_u64(OptionTag.MAX_AGE_SECONDS, 3_600_000)
        options = builder.build()

        request = serialize_request(
            request_id=1,
            op_code=OpCode.STREAM_READ,
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

    def test_crc32c_check_value(self) -> None:
        assert crc32c(b"123456789") == 0xE3069283

    @pytest.mark.skipif(not os.environ.get("CI"), reason="CI checks the wheel it installed")
    def test_crc32c_is_the_c_implementation(self) -> None:
        # The library falls back to pure Python, hundreds of times slower,
        # where no wheel fits; CI's platforms all have one.
        assert google_crc32c.implementation == "c"

    def test_frame_checksum_matches_the_servers(self) -> None:
        # The same frame as flo's own test of a frame's checksum.
        header = bytes.fromhex("464c4f0005000000070000000000000000000000010103000807060504030201")
        assert compute_crc32c(header, b"hello") == 0xA2996E56

    def test_crc32c_computation(self) -> None:
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
        computed_crc = compute_crc32c(request[:HEADER_SIZE], payload)

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
            "<IIQIBBBBQ",
            MAGIC,
            len(data),
            request_id,
            0,  # CRC placeholder
            VERSION,
            status,
            0,  # flags
            0,  # _pad
            TABLE_HASH,
        )

        crc = compute_crc32c(header_without_crc, data)

        header = struct.pack(
            "<IIQIBBBBQ",
            MAGIC,
            len(data),
            request_id,
            crc,
            VERSION,
            status,
            0,
            0,
            TABLE_HASH,
        )

        return header + data

    def test_an_answer_from_another_protocol_or_table_is_refused(self) -> None:
        cases = [
            (1, 0, "server protocol 1, client protocol 3: upgrade the client"),
            (
                VERSION,
                TABLE_HASH + 1,
                f"server table 0x{TABLE_HASH + 1:016x}, client table 0x{TABLE_HASH:016x}: "
                "upgrade the client",
            ),
        ]
        for version, table, message in cases:
            header = struct.pack("<IIQIBBBBQ", MAGIC, 0, 1, 0, version, 0, 0, 0, table)
            with pytest.raises(TableMismatchError, match=message) as e:
                parse_response(header)
            assert e.value.server_table == table

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


class TestParseEnqueueResponse:
    """Tests for enqueue response parsing."""

    def test_parse_enqueue_response(self) -> None:
        data = struct.pack("<Q", 12345)
        seq = parse_enqueue_response(data)
        assert seq == 12345


class TestBuildStreamBatchValue:
    """Tests for stream append batch framing."""

    def test_payload_and_headers(self) -> None:
        value = build_stream_batch_value(b"hello", {"k1": "v1"})

        assert struct.unpack("<I", value[0:4])[0] == 1
        assert struct.unpack("<I", value[4:8])[0] == 5
        assert value[8:13] == b"hello"
        assert struct.unpack("<H", value[13:15])[0] == 1


class TestGroupPendingWire:
    """A group pending request names the group, and a consumer only to filter."""

    def test_serialize_group_pending_no_consumer(self) -> None:
        value = serialize_group_value("grp")
        # [group_len:u16][group] only — no consumer trailer.
        assert value == struct.pack("<H", 3) + b"grp"

    def test_serialize_group_pending_with_consumer(self) -> None:
        value = serialize_group_value("grp", "cons")
        assert value == struct.pack("<H", 3) + b"grp" + struct.pack("<H", 4) + b"cons"

    def test_a_group_name_past_256_bytes_is_refused_before_sending(self) -> None:
        serialize_group_value("g" * 256)
        with pytest.raises(ValidationError):
            serialize_group_value("g" * 257)
        with pytest.raises(ValidationError):
            serialize_group_value("")

    def test_parse_pending_entries(self) -> None:
        blob = (
            struct.pack("<I", 2)
            + struct.pack("<Q", 111)
            + struct.pack("<Q", 5)
            + struct.pack("<I", 3)
            + struct.pack("<H", 4)
            + b"cons"
            + struct.pack("<Q", 222)
            + struct.pack("<Q", 9)
            + struct.pack("<I", 1)
            + struct.pack("<H", 0)
        )
        entries = parse_pending_entries(blob)
        assert len(entries) == 2
        assert entries[0].id == StreamID(timestamp_ms=111, sequence=5)
        assert entries[0].consumer == "cons"
        assert entries[0].delivery_count == 3
        assert entries[1].id == StreamID(timestamp_ms=222, sequence=9)
        assert entries[1].consumer == ""
        assert entries[1].delivery_count == 1

    def test_parse_pending_entries_empty(self) -> None:
        assert parse_pending_entries(struct.pack("<I", 0)) == []


class TestActionInvokeValue:
    """Invoke value: [has_labels:u8]([labels_len:u16][labels])?[input]."""

    def test_no_labels(self) -> None:
        assert serialize_action_invoke_value(b"x") == b"\x00x"

    def test_labels(self) -> None:
        value = serialize_action_invoke_value(b"x", labels={"gpu": True})
        assert value == b"\x01\x0c\x00" + b'{"gpu":true}' + b"x"

    def test_oversized_labels_raise(self) -> None:
        with pytest.raises(ValueError):
            serialize_action_invoke_value(b"x", labels={"k": "v" * 0xFFFF})
