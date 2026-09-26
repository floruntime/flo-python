"""block_ms ceiling, and the worker's reading of 0 as its default."""

import pytest

from flo import BlockTooLongError, FloClient, GetOptions, ValidationError
from flo.types import _DEFAULT_WORKER_BLOCK_MS, MAX_BLOCK_MS, OptionTag
from flo.wire import OptionsBuilder
from flo.worker import ActionWorkerOptions, StreamWorkerOptions


class TestOptionsBuilder:
    @pytest.mark.parametrize("tag", [OptionTag.BLOCK_MS, OptionTag.WAIT_MS])
    def test_ceiling(self, tag: OptionTag) -> None:
        OptionsBuilder().add_u32(tag, 0)
        OptionsBuilder().add_u32(tag, MAX_BLOCK_MS)
        with pytest.raises(BlockTooLongError, match="at most 300000 ms"):
            OptionsBuilder().add_u32(tag, MAX_BLOCK_MS + 1)

    def test_negative_refused(self) -> None:
        with pytest.raises(ValidationError):
            OptionsBuilder().add_u32(OptionTag.BLOCK_MS, -1)

    def test_other_u32_options_unaffected(self) -> None:
        OptionsBuilder().add_u32(OptionTag.COUNT, MAX_BLOCK_MS + 1)

    def test_block_too_long_is_a_validation_error(self) -> None:
        assert issubclass(BlockTooLongError, ValidationError)


async def test_refused_before_the_round_trip() -> None:
    # Unconnected: without the check this would raise NotConnectedError.
    client = FloClient("localhost:1")
    with pytest.raises(BlockTooLongError):
        await client.kv.get("k", GetOptions(block_ms=MAX_BLOCK_MS + 1))


class TestWorkerBlockMs:
    @pytest.mark.parametrize("given", [0, None])
    def test_unset_means_default(self, given: int | None) -> None:
        assert ActionWorkerOptions(block_ms=given).block_ms == _DEFAULT_WORKER_BLOCK_MS  # type: ignore[arg-type]
        opts = StreamWorkerOptions(stream="s", block_ms=given)  # type: ignore[arg-type]
        assert opts.block_ms == _DEFAULT_WORKER_BLOCK_MS

    def test_explicit_value_kept(self) -> None:
        assert ActionWorkerOptions(block_ms=1000).block_ms == 1000
        assert StreamWorkerOptions(stream="s", block_ms=MAX_BLOCK_MS).block_ms == MAX_BLOCK_MS

    def test_out_of_range_refused_at_config_time(self) -> None:
        with pytest.raises(BlockTooLongError):
            ActionWorkerOptions(block_ms=MAX_BLOCK_MS + 1)
        with pytest.raises(BlockTooLongError):
            StreamWorkerOptions(stream="s", block_ms=MAX_BLOCK_MS + 1)
        with pytest.raises(ValidationError):
            ActionWorkerOptions(block_ms=-1)
