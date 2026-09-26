"""block_ms / wait_ms validation: 0 means don't wait, 300000 ms is the ceiling."""

import pytest

from flo import BlockTooLongError, ValidationError
from flo.types import MAX_BLOCK_MS, OptionTag
from flo.wire import OptionsBuilder, validate_block_ms
from flo.worker import (
    DEFAULT_WORKER_BLOCK_MS,
    ActionWorkerOptions,
    StreamWorkerOptions,
)


class TestValidateBlockMs:
    def test_zero_and_ceiling_accepted(self) -> None:
        validate_block_ms(0)
        validate_block_ms(MAX_BLOCK_MS)

    def test_over_ceiling_refused(self) -> None:
        with pytest.raises(BlockTooLongError, match="at most 300000 ms"):
            validate_block_ms(MAX_BLOCK_MS + 1)

    def test_is_a_validation_error(self) -> None:
        assert issubclass(BlockTooLongError, ValidationError)

    @pytest.mark.parametrize("tag", [OptionTag.BLOCK_MS, OptionTag.WAIT_MS])
    def test_options_builder_refuses_before_round_trip(self, tag: OptionTag) -> None:
        OptionsBuilder().add_u32(tag, MAX_BLOCK_MS)
        with pytest.raises(BlockTooLongError):
            OptionsBuilder().add_u32(tag, MAX_BLOCK_MS + 1)

    def test_other_u32_options_unaffected(self) -> None:
        OptionsBuilder().add_u32(OptionTag.COUNT, MAX_BLOCK_MS + 1)


class TestWorkerBlockMs:
    def test_action_worker_zero_means_default(self) -> None:
        assert ActionWorkerOptions(block_ms=0).block_ms == DEFAULT_WORKER_BLOCK_MS

    def test_stream_worker_zero_means_default(self) -> None:
        opts = StreamWorkerOptions(stream="s", block_ms=0)
        assert opts.block_ms == DEFAULT_WORKER_BLOCK_MS

    def test_explicit_value_kept(self) -> None:
        assert ActionWorkerOptions(block_ms=1000).block_ms == 1000
        assert StreamWorkerOptions(stream="s", block_ms=MAX_BLOCK_MS).block_ms == MAX_BLOCK_MS

    def test_over_ceiling_refused_at_config_time(self) -> None:
        with pytest.raises(BlockTooLongError):
            ActionWorkerOptions(block_ms=MAX_BLOCK_MS + 1)
        with pytest.raises(BlockTooLongError):
            StreamWorkerOptions(stream="s", block_ms=MAX_BLOCK_MS + 1)
