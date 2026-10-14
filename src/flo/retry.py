"""Retrying requests the server refused without running them."""

import asyncio
import contextlib
import random
from dataclasses import dataclass

from .exceptions import ServerError, raise_for_status
from .types import Ran, StatusCode

_RETRYABLE = (StatusCode.OVERLOADED, StatusCode.UNAVAILABLE)


@dataclass(frozen=True)
class RetryPolicy:
    """How often a request the server refused without running it is sent again.

    Only a refusal whose status is overloaded or unavailable and whose ``ran``
    is ``Ran.NO`` is retried: the server says it never ran, so resending is
    safe for any operation, writes included. Each retry is a new request with
    a new id; the server routes it afresh.

    Attributes:
        max_attempts: Tries in all, the first included; 1 (or less) turns
            retrying off.
        initial_backoff_ms: Ceiling of the first wait, in milliseconds; each
            later ceiling doubles, up to ``max_backoff_ms``. Each wait is
            uniform in [0, ceiling].
        max_backoff_ms: Largest ceiling, in milliseconds.

    A negative backoff counts as 0, and a ``max_backoff_ms`` below
    ``initial_backoff_ms`` as ``initial_backoff_ms``.
    """

    max_attempts: int = 4
    initial_backoff_ms: float = 50
    max_backoff_ms: float = 1000

    def __post_init__(self) -> None:
        # A negative bound would reach random.uniform, and a max below the
        # initial would make the ceiling jump down after the first wait.
        initial = max(self.initial_backoff_ms, 0)
        object.__setattr__(self, "initial_backoff_ms", initial)
        object.__setattr__(self, "max_backoff_ms", max(self.max_backoff_ms, initial))


# The default waits at most about 350 ms in all before giving back the refusal.
DEFAULT_RETRY_POLICY = RetryPolicy()


def _refused_unrun(status: StatusCode, data: bytes) -> bool:
    """Whether a response is a refusal the server says it didn't run, of a
    kind worth sending again. A refusal whose body doesn't say whether it ran
    is not retried."""
    if status not in _RETRYABLE:
        return False
    try:
        raise_for_status(status, data)
    except ServerError as e:
        return e.ran == Ran.NO
    except Exception:
        return False
    return False


def is_refused_unrun(exc: BaseException) -> bool:
    """Whether exc is a refusal the server says it didn't run, of a kind worth
    trying again later."""
    return isinstance(exc, ServerError) and exc.status_code in _RETRYABLE and exc.ran == Ran.NO


class _Backoff:
    """A capped, doubling, fully jittered wait, in seconds."""

    def __init__(self, initial_s: float, max_s: float) -> None:
        self._initial = max(initial_s, 0.0)
        self._max = max(max_s, self._initial)
        self._ceiling = 0.0

    def next(self) -> float:
        """The next wait; doubles the ceiling for the one after."""
        if self._ceiling == 0:
            self._ceiling = self._initial
        d = random.uniform(0, self._ceiling)
        self._ceiling = min(self._ceiling * 2, self._max)
        return d

    def reset(self) -> None:
        """Start the waits again from the initial ceiling."""
        self._ceiling = 0.0


def _policy_backoff(policy: RetryPolicy) -> _Backoff:
    return _Backoff(policy.initial_backoff_ms / 1000.0, policy.max_backoff_ms / 1000.0)


# Worker poll loops wait this way after a failed poll, and start again from
# the bottom after a poll that succeeds.
_WORKER_INITIAL_BACKOFF_S = 0.1
_WORKER_MAX_BACKOFF_S = 5.0


def _worker_backoff() -> _Backoff:
    return _Backoff(_WORKER_INITIAL_BACKOFF_S, _WORKER_MAX_BACKOFF_S)


async def _sleep_unless_stopped(stop: asyncio.Event, seconds: float) -> None:
    """Wait seconds, or until stop is set."""
    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)
