"""Flo SDK Exceptions

Exception classes for Flo client errors.
"""

import asyncio
import contextlib
from collections.abc import Callable

from .types import TABLE_HASH, VERSION, Ran, Reason, StatusCode


class FloError(Exception):
    """Base exception for all Flo errors."""

    pass


class NonRetryableError(FloError):
    """Marks an action error as non-retryable.

    When raised from an action handler, the task will be failed with
    retry=False, so the server will not re-queue it.

    Example:
        raise NonRetryableError("invalid input, will never succeed")
    """

    pass


# =============================================================================
# Connection Errors
# =============================================================================


class NotConnectedError(FloError):
    """Client is not connected to the server."""

    pass


class ConnectionFailedError(FloError):
    """Failed to establish connection to the server."""

    pass


class InvalidEndpointError(FloError):
    """Invalid endpoint format."""

    pass


class UnexpectedEofError(FloError):
    """Unexpected end of stream while reading from server."""

    pass


class RequestTimeoutError(FloError, asyncio.TimeoutError):
    """The server did not answer a request in time; the connection was dropped."""

    pass


# =============================================================================
# Protocol Errors
# =============================================================================


class ProtocolError(FloError):
    """Base class for protocol-related errors."""

    pass


class InvalidMagicError(ProtocolError):
    """Invalid protocol magic number."""

    pass


class TableMismatchError(ProtocolError):
    """The server was built from another protocol version or op table, so
    neither can read the other. Upgrade the SDK and the server together."""

    def __init__(self, server_version: int, server_table: int) -> None:
        self.server_version = server_version
        self.server_table = server_table
        if server_version != VERSION:
            message = (
                f"server protocol {server_version}, client protocol {VERSION}: upgrade the client"
            )
        else:
            message = (
                f"server table 0x{server_table:016x}, client table 0x{TABLE_HASH:016x}: "
                "upgrade the client"
            )
        super().__init__(message)


class InvalidChecksumError(ProtocolError):
    """A frame's CRC32C checksum didn't match."""

    pass


class PayloadTooLargeError(ProtocolError):
    """Payload exceeds maximum allowed size."""

    pass


class IncompleteResponseError(ProtocolError):
    """Response data is incomplete."""

    pass


# =============================================================================
# Validation Errors
# =============================================================================


class ValidationError(FloError):
    """Base class for validation errors."""

    pass


class NamespaceTooLargeError(ValidationError):
    """Namespace exceeds maximum size (255 bytes)."""

    pass


class KeyTooLargeError(ValidationError):
    """Key exceeds maximum size (64 KB)."""

    pass


class ValueTooLargeError(ValidationError):
    """Value exceeds maximum size (16 MB)."""

    pass


class BlockTooLongError(ValidationError):
    """A blocking wait (block_ms) exceeds 300000 ms (5 minutes)."""

    pass


# =============================================================================
# Server Response Errors
# =============================================================================


class ServerError(FloError):
    """Base class for server-returned errors."""

    status_code: StatusCode
    # Why; Reason.UNCLASSIFIED when the server didn't say.
    reason: Reason = Reason.UNCLASSIFIED
    # Whether the request took effect; unknown unless the server said.
    ran: Ran = Ran.UNKNOWN

    def __init__(self, message: str, status_code: StatusCode):
        super().__init__(message)
        self.status_code = status_code


class NotFoundError(ServerError):
    """Resource not found."""

    def __init__(self, message: str = "Not found"):
        super().__init__(message, StatusCode.NOT_FOUND)


class BadRequestError(ServerError):
    """Invalid request parameters."""

    def __init__(self, message: str = "Bad request"):
        super().__init__(message, StatusCode.BAD_REQUEST)


class ConflictError(ServerError):
    """Conflict (e.g., CAS version mismatch)."""

    def __init__(self, message: str = "Conflict"):
        super().__init__(message, StatusCode.CONFLICT)


class UnauthorizedError(ServerError):
    """Authentication required or failed."""

    def __init__(self, message: str = "Unauthorized"):
        super().__init__(message, StatusCode.UNAUTHORIZED)


class OverloadedError(ServerError):
    """Server is overloaded."""

    def __init__(self, message: str = "Server overloaded"):
        super().__init__(message, StatusCode.OVERLOADED)


class RateLimitedError(ServerError):
    """Request rate limit exceeded."""

    def __init__(self, message: str = "Request rate limit exceeded"):
        super().__init__(message, StatusCode.RATE_LIMITED)


class UnavailableError(ServerError):
    """No leader, or the shard isn't taking writes or is offline. Retryable.

    The message says why; an offline shard stays unavailable until an
    operator acts, so whether and when to retry is the caller's call.
    """

    def __init__(self, message: str = StatusCode.UNAVAILABLE.message()):
        super().__init__(message, StatusCode.UNAVAILABLE)


class InternalServerError(ServerError):
    """Internal server error. Not retryable: the server uses it for a write
    that committed but wasn't applied, which must not be resent."""

    def __init__(self, message: str = "Internal server error"):
        super().__init__(message, StatusCode.INTERNAL_ERROR)


class GenericServerError(ServerError):
    """Generic server error, or a status with no dedicated error type."""

    def __init__(
        self, message: str = "Generic error", status_code: StatusCode = StatusCode.ERROR_GENERIC
    ):
        super().__init__(message, status_code)


def is_connection_error(exc: BaseException) -> bool:
    """Return True if the exception indicates a broken connection.

    Connection errors may be resolved by reconnecting. A timed-out request
    counts: the client drops its connection rather than risk reading the late
    reply as the next answer.
    """
    return isinstance(exc, (UnexpectedEofError, NotConnectedError, RequestTimeoutError, OSError))


def raise_for_status(status: StatusCode, data: bytes = b"") -> None:
    """Raise appropriate exception for non-OK status codes.

    Args:
        status: The status code from the server response.
        data: Optional response data that may contain error details.

    Raises:
        ServerError: If status is not OK.
    """
    if status == StatusCode.OK:
        return

    # A refusal's body: [reason:u16][ran:u8][message].
    if len(data) < 3:
        raise IncompleteResponseError(f"a refusal body of {len(data)} bytes")
    try:
        reason = Reason(int.from_bytes(data[0:2], "little"))
        ran = Ran(data[2])
    except ValueError as e:
        raise IncompleteResponseError(f"a refusal with reason and ran {data[0:3].hex()}") from e
    message = status.message()
    if len(data) > 3:
        with contextlib.suppress(UnicodeDecodeError, ValueError):
            message = data[3:].decode("utf-8")
    error = _error_for(status, message)
    error.reason = reason
    error.ran = ran
    raise error


def _error_for(status: StatusCode, message: str) -> ServerError:

    error_map: dict[StatusCode, Callable[[str], ServerError]] = {
        StatusCode.NOT_FOUND: NotFoundError,
        StatusCode.BAD_REQUEST: BadRequestError,
        StatusCode.CONFLICT: ConflictError,
        StatusCode.UNAUTHORIZED: UnauthorizedError,
        StatusCode.OVERLOADED: OverloadedError,
        StatusCode.UNAVAILABLE: UnavailableError,
        StatusCode.RATE_LIMITED: RateLimitedError,
        StatusCode.INTERNAL_ERROR: InternalServerError,
        StatusCode.ERROR_GENERIC: GenericServerError,
    }

    error_class = error_map.get(status)
    if error_class is not None:
        return error_class(message)
    if status.name not in StatusCode.__members__:
        message = f"Unknown status {int(status)}: {message}"
    return GenericServerError(message, status)
