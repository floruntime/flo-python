"""Flo SDK Exceptions

Exception classes for Flo client errors.
"""

import asyncio
import contextlib

from .types import StatusCode


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


class UnsupportedVersionError(ProtocolError):
    """Unsupported protocol version."""

    pass


class InvalidChecksumError(ProtocolError):
    """CRC32 checksum validation failed."""

    pass


class InvalidReservedFieldError(ProtocolError):
    """Reserved field contains non-zero value."""

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

    # Try to decode error message from data
    message = status.message()
    if data:
        with contextlib.suppress(UnicodeDecodeError, ValueError):
            message = data.decode("utf-8")

    error_map = {
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
        raise error_class(message)
    if status.name not in StatusCode.__members__:
        message = f"Unknown status {int(status)}: {message}"
    raise GenericServerError(message, status)
