"""Flo SDK Exceptions

Exception classes for Flo client errors.
"""

from .types import StatusCode


class FloError(Exception):
    """Base exception for all Flo errors."""

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


class InternalServerError(ServerError):
    """Internal server error."""

    def __init__(self, message: str = "Internal server error"):
        super().__init__(message, StatusCode.INTERNAL_ERROR)


class GenericServerError(ServerError):
    """Generic server error."""

    def __init__(self, message: str = "Generic error"):
        super().__init__(message, StatusCode.ERROR_GENERIC)


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
        try:
            message = data.decode("utf-8")
        except (UnicodeDecodeError, ValueError):
            pass

    error_map = {
        StatusCode.NOT_FOUND: NotFoundError,
        StatusCode.BAD_REQUEST: BadRequestError,
        StatusCode.CONFLICT: ConflictError,
        StatusCode.UNAUTHORIZED: UnauthorizedError,
        StatusCode.OVERLOADED: OverloadedError,
        StatusCode.RATE_LIMITED: RateLimitedError,
        StatusCode.INTERNAL_ERROR: InternalServerError,
        StatusCode.ERROR_GENERIC: GenericServerError,
    }

    error_class = error_map.get(status, GenericServerError)
    raise error_class(message)
