"""Flo Client

Async client for connecting to Flo servers.
"""

import asyncio
import logging

from .exceptions import (
    ConnectionFailedError,
    InvalidEndpointError,
    NotConnectedError,
    UnexpectedEofError,
    raise_for_status,
)
from .types import HEADER_SIZE, OpCode, StatusCode
from .wire import RawResponse, parse_response_header, serialize_request

logger = logging.getLogger("flo")


class FloClient:
    """Async client for Flo server.

    Example:
        async with FloClient("localhost:9000") as client:
            await client.kv.put("key", b"value")
            value = await client.kv.get("key")
    """

    def __init__(
        self,
        endpoint: str,
        *,
        namespace: str = "default",
        timeout_ms: int = 5000,
        debug: bool = False,
    ):
        """Initialize Flo client.

        Args:
            endpoint: Server endpoint in "host:port" format.
            namespace: Default namespace for operations.
            timeout_ms: Connection and operation timeout in milliseconds.
            debug: Enable debug logging.
        """
        self._endpoint = endpoint
        self._namespace = namespace
        self._timeout = timeout_ms / 1000.0  # Convert to seconds
        self._debug = debug

        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._request_id: int = 0
        self._lock = asyncio.Lock()

        # Parse endpoint
        self._host, self._port = self._parse_endpoint(endpoint)

        # Initialize operation mixins (will be set after import to avoid circular deps)
        from .actions import ActionOperations, WorkerOperations
        from .kv import KVOperations
        from .queue import QueueOperations
        from .streams import StreamOperations

        self.kv = KVOperations(self)
        self.queue = QueueOperations(self)
        self.stream = StreamOperations(self)
        self.action = ActionOperations(self)
        self.worker = WorkerOperations(self)

        if debug:
            logging.basicConfig(level=logging.DEBUG)

    @staticmethod
    def _parse_endpoint(endpoint: str) -> tuple[str, int]:
        """Parse endpoint string into host and port."""
        parts = endpoint.rsplit(":", 1)
        if len(parts) != 2:
            raise InvalidEndpointError(f"Invalid endpoint format: {endpoint} (expected host:port)")

        host = parts[0]
        try:
            port = int(parts[1])
        except ValueError as e:
            raise InvalidEndpointError(f"Invalid port: {parts[1]}") from e

        if port < 1 or port > 65535:
            raise InvalidEndpointError(f"Port out of range: {port}")

        # Handle IPv6 addresses in brackets
        if host.startswith("[") and host.endswith("]"):
            host = host[1:-1]

        return host, port

    @property
    def namespace(self) -> str:
        """Get the default namespace."""
        return self._namespace

    @property
    def is_connected(self) -> bool:
        """Check if client is connected."""
        return self._writer is not None and not self._writer.is_closing()

    async def connect(self) -> "FloClient":
        """Connect to the server.

        Returns:
            Self for method chaining.

        Raises:
            ConnectionFailedError: If connection fails.
        """
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self._host, self._port),
                timeout=self._timeout,
            )
            if self._debug:
                logger.debug(f"[flo] Connected to {self._endpoint}")
            return self
        except asyncio.TimeoutError as e:
            raise ConnectionFailedError(f"Connection timeout: {self._endpoint}") from e
        except OSError as e:
            raise ConnectionFailedError(f"Connection failed: {self._endpoint} - {e}") from e

    async def close(self) -> None:
        """Close the connection."""
        if self._writer:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass
            self._writer = None
            self._reader = None
            if self._debug:
                logger.debug("[flo] Disconnected")

    async def __aenter__(self) -> "FloClient":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()

    def _next_request_id(self) -> int:
        """Get next request ID."""
        self._request_id += 1
        return self._request_id

    async def _send_request(
        self,
        op_code: OpCode,
        namespace: str,
        key: bytes,
        value: bytes,
        options: bytes = b"",
    ) -> RawResponse:
        """Send a request and wait for response.

        Args:
            op_code: Operation code.
            namespace: Namespace for the operation.
            key: Key bytes.
            value: Value bytes.
            options: TLV-encoded options.

        Returns:
            RawResponse with status and data.

        Raises:
            NotConnectedError: If not connected.
            UnexpectedEofError: If connection closed unexpectedly.
        """
        if not self.is_connected or self._writer is None or self._reader is None:
            raise NotConnectedError("Not connected to server")

        async with self._lock:
            request_id = self._next_request_id()

            # Serialize request
            request = serialize_request(
                request_id=request_id,
                op_code=op_code,
                namespace=namespace.encode("utf-8"),
                key=key,
                value=value,
                options=options,
            )

            if self._debug:
                logger.debug(f"[flo] -> {op_code.name} ns={namespace} key={key!r}")

            # Send request
            self._writer.write(request)
            await self._writer.drain()

            # Read response header
            try:
                header_data = await asyncio.wait_for(
                    self._reader.readexactly(HEADER_SIZE),
                    timeout=self._timeout,
                )
            except asyncio.IncompleteReadError as e:
                raise UnexpectedEofError("Connection closed while reading response header") from e

            # Parse header to get data length
            status, data_len, resp_request_id, crc = parse_response_header(header_data)

            # Read response data
            if data_len > 0:
                try:
                    response_data = await asyncio.wait_for(
                        self._reader.readexactly(data_len),
                        timeout=self._timeout,
                    )
                except asyncio.IncompleteReadError as e:
                    raise UnexpectedEofError("Connection closed while reading response data") from e
            else:
                response_data = b""

            if self._debug:
                logger.debug(f"[flo] <- {status.name} {len(response_data)} bytes")

            # Verify CRC32
            from .wire import compute_crc32

            computed_crc = compute_crc32(header_data, response_data)
            if computed_crc != crc:
                from .exceptions import InvalidChecksumError

                raise InvalidChecksumError(f"CRC32 mismatch: 0x{computed_crc:08X} != 0x{crc:08X}")

            return RawResponse(status=status, data=response_data, request_id=resp_request_id)

    async def _send_and_check(
        self,
        op_code: OpCode,
        namespace: str,
        key: bytes,
        value: bytes,
        options: bytes = b"",
        *,
        allow_not_found: bool = False,
    ) -> RawResponse:
        """Send request and check response status.

        Args:
            op_code: Operation code.
            namespace: Namespace for the operation.
            key: Key bytes.
            value: Value bytes.
            options: TLV-encoded options.
            allow_not_found: If True, don't raise for NOT_FOUND status.

        Returns:
            RawResponse with status and data.

        Raises:
            ServerError: If server returns error status.
        """
        response = await self._send_request(op_code, namespace, key, value, options)

        if response.status == StatusCode.OK:
            return response

        if allow_not_found and response.status == StatusCode.NOT_FOUND:
            return response

        raise_for_status(response.status, response.data)
        return response  # Unreachable, but makes type checker happy

    def get_namespace(self, override: str | None) -> str:
        """Get effective namespace, using override if provided."""
        return override if override is not None else self._namespace
