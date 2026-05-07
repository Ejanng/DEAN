"""Socket helpers for sending and receiving DEAN JSON messages."""

from __future__ import annotations

import socket
from typing import Any, Iterable, Mapping

from config import (
    JSON_ENCODING,
    MESSAGE_DELIMITER,
    NETWORK_BUFFER_SIZE,
    SOCKET_BACKLOG,
    SOCKET_TIMEOUT_SECONDS,
)
from message_protocol import ProtocolError, decode_message, encode_message, validate_message


class ConnectionClosedError(ConnectionError):
    """Raised when a socket closes while waiting for a full message."""


class MessageConnection:
    """Wrap a socket with line-delimited JSON send/receive helpers."""

    def __init__(
        self,
        sock: socket.socket,
        address: tuple[str, int] | None = None,
        *,
        buffer_size: int = NETWORK_BUFFER_SIZE,
        delimiter: str = MESSAGE_DELIMITER,
        encoding: str = JSON_ENCODING,
        timeout: float | None = SOCKET_TIMEOUT_SECONDS,
    ) -> None:
        self.socket = sock
        self.address = address
        self.buffer_size = buffer_size
        self.delimiter = delimiter
        self.encoding = encoding
        self._buffer = ""

        if timeout is not None:
            self.socket.settimeout(timeout)

    def send_message(self, message: Mapping[str, Any]) -> None:
        """Validate and send one JSON message."""

        payload = encode_message(validate_message(message))
        self.socket.sendall(payload)

    def receive_message(self) -> dict[str, Any]:
        """Receive a single complete message, handling partial TCP reads."""

        while True:
            if self.delimiter in self._buffer:
                raw_message, self._buffer = self._buffer.split(self.delimiter, 1)
                if not raw_message.strip():
                    continue
                return decode_message(raw_message)

            chunk = self.socket.recv(self.buffer_size)
            if not chunk:
                if self._buffer.strip():
                    raise ConnectionClosedError(
                        "Connection closed before a complete message was received."
                    )
                raise ConnectionClosedError("Connection closed by peer.")

            try:
                self._buffer += chunk.decode(self.encoding)
            except UnicodeDecodeError as exc:
                raise ProtocolError("Received bytes that were not valid UTF-8.") from exc

    def close(self) -> None:
        """Close the underlying socket safely."""

        try:
            self.socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.socket.close()

    def fileno(self) -> int:
        return self.socket.fileno()

    def settimeout(self, timeout: float | None) -> None:
        self.socket.settimeout(timeout)

    def __enter__(self) -> "MessageConnection":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()


def create_server_socket(
    host: str,
    port: int,
    *,
    backlog: int = SOCKET_BACKLOG,
    timeout: float | None = SOCKET_TIMEOUT_SECONDS,
) -> socket.socket:
    """Create, bind, and listen on a TCP server socket."""

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))
    server_socket.listen(backlog)
    if timeout is not None:
        server_socket.settimeout(timeout)
    return server_socket


def create_client_connection(
    host: str,
    port: int,
    *,
    timeout: float | None = SOCKET_TIMEOUT_SECONDS,
) -> MessageConnection:
    """Connect to a TCP server and wrap the socket in MessageConnection."""

    client_socket = socket.create_connection((host, port), timeout=timeout)
    return MessageConnection(client_socket, address=(host, port), timeout=timeout)


def accept_connection(
    server_socket: socket.socket,
    *,
    timeout: float | None = SOCKET_TIMEOUT_SECONDS,
) -> MessageConnection:
    """Accept one client connection and wrap it for JSON messaging."""

    client_socket, address = server_socket.accept()
    return MessageConnection(client_socket, address=address, timeout=timeout)


def broadcast_message(
    connections: Iterable[MessageConnection],
    message: Mapping[str, Any],
) -> list[tuple[tuple[str, int] | None, Exception]]:
    """Send the same message to many clients and collect send failures."""

    validated_message = validate_message(message)
    failures: list[tuple[tuple[str, int] | None, Exception]] = []

    for connection in connections:
        try:
            connection.send_message(validated_message)
        except (OSError, ProtocolError) as exc:
            failures.append((connection.address, exc))

    return failures
