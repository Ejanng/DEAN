from __future__ import annotations

import socket
import unittest
from unittest import mock

from message_protocol import build_lookup_message, encode_message
from network_layer import (
    ConnectionClosedError,
    MessageConnection,
    accept_connection,
    broadcast_message,
    create_client_connection,
    create_server_socket,
)


class FakeSocket:
    def __init__(
        self,
        *,
        recv_chunks: list[bytes] | None = None,
        send_error: Exception | None = None,
    ) -> None:
        self.recv_chunks = list(recv_chunks or [])
        self.send_error = send_error
        self.sent_data: list[bytes] = []
        self.timeout: float | None = None
        self.shutdown_called = False
        self.closed = False
        self.socket_options: list[tuple[int, int, int]] = []
        self.bound_address: tuple[str, int] | None = None
        self.listen_backlog: int | None = None
        self.accept_queue: list[tuple[FakeSocket, tuple[str, int]]] = []

    def settimeout(self, timeout: float | None) -> None:
        self.timeout = timeout

    def sendall(self, data: bytes) -> None:
        if self.send_error:
            raise self.send_error
        self.sent_data.append(data)

    def recv(self, buffer_size: int) -> bytes:
        del buffer_size
        if self.recv_chunks:
            return self.recv_chunks.pop(0)
        return b""

    def shutdown(self, how: int) -> None:
        del how
        self.shutdown_called = True

    def close(self) -> None:
        self.closed = True

    def fileno(self) -> int:
        return 123

    def setsockopt(self, level: int, optname: int, value: int) -> None:
        self.socket_options.append((level, optname, value))

    def bind(self, address: tuple[str, int]) -> None:
        self.bound_address = address

    def listen(self, backlog: int) -> None:
        self.listen_backlog = backlog

    def accept(self) -> tuple["FakeSocket", tuple[str, int]]:
        return self.accept_queue.pop(0)


class NetworkLayerTests(unittest.TestCase):
    def test_send_message_writes_encoded_json(self) -> None:
        fake_socket = FakeSocket()
        connection = MessageConnection(fake_socket, timeout=1.0)
        message = build_lookup_message("emergency.monitor.main", 1)

        connection.send_message(message)

        self.assertEqual(fake_socket.sent_data, [encode_message(message)])

    def test_receive_message_reassembles_partial_chunks(self) -> None:
        message = build_lookup_message("emergency.monitor.main", 2)
        payload = encode_message(message)
        midpoint = len(payload) // 2
        fake_socket = FakeSocket(recv_chunks=[payload[:midpoint], payload[midpoint:]])
        connection = MessageConnection(fake_socket, timeout=1.0)

        self.assertEqual(connection.receive_message(), message)

    def test_receive_message_raises_if_connection_closes_mid_message(self) -> None:
        partial_payload = b'{"type":"LOOKUP","logical_name":"emergency.monitor.main"'
        fake_socket = FakeSocket(recv_chunks=[partial_payload, b""])
        connection = MessageConnection(fake_socket, timeout=1.0)

        with self.assertRaises(ConnectionClosedError):
            connection.receive_message()

    def test_broadcast_message_sends_to_every_connection(self) -> None:
        sender_one = MessageConnection(FakeSocket(), timeout=1.0)
        sender_two = MessageConnection(FakeSocket(), timeout=1.0)
        message = build_lookup_message("emergency.monitor.main", 3)

        failures = broadcast_message([sender_one, sender_two], message)

        self.assertEqual(failures, [])
        self.assertEqual(sender_one.socket.sent_data, [encode_message(message)])
        self.assertEqual(sender_two.socket.sent_data, [encode_message(message)])

    def test_broadcast_message_collects_send_failures(self) -> None:
        failing_socket = FakeSocket(send_error=OSError("broken pipe"))
        healthy_socket = FakeSocket()
        failing_connection = MessageConnection(failing_socket, address=("127.0.0.1", 5001))
        healthy_connection = MessageConnection(healthy_socket, address=("127.0.0.1", 5002))
        message = build_lookup_message("emergency.monitor.main", 4)

        failures = broadcast_message([failing_connection, healthy_connection], message)

        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0][0], ("127.0.0.1", 5001))
        self.assertIsInstance(failures[0][1], OSError)
        self.assertEqual(healthy_socket.sent_data, [encode_message(message)])

    @mock.patch("network_layer.socket.socket")
    def test_create_server_socket_configures_listener(self, mock_socket: mock.Mock) -> None:
        fake_server_socket = FakeSocket()
        mock_socket.return_value = fake_server_socket

        server_socket = create_server_socket("127.0.0.1", 9000, backlog=5, timeout=2.0)

        self.assertIs(server_socket, fake_server_socket)
        self.assertEqual(fake_server_socket.bound_address, ("127.0.0.1", 9000))
        self.assertEqual(fake_server_socket.listen_backlog, 5)
        self.assertEqual(fake_server_socket.timeout, 2.0)
        self.assertIn((socket.SOL_SOCKET, socket.SO_REUSEADDR, 1), fake_server_socket.socket_options)

    @mock.patch("network_layer.socket.create_connection")
    def test_create_client_connection_wraps_socket(self, mock_create_connection: mock.Mock) -> None:
        fake_client_socket = FakeSocket()
        mock_create_connection.return_value = fake_client_socket

        connection = create_client_connection("127.0.0.1", 9000, timeout=3.0)

        self.assertIsInstance(connection, MessageConnection)
        self.assertIs(connection.socket, fake_client_socket)
        self.assertEqual(connection.address, ("127.0.0.1", 9000))
        self.assertEqual(fake_client_socket.timeout, 3.0)

    def test_accept_connection_wraps_accepted_client(self) -> None:
        fake_server_socket = FakeSocket()
        accepted_socket = FakeSocket()
        fake_server_socket.accept_queue.append((accepted_socket, ("127.0.0.1", 9001)))

        connection = accept_connection(fake_server_socket, timeout=4.0)

        self.assertIsInstance(connection, MessageConnection)
        self.assertIs(connection.socket, accepted_socket)
        self.assertEqual(connection.address, ("127.0.0.1", 9001))
        self.assertEqual(accepted_socket.timeout, 4.0)


if __name__ == "__main__":
    unittest.main()
