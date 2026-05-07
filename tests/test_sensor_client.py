from __future__ import annotations

import json
import unittest
from unittest import mock

from sensor_client import SensorClient
from lamport_clock import LamportClock
from message_protocol import (
    ACK,
    EMERGENCY_SEQUENCE,
    HEARTBEAT,
    STATUS_UPDATE,
    build_ack_message,
    build_emergency_sequence_message,
    build_status_update_message,
    encode_message,
)
from network_layer import MessageConnection


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
        self.closed = False
        self.timeout: float | None = None

    def settimeout(self, timeout: float | None) -> None:
        self.timeout = timeout

    def sendall(self, data: bytes) -> None:
        if self.send_error:
            raise self.send_error
        self.sent_data.append(data)

    def recv(self, buffer_size: int) -> bytes:
        if self.recv_chunks:
            return self.recv_chunks.pop(0)
        return b""

    def shutdown(self, how: int) -> None:
        pass

    def close(self) -> None:
        self.closed = True

    def fileno(self) -> int:
        return 123


class SensorClientTests(unittest.TestCase):
    def test_init_defaults(self) -> None:
        client = SensorClient("sensor_01", "Room 101")
        self.assertEqual(client.sensor_id, "sensor_01")
        self.assertEqual(client.location, "Room 101")
        self.assertFalse(client.lag)
        self.assertIsInstance(client.clock, LamportClock)

    @mock.patch("sensor_client.create_client_connection")
    def test_lookup_monitor_address(self, mock_create: mock.Mock) -> None:
        lookup_response = encode_message(
            {
                "type": "LOOKUP_RESPONSE",
                "logical_name": "emergency.monitor.main",
                "ip": "127.0.0.1",
                "port": 9000,
                "timestamp": 5,
            }
        )
        fake_socket = FakeSocket(recv_chunks=[lookup_response])
        fake_conn = MessageConnection(fake_socket, timeout=1.0)
        mock_create.return_value = fake_conn

        client = SensorClient("sensor_01", "Room 101")
        address = client._lookup_monitor_address()

        self.assertEqual(address, ("127.0.0.1", 9000))
        self.assertGreater(client.clock.time, 0)

    @mock.patch("sensor_client.create_client_connection")
    def test_connect_to_monitor(self, mock_create: mock.Mock) -> None:
        fake_socket = FakeSocket()
        fake_conn = MessageConnection(
            fake_socket, address=("127.0.0.1", 9000), timeout=1.0
        )
        mock_create.return_value = fake_conn

        client = SensorClient("sensor_01", "Room 101")
        conn = client._connect_to_monitor(("127.0.0.1", 9000))

        self.assertIs(conn, fake_conn)

    def test_send_alert_updates_clock_and_sends_message(self) -> None:
        fake_socket = FakeSocket()
        conn = MessageConnection(
            fake_socket, address=("127.0.0.1", 9000), timeout=1.0
        )

        client = SensorClient("sensor_01", "Room 101")
        client.connection = conn
        initial_time = client.clock.time

        client._send_alert("Fire detected!")

        self.assertGreater(client.clock.time, initial_time)
        self.assertEqual(len(fake_socket.sent_data), 1)

        msg = json.loads(fake_socket.sent_data[0].decode("utf-8").strip())
        self.assertEqual(msg["type"], "ALERT")
        self.assertEqual(msg["sensor_id"], "sensor_01")
        self.assertEqual(msg["location"], "Room 101")
        self.assertEqual(msg["payload"], "Fire detected!")

    def test_handle_ack_updates_clock(self) -> None:
        client = SensorClient("sensor_01", "Room 101")
        initial_time = client.clock.time
        ack = build_ack_message("alert_001", "sensor_01", "received", 10)

        client._handle_message(ack)

        self.assertGreater(client.clock.time, initial_time)

    def test_handle_emergency_sequence_updates_clock(self) -> None:
        client = SensorClient("sensor_01", "Room 101")
        initial_time = client.clock.time
        seq = build_emergency_sequence_message(
            sequence=[
                {
                    "order": 1,
                    "sensor_id": "sensor_01",
                    "location": "Room 101",
                    "lamport_timestamp": 5,
                    "severity": "critical",
                }
            ],
            lamport_timestamp=15,
            message="Test sequence",
        )

        client._handle_message(seq)

        self.assertGreater(client.clock.time, initial_time)

    def test_handle_status_update_updates_clock(self) -> None:
        client = SensorClient("sensor_01", "Room 101")
        initial_time = client.clock.time
        status = build_status_update_message(3, 1, 20)

        client._handle_message(status)

        self.assertGreater(client.clock.time, initial_time)

    def test_heartbeat_loop_sends_periodically(self) -> None:
        fake_socket = FakeSocket()
        conn = MessageConnection(
            fake_socket, address=("127.0.0.1", 9000), timeout=1.0
        )

        client = SensorClient("sensor_01", "Room 101")
        client.connection = conn

        mock_event = mock.Mock()
        mock_event.is_set.side_effect = [False, False, True]
        mock_event.wait.side_effect = [False, True]
        client._shutdown_event = mock_event

        client._heartbeat_loop()

        self.assertEqual(len(fake_socket.sent_data), 1)
        msg = json.loads(fake_socket.sent_data[0].decode("utf-8").strip())
        self.assertEqual(msg["type"], HEARTBEAT)
        self.assertEqual(msg["sensor_id"], "sensor_01")

    def test_shutdown_closes_connection(self) -> None:
        fake_socket = FakeSocket()
        conn = MessageConnection(
            fake_socket, address=("127.0.0.1", 9000), timeout=1.0
        )

        client = SensorClient("sensor_01", "Room 101")
        client.connection = conn

        client.shutdown()

        self.assertTrue(fake_socket.closed)
        self.assertTrue(client._shutdown_event.is_set())

    @mock.patch("sensor_client.time.sleep")
    def test_simulated_lag_sleeps_before_send(self, mock_sleep: mock.Mock) -> None:
        fake_socket = FakeSocket()
        conn = MessageConnection(
            fake_socket, address=("127.0.0.1", 9000), timeout=1.0
        )

        client = SensorClient("sensor_01", "Room 101", lag=True)
        client.connection = conn

        client._send_alert()

        mock_sleep.assert_called_once()
        self.assertEqual(len(fake_socket.sent_data), 1)

    def test_receive_loop_handles_messages(self) -> None:
        ack = build_ack_message("alert_001", "sensor_01", "received", 10)
        fake_socket = FakeSocket(recv_chunks=[encode_message(ack), b""])
        conn = MessageConnection(
            fake_socket, address=("127.0.0.1", 9000), timeout=1.0
        )

        client = SensorClient("sensor_01", "Room 101")
        client.connection = conn

        mock_event = mock.Mock()
        mock_event.is_set.side_effect = [False, True]
        client._shutdown_event = mock_event

        client._receive_loop()

        self.assertGreater(client.clock.time, 0)

    @mock.patch("builtins.input", side_effect=["", "q"])
    def test_input_loop_triggers_alert_and_quits(self, mock_input: mock.Mock) -> None:
        fake_socket = FakeSocket()
        conn = MessageConnection(
            fake_socket, address=("127.0.0.1", 9000), timeout=1.0
        )

        client = SensorClient("sensor_01", "Room 101")
        client.connection = conn

        client._input_loop()

        self.assertEqual(len(fake_socket.sent_data), 1)
        msg = json.loads(fake_socket.sent_data[0].decode("utf-8").strip())
        self.assertEqual(msg["type"], "ALERT")

    def test_handle_error_updates_clock(self) -> None:
        client = SensorClient("sensor_01", "Room 101")
        initial_time = client.clock.time
        error_msg = {
            "type": "ERROR",
            "error": "Something went wrong",
            "timestamp": 7,
        }

        client._handle_message(error_msg)

        self.assertGreater(client.clock.time, initial_time)


if __name__ == "__main__":
    unittest.main()
