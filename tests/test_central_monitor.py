from __future__ import annotations

import socket
import unittest
from unittest import mock

from central_monitor import CentralMonitor
from lamport_clock import LamportClock
from message_protocol import (
    ACK,
    ALERT,
    EMERGENCY_SEQUENCE,
    HEARTBEAT,
    STATUS_UPDATE,
    build_alert_message,
    build_heartbeat_message,
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


class CentralMonitorTests(unittest.TestCase):
    def test_init_defaults(self) -> None:
        monitor = CentralMonitor()
        self.assertEqual(monitor.host, "127.0.0.1")
        self.assertEqual(monitor.port, 9000)
        self.assertEqual(monitor.logical_name, "emergency.monitor.main")
        self.assertIsInstance(monitor.clock, LamportClock)

    def test_handle_alert_updates_clock_queue_and_log(self) -> None:
        monitor = CentralMonitor()
        initial_time = monitor.clock.time
        alert = build_alert_message("sensor_01", "Room 101", "critical", 5, "Fire!")

        fake_socket = FakeSocket()
        conn = MessageConnection(fake_socket, address=("127.0.0.1", 5001), timeout=1.0)

        monitor._handle_alert(alert, conn)

        self.assertGreater(monitor.clock.time, initial_time)
        self.assertEqual(len(monitor._event_queue), 1)
        self.assertEqual(len(monitor._global_log), 1)
        self.assertTrue(monitor._global_log[0]["alert_id"].startswith("alert_"))

    def test_handle_alert_sends_ack(self) -> None:
        monitor = CentralMonitor()
        alert = build_alert_message("sensor_01", "Room 101", "critical", 5, "Fire!")

        fake_socket = FakeSocket()
        conn = MessageConnection(fake_socket, address=("127.0.0.1", 5001), timeout=1.0)

        with monitor._sensors_lock:
            monitor._sensors.add(conn)

        monitor._handle_alert(alert, conn)

        self.assertEqual(len(fake_socket.sent_data), 2)  # ACK + EMERGENCY_SEQUENCE
        # First message should be ACK
        import json

        ack = json.loads(fake_socket.sent_data[0].decode("utf-8").strip())
        self.assertEqual(ack["type"], ACK)
        self.assertEqual(ack["sensor_id"], "sensor_01")
        self.assertEqual(ack["status"], "received")

    def test_handle_alert_broadcasts_emergency_sequence(self) -> None:
        monitor = CentralMonitor()
        alert = build_alert_message("sensor_01", "Room 101", "critical", 5, "Fire!")

        fake_socket = FakeSocket()
        conn = MessageConnection(fake_socket, address=("127.0.0.1", 5001), timeout=1.0)

        with monitor._sensors_lock:
            monitor._sensors.add(conn)

        monitor._handle_alert(alert, conn)

        import json

        # Second message should be EMERGENCY_SEQUENCE
        seq = json.loads(fake_socket.sent_data[1].decode("utf-8").strip())
        self.assertEqual(seq["type"], EMERGENCY_SEQUENCE)
        self.assertEqual(len(seq["sequence"]), 1)
        self.assertEqual(seq["sequence"][0]["sensor_id"], "sensor_01")
        self.assertEqual(seq["sequence"][0]["order"], 1)

    def test_multiple_alerts_ordered_by_lamport(self) -> None:
        monitor = CentralMonitor()
        alert1 = build_alert_message("sensor_01", "Room 101", "critical", 10, "Fire!")
        alert2 = build_alert_message("sensor_02", "Room 102", "critical", 5, "Fire!")

        fake_socket = FakeSocket()
        conn = MessageConnection(fake_socket, address=("127.0.0.1", 5001), timeout=1.0)

        with monitor._sensors_lock:
            monitor._sensors.add(conn)

        monitor._handle_alert(alert1, conn)
        monitor._handle_alert(alert2, conn)

        import json

        # The last broadcast should have sensor_02 first (lower Lamport timestamp)
        seq = json.loads(fake_socket.sent_data[-1].decode("utf-8").strip())
        self.assertEqual(seq["sequence"][0]["sensor_id"], "sensor_02")
        self.assertEqual(seq["sequence"][1]["sensor_id"], "sensor_01")

    def test_handle_heartbeat_updates_clock(self) -> None:
        monitor = CentralMonitor()
        initial_time = monitor.clock.time
        heartbeat = build_heartbeat_message("sensor_01", 10)

        monitor._handle_heartbeat(heartbeat)

        self.assertGreater(monitor.clock.time, initial_time)

    def test_broadcast_status_sends_to_all_sensors(self) -> None:
        monitor = CentralMonitor()
        fake_socket1 = FakeSocket()
        fake_socket2 = FakeSocket()
        conn1 = MessageConnection(
            fake_socket1, address=("127.0.0.1", 5001), timeout=1.0
        )
        conn2 = MessageConnection(
            fake_socket2, address=("127.0.0.1", 5002), timeout=1.0
        )

        with monitor._sensors_lock:
            monitor._sensors.add(conn1)
            monitor._sensors.add(conn2)

        monitor._broadcast_status()

        self.assertEqual(len(fake_socket1.sent_data), 1)
        self.assertEqual(len(fake_socket2.sent_data), 1)

        import json

        status1 = json.loads(fake_socket1.sent_data[0].decode("utf-8").strip())
        self.assertEqual(status1["type"], STATUS_UPDATE)
        self.assertEqual(status1["active_sensors"], 2)
        self.assertEqual(status1["pending_alerts"], 0)

    def test_sensor_connection_tracking(self) -> None:
        monitor = CentralMonitor()
        fake_socket = FakeSocket(recv_chunks=[b""])
        conn = MessageConnection(fake_socket, address=("127.0.0.1", 5001), timeout=1.0)

        with monitor._sensors_lock:
            self.assertEqual(len(monitor._sensors), 0)

        # _serve_sensor should add then remove the connection
        monitor._serve_sensor(conn)

        with monitor._sensors_lock:
            self.assertEqual(len(monitor._sensors), 0)

    def test_shutdown_sets_event_and_closes_socket(self) -> None:
        monitor = CentralMonitor()
        fake_socket = FakeSocket()
        monitor._server_socket = fake_socket

        monitor.shutdown()

        self.assertTrue(monitor._shutdown_event.is_set())
        self.assertTrue(fake_socket.closed)
        self.assertIsNone(monitor._server_socket)

    @mock.patch("central_monitor.create_client_connection")
    def test_register_with_naming_server(self, mock_create: mock.Mock) -> None:
        monitor = CentralMonitor()
        fake_naming_socket = FakeSocket(
            recv_chunks=[
                b'{"type":"REGISTER_RESPONSE","logical_name":"emergency.monitor.main","ip":"127.0.0.1","port":9000,"status":"registered","timestamp":5}\n'
            ]
        )
        fake_conn = MessageConnection(fake_naming_socket, timeout=1.0)
        mock_create.return_value = fake_conn

        monitor._register_with_naming_server()

        self.assertEqual(len(fake_naming_socket.sent_data), 1)
        import json

        msg = json.loads(fake_naming_socket.sent_data[0].decode("utf-8").strip())
        self.assertEqual(msg["type"], "REGISTER")
        self.assertEqual(msg["logical_name"], "emergency.monitor.main")

    def test_handle_message_dispatches_alert(self) -> None:
        monitor = CentralMonitor()
        alert = build_alert_message("sensor_01", "Room 101", "critical", 5, "Fire!")

        fake_socket = FakeSocket()
        conn = MessageConnection(fake_socket, address=("127.0.0.1", 5001), timeout=1.0)

        monitor._handle_message(alert, conn)

        self.assertEqual(len(monitor._event_queue), 1)

    def test_handle_message_dispatches_heartbeat(self) -> None:
        monitor = CentralMonitor()
        initial_time = monitor.clock.time
        heartbeat = build_heartbeat_message("sensor_01", 10)

        fake_socket = FakeSocket()
        conn = MessageConnection(fake_socket, address=("127.0.0.1", 5001), timeout=1.0)

        monitor._handle_message(heartbeat, conn)

        self.assertGreater(monitor.clock.time, initial_time)

    def test_broadcast_skips_when_queue_empty(self) -> None:
        monitor = CentralMonitor()
        fake_socket = FakeSocket()
        conn = MessageConnection(fake_socket, address=("127.0.0.1", 5001), timeout=1.0)

        with monitor._sensors_lock:
            monitor._sensors.add(conn)

        monitor._broadcast_emergency_sequence()

        self.assertEqual(len(fake_socket.sent_data), 0)


if __name__ == "__main__":
    unittest.main()
