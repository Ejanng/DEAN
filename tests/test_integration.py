"""Integration tests for the DEAN project."""

from __future__ import annotations

import threading
import time
import unittest
from unittest import mock

from auction_server import CentralMonitor
from lamport_clock import LamportClock
from message_protocol import build_alert_message, build_heartbeat_message
from network_layer import create_client_connection
from bidder_client import SensorClient


class IntegrationTests(unittest.TestCase):
    """End-to-end tests using real TCP sockets."""

    def _start_monitor(self, port: int = 0) -> CentralMonitor:
        """Start a CentralMonitor in a background thread on an ephemeral port."""
        monitor = CentralMonitor(port=port)
        with mock.patch.object(monitor, "_register_with_naming_server"):
            monitor_thread = threading.Thread(target=monitor.start, daemon=True)
            monitor_thread.start()
            for _ in range(100):
                if monitor._server_socket is not None:
                    break
                time.sleep(0.01)
            else:
                raise RuntimeError("Monitor failed to start")
        self.addCleanup(monitor.shutdown)
        self.addCleanup(monitor_thread.join, 1.0)
        return monitor

    def _connect_client(self, monitor: CentralMonitor):
        """Return a MessageConnection to the running monitor."""
        address = monitor._server_socket.getsockname()
        return create_client_connection(*address)

    def test_alert_to_ack_and_emergency_sequence(self) -> None:
        """A single alert yields an ACK and an EMERGENCY_SEQUENCE broadcast."""
        monitor = self._start_monitor()
        conn = self._connect_client(monitor)
        self.addCleanup(conn.close)

        clock = LamportClock()
        alert = build_alert_message(
            "sensor_01", "Room 101", "critical", clock.send_event(), "Fire!"
        )
        conn.send_message(alert)

        ack = conn.receive_message()
        self.assertEqual(ack["type"], "ACK")
        self.assertEqual(ack["sensor_id"], "sensor_01")

        seq = conn.receive_message()
        self.assertEqual(seq["type"], "EMERGENCY_SEQUENCE")
        self.assertEqual(len(seq["sequence"]), 1)
        self.assertEqual(seq["sequence"][0]["sensor_id"], "sensor_01")

        self.assertEqual(len(monitor._event_queue), 1)

    def test_multiple_alerts_ordered_by_lamport(self) -> None:
        """Two alerts with different Lamport timestamps are ordered correctly."""
        monitor = self._start_monitor()
        conn1 = self._connect_client(monitor)

        # Sensor 1 sends an alert with timestamp 10
        clock1 = LamportClock(initial_time=9)
        alert1 = build_alert_message(
            "sensor_01", "Room 101", "critical", clock1.send_event(), "Fire 1!"
        )
        conn1.send_message(alert1)
        conn1.receive_message()  # ACK
        conn1.receive_message()  # SEQ([alert1])

        # Connect sensor 2 after the first alert is processed
        conn2 = self._connect_client(monitor)
        self.addCleanup(conn1.close)
        self.addCleanup(conn2.close)

        # Sensor 2 sends an alert with timestamp 5
        clock2 = LamportClock(initial_time=4)
        alert2 = build_alert_message(
            "sensor_02", "Room 102", "critical", clock2.send_event(), "Fire 2!"
        )
        conn2.send_message(alert2)
        conn2.receive_message()  # ACK

        seq2 = conn2.receive_message()  # SEQ([alert2, alert1])
        seq1 = conn1.receive_message()  # SEQ([alert2, alert1])

        self.assertEqual(seq1["sequence"][0]["sensor_id"], "sensor_02")
        self.assertEqual(seq1["sequence"][1]["sensor_id"], "sensor_01")
        self.assertEqual(seq2["sequence"][0]["sensor_id"], "sensor_02")
        self.assertEqual(seq2["sequence"][1]["sensor_id"], "sensor_01")

        with monitor._queue_lock:
            ordered = [
                alert for _, alert in sorted(monitor._event_queue, key=lambda x: x[0])
            ]
        self.assertEqual(ordered[0]["sensor_id"], "sensor_02")
        self.assertEqual(ordered[1]["sensor_id"], "sensor_01")

    def test_sensor_client_sends_alert(self) -> None:
        """A SensorClient can send an alert that lands in the monitor queue."""
        monitor = self._start_monitor()
        address = monitor._server_socket.getsockname()

        sensor = SensorClient("sensor_01", "Room 101")
        sensor.connection = create_client_connection(*address)
        self.addCleanup(sensor.shutdown)

        sensor._send_alert("Integration test fire")

        time.sleep(0.1)
        self.assertEqual(len(monitor._event_queue), 1)
        self.assertEqual(monitor._event_queue[0][1]["sensor_id"], "sensor_01")

    def test_heartbeat_processed_without_error(self) -> None:
        """A HEARTBEAT is accepted by the monitor without crashing."""
        monitor = self._start_monitor()
        conn = self._connect_client(monitor)
        self.addCleanup(conn.close)

        clock = LamportClock()
        heartbeat = build_heartbeat_message("sensor_01", clock.send_event())
        conn.send_message(heartbeat)

        time.sleep(0.1)
        self.assertEqual(len(monitor._event_queue), 0)

    def test_status_broadcast_reaches_multiple_clients(self) -> None:
        """STATUS_UPDATE is delivered to every connected sensor."""
        monitor = self._start_monitor()
        conn1 = self._connect_client(monitor)
        conn2 = self._connect_client(monitor)
        self.addCleanup(conn1.close)
        self.addCleanup(conn2.close)

        # Allow the monitor's accept threads to add both connections
        time.sleep(0.1)

        monitor._broadcast_status()

        status1 = conn1.receive_message()
        status2 = conn2.receive_message()

        self.assertEqual(status1["type"], "STATUS_UPDATE")
        self.assertEqual(status2["type"], "STATUS_UPDATE")
        self.assertEqual(status1["active_sensors"], 2)
        self.assertEqual(status2["active_sensors"], 2)


if __name__ == "__main__":
    unittest.main()
