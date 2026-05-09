"""Sensor Client for the DEAN project."""

from __future__ import annotations

import argparse
import random
import socket
import threading
import time
from typing import Any, Mapping

from config import (
    CENTRAL_MONITOR_LOGICAL_NAME,
    DEFAULT_ALERT_SEVERITY,
    HEARTBEAT_INTERVAL_SECONDS,
    SIMULATED_LAG_MAX_SECONDS,
    SIMULATED_LAG_MIN_SECONDS,
    naming_server_address,
)
from lamport_clock import LamportClock
from message_protocol import (
    ACK,
    EMERGENCY_SEQUENCE,
    ERROR,
    HEARTBEAT,
    STATUS_UPDATE,
    build_alert_message,
    build_heartbeat_message,
    build_lookup_message,
)
from network_layer import (
    ConnectionClosedError,
    MessageConnection,
    create_client_connection,
)
from utils import configure_logging


class SensorClient:
    """Interactive sensor node that connects to the Central Monitoring server."""

    def __init__(
        self,
        sensor_id: str,
        location: str,
        naming_address: tuple[str, int] | None = None,
        lag: bool = False,
        *,
        clock: LamportClock | None = None,
    ) -> None:
        self.sensor_id = sensor_id
        self.location = location
        self.naming_address: tuple[str, int] = naming_address or naming_server_address()
        self.lag = lag
        self.clock = clock or LamportClock()
        self.logger = configure_logging(f"DEAN.Sensor.{sensor_id}")
        self.connection: MessageConnection | None = None
        self._shutdown_event = threading.Event()
        self._receive_thread: threading.Thread | None = None
        self._heartbeat_thread: threading.Thread | None = None
        self._print_lock = threading.Lock()

    def run(self) -> None:
        """Lookup the monitor, connect, and start the interactive session."""
        try:
            monitor_address = self._lookup_monitor_address()
            self.connection = self._connect_to_monitor(monitor_address)
            self.logger.info("Connected to Central Monitor at %s", monitor_address)

            self._receive_thread = threading.Thread(
                target=self._receive_loop,
                daemon=True,
                name="sensor-receive",
            )
            self._heartbeat_thread = threading.Thread(
                target=self._heartbeat_loop,
                daemon=True,
                name="sensor-heartbeat",
            )
            self._receive_thread.start()
            self._heartbeat_thread.start()

            self._input_loop()
        except (OSError, ConnectionClosedError) as exc:
            self.logger.error("Connection failed: %s", exc)
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        """Signal shutdown and close the monitor connection."""
        self._shutdown_event.set()
        if self.connection is not None:
            try:
                self.connection.close()
            except OSError:
                pass
            finally:
                self.connection = None
        self.logger.info("Sensor %s shutdown complete.", self.sensor_id)

    def _lookup_monitor_address(self) -> tuple[str, int]:
        """Query the Naming Server for the Central Monitor address."""
        with create_client_connection(*self.naming_address) as conn:
            lookup_msg = build_lookup_message(
                CENTRAL_MONITOR_LOGICAL_NAME,
                self.clock.send_event(),
            )
            conn.send_message(lookup_msg)
            response = conn.receive_message()
            self.clock.receive_event(
                response.get("timestamp", response.get("lamport_timestamp", 0))
            )
            return response["ip"], response["port"]

    def _connect_to_monitor(self, address: tuple[str, int]) -> MessageConnection:
        """Open a persistent connection to the Central Monitor."""
        return create_client_connection(*address)

    def _input_loop(self) -> None:
        """Read user commands and trigger alerts."""
        print(f"\nSensor {self.sensor_id} ready at {self.location}.")
        print("Commands: [Enter/f] trigger alert | [q] quit")

        while not self._shutdown_event.is_set():
            try:
                user_input = input(
                    f"[{self.sensor_id} | Lamport={self.clock.time}] > "
                )
            except EOFError:
                break

            command = user_input.strip().lower()
            if command in ("", "f", "fire"):
                self._send_alert()
            elif command == "q":
                break

    def _send_alert(self, payload: str = "Smoke and heat detected") -> None:
        """Build and send an ALERT message to the Central Monitor."""
        if self.lag:
            lag_seconds = random.uniform(
                SIMULATED_LAG_MIN_SECONDS, SIMULATED_LAG_MAX_SECONDS
            )
            self.logger.info("Simulating network lag: %.2fs", lag_seconds)
            time.sleep(lag_seconds)

        if self.connection is None:
            self.logger.warning("Not connected to monitor. Cannot send alert.")
            return

        msg = build_alert_message(
            sensor_id=self.sensor_id,
            location=self.location,
            severity=DEFAULT_ALERT_SEVERITY,
            lamport_timestamp=self.clock.send_event(),
            payload=payload,
        )
        try:
            self.connection.send_message(msg)
            with self._print_lock:
                print(
                    f"\n[ALERT SENT] {payload} at {self.location} "
                    f"(Lamport={msg['lamport_timestamp']})"
                )
        except (OSError, ConnectionClosedError) as exc:
            self.logger.warning("Failed to send alert: %s", exc)

    def _heartbeat_loop(self) -> None:
        """Periodically send HEARTBEAT messages while connected."""
        while not self._shutdown_event.is_set():
            self._shutdown_event.wait(HEARTBEAT_INTERVAL_SECONDS)
            if self._shutdown_event.is_set():
                break
            self._send_heartbeat()

    def _send_heartbeat(self) -> None:
        """Send a single HEARTBEAT message."""
        if self.connection is None:
            return

        msg = build_heartbeat_message(
            self.sensor_id,
            self.clock.send_event(),
        )
        try:
            self.connection.send_message(msg)
            self.logger.debug("Heartbeat sent (Lamport=%s)", msg["lamport_timestamp"])
        except (OSError, ConnectionClosedError) as exc:
            self.logger.warning("Failed to send heartbeat: %s", exc)

    def _receive_loop(self) -> None:
        """Listen for incoming messages from the Central Monitor."""
        if self.connection is None:
            return

        try:
            while not self._shutdown_event.is_set():
                try:
                    message = self.connection.receive_message()
                except socket.timeout:
                    continue
                except ConnectionClosedError:
                    break
                except Exception as exc:
                    self.logger.warning("Receive error: %s", exc)
                    break

                self._handle_message(message)
        except OSError as exc:
            self.logger.warning("Connection error: %s", exc)

    def _handle_message(self, message: Mapping[str, Any]) -> None:
        """Dispatch an incoming message and update the Lamport clock."""
        msg_type = message.get("type")
        timestamp = message.get("lamport_timestamp", message.get("timestamp", 0))
        self.clock.receive_event(timestamp)

        if msg_type == ACK:
            self._handle_ack(message)
        elif msg_type == EMERGENCY_SEQUENCE:
            self._handle_emergency_sequence(message)
        elif msg_type == STATUS_UPDATE:
            self._handle_status_update(message)
        elif msg_type == ERROR:
            self._handle_error(message)

    def _handle_ack(self, message: Mapping[str, Any]) -> None:
        with self._print_lock:
            print(
                f"\n[ACK] Alert {message['alert_id']} {message['status']} "
                f"(Lamport={message['lamport_timestamp']})"
            )

    def _handle_emergency_sequence(self, message: Mapping[str, Any]) -> None:
        with self._print_lock:
            print(f"\n[EMERGENCY SEQUENCE] {message['message']}")
            for item in message["sequence"]:
                print(
                    f"  {item['order']}. {item['sensor_id']} @ {item['location']} "
                    f"(Lamport={item['lamport_timestamp']}, {item['severity']})"
                )

    def _handle_status_update(self, message: Mapping[str, Any]) -> None:
        with self._print_lock:
            print(
                f"\n[STATUS] Sensors: {message['active_sensors']} | "
                f"Pending: {message['pending_alerts']} | "
                f"Status: {message['system_status']} "
                f"(Lamport={message['lamport_timestamp']})"
            )

    def _handle_error(self, message: Mapping[str, Any]) -> None:
        with self._print_lock:
            print(f"\n[ERROR] {message['error']}")
            if "details" in message:
                print(f"  Details: {message['details']}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a DEAN Sensor Client.")
    parser.add_argument("--id", default="sensor_01", help="Sensor identifier.")
    parser.add_argument(
        "--location", default="Building A, Floor 1", help="Sensor location."
    )
    parser.add_argument("--naming-host", default=None, help="Naming Server host.")
    parser.add_argument(
        "--naming-port", type=int, default=None, help="Naming Server port."
    )
    parser.add_argument(
        "--lag",
        action="store_true",
        help="Simulate network lag before sending alerts.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    naming_address: tuple[str, int] = (
        args.naming_host or naming_server_address()[0],
        args.naming_port or naming_server_address()[1],
    )
    client = SensorClient(
        sensor_id=args.id,
        location=args.location,
        naming_address=naming_address,
        lag=args.lag,
    )

    try:
        client.run()
    except KeyboardInterrupt:
        client.logger.info("Keyboard interrupt received. Shutting down sensor.")
        client.shutdown()


if __name__ == "__main__":
    main()
