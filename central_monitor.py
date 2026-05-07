"""Central Monitoring server for the DEAN project."""

from __future__ import annotations

import argparse
import heapq
import socket
import threading
from typing import Any, Mapping

from config import (
    CENTRAL_MONITOR_HOST,
    CENTRAL_MONITOR_LOGICAL_NAME,
    CENTRAL_MONITOR_PORT,
    STATUS_BROADCAST_INTERVAL_SECONDS,
    naming_server_address,
)
from lamport_clock import LamportClock, alert_priority_key
from message_protocol import (
    ALERT,
    HEARTBEAT,
    build_ack_message,
    build_emergency_sequence_message,
    build_error_message,
    build_register_message,
    build_status_update_message,
)
from network_layer import (
    ConnectionClosedError,
    MessageConnection,
    accept_connection,
    broadcast_message,
    create_client_connection,
    create_server_socket,
)
from utils import configure_logging, next_alert_id


class CentralMonitor:
    """TCP server that receives sensor alerts and broadcasts ordered sequences."""

    def __init__(
        self,
        host: str = CENTRAL_MONITOR_HOST,
        port: int = CENTRAL_MONITOR_PORT,
        logical_name: str = CENTRAL_MONITOR_LOGICAL_NAME,
        naming_host: str | None = None,
        naming_port: int | None = None,
        *,
        clock: LamportClock | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.logical_name = logical_name
        self.naming_address: tuple[str, int] = (
            naming_host or naming_server_address()[0],
            naming_port or naming_server_address()[1],
        )
        self.clock = clock or LamportClock()
        self.logger = configure_logging("DEAN.CentralMonitor")
        self._shutdown_event = threading.Event()
        self._server_socket: socket.socket | None = None

        self._sensors: set[MessageConnection] = set()
        self._sensors_lock = threading.Lock()

        self._event_queue: list[tuple[tuple[int, str], dict[str, Any]]] = []
        self._queue_lock = threading.Lock()

        self._global_log: list[dict[str, Any]] = []
        self._log_lock = threading.Lock()

        self._client_threads: set[threading.Thread] = set()
        self._client_threads_lock = threading.Lock()

        self._status_thread: threading.Thread | None = None

    def start(self) -> None:
        """Register with the Naming Server and begin accepting sensor connections."""
        self._register_with_naming_server()
        self._server_socket = create_server_socket(self.host, self.port)
        self.logger.info("Central Monitor listening on %s:%s", self.host, self.port)

        self._status_thread = threading.Thread(
            target=self._status_broadcast_loop,
            daemon=True,
            name="status-broadcaster",
        )
        self._status_thread.start()

        try:
            while not self._shutdown_event.is_set():
                try:
                    connection = accept_connection(self._server_socket)
                except socket.timeout:
                    continue
                except OSError:
                    if self._shutdown_event.is_set():
                        break
                    raise

                self.logger.info("Sensor connected from %s", connection.address)

                worker = threading.Thread(
                    target=self._serve_sensor,
                    args=(connection,),
                    daemon=True,
                    name=f"sensor-{connection.address}",
                )
                with self._client_threads_lock:
                    self._client_threads.add(worker)
                worker.start()
        finally:
            self.shutdown()
            self._join_client_threads()
            self.logger.info("Central Monitor stopped.")

    def shutdown(self) -> None:
        """Signal shutdown and close the server socket."""
        self._shutdown_event.set()
        if self._server_socket is not None:
            try:
                self._server_socket.close()
            except OSError:
                pass
            finally:
                self._server_socket = None

    def _register_with_naming_server(self) -> None:
        """Register this monitor's logical name with the Naming Server."""
        try:
            with create_client_connection(*self.naming_address) as conn:
                register_msg = build_register_message(
                    self.logical_name,
                    self.host,
                    self.port,
                    self.clock.send_event(),
                )
                conn.send_message(register_msg)
                response = conn.receive_message()
                self.clock.receive_event(
                    response.get("timestamp", response.get("lamport_timestamp", 0))
                )
                self.logger.info(
                    "Registered with Naming Server: %s -> %s:%s (%s)",
                    self.logical_name,
                    self.host,
                    self.port,
                    response.get("status", "unknown"),
                )
        except (OSError, ConnectionClosedError) as exc:
            self.logger.warning("Failed to register with Naming Server: %s", exc)

    def _serve_sensor(self, connection: MessageConnection) -> None:
        """Handle messages from a single sensor connection."""
        with self._sensors_lock:
            self._sensors.add(connection)

        try:
            with connection:
                while not self._shutdown_event.is_set():
                    try:
                        message = connection.receive_message()
                    except socket.timeout:
                        continue
                    except ConnectionClosedError:
                        break
                    except Exception as exc:
                        self.logger.warning(
                            "Protocol error from %s: %s", connection.address, exc
                        )
                        try:
                            error_msg = build_error_message(
                                str(exc),
                                self.clock.local_event(),
                                details="protocol_error",
                            )
                            connection.send_message(error_msg)
                        except (OSError, ConnectionClosedError):
                            pass
                        break

                    self._handle_message(message, connection)
        except OSError as exc:
            self.logger.warning("Connection error from %s: %s", connection.address, exc)
        finally:
            with self._sensors_lock:
                self._sensors.discard(connection)
            current = threading.current_thread()
            with self._client_threads_lock:
                self._client_threads.discard(current)
            self.logger.info("Sensor disconnected: %s", connection.address)

    def _handle_message(
        self, message: Mapping[str, Any], connection: MessageConnection
    ) -> None:
        """Dispatch a sensor message to the appropriate handler."""
        msg_type = message.get("type")

        if msg_type == ALERT:
            self._handle_alert(message, connection)
        elif msg_type == HEARTBEAT:
            self._handle_heartbeat(message)

    def _handle_alert(
        self, alert: Mapping[str, Any], connection: MessageConnection
    ) -> None:
        """Process an incoming ALERT and broadcast the ordered sequence."""
        self.clock.receive_event(alert["lamport_timestamp"])

        alert_copy = dict(alert)
        alert_copy["alert_id"] = next_alert_id()

        with self._queue_lock:
            heapq.heappush(self._event_queue, (alert_priority_key(alert_copy), alert_copy))

        with self._log_lock:
            self._global_log.append(alert_copy)

        self.logger.info(
            "Alert from %s at %s (Lamport=%s)",
            alert_copy["sensor_id"],
            alert_copy["location"],
            alert_copy["lamport_timestamp"],
        )

        # Send ACK
        ack = build_ack_message(
            alert_copy["alert_id"],
            alert_copy["sensor_id"],
            "received",
            self.clock.send_event(),
        )
        try:
            connection.send_message(ack)
        except (OSError, ConnectionClosedError) as exc:
            self.logger.warning("Failed to send ACK to %s: %s", connection.address, exc)

        # Broadcast emergency sequence
        self._broadcast_emergency_sequence()

    def _handle_heartbeat(self, heartbeat: Mapping[str, Any]) -> None:
        """Process a HEARTBEAT message."""
        self.clock.receive_event(heartbeat["lamport_timestamp"])
        self.logger.debug(
            "Heartbeat from %s (Lamport=%s)",
            heartbeat["sensor_id"],
            heartbeat["lamport_timestamp"],
        )

    def _broadcast_emergency_sequence(self) -> None:
        """Broadcast the current ordered alert sequence to all sensors."""
        with self._queue_lock:
            ordered = [
                {
                    "order": idx + 1,
                    "sensor_id": alert["sensor_id"],
                    "location": alert["location"],
                    "lamport_timestamp": alert["lamport_timestamp"],
                    "severity": alert["severity"],
                }
                for idx, (_, alert) in enumerate(
                    sorted(self._event_queue, key=lambda item: item[0])
                )
            ]

        if not ordered:
            return

        message = build_emergency_sequence_message(
            sequence=ordered,
            lamport_timestamp=self.clock.send_event(),
            message=f"Emergency sequence contains {len(ordered)} alert(s).",
        )

        with self._sensors_lock:
            sensors = list(self._sensors)

        failures = broadcast_message(sensors, message)
        for address, exc in failures:
            self.logger.warning(
                "Failed to broadcast sequence to %s: %s", address, exc
            )

    def _status_broadcast_loop(self) -> None:
        """Periodically broadcast STATUS_UPDATE to all connected sensors."""
        while not self._shutdown_event.is_set():
            self._shutdown_event.wait(STATUS_BROADCAST_INTERVAL_SECONDS)
            if self._shutdown_event.is_set():
                break
            self._broadcast_status()

    def _broadcast_status(self) -> None:
        """Broadcast a STATUS_UPDATE message to all sensors."""
        with self._sensors_lock:
            active = len(self._sensors)
        with self._queue_lock:
            pending = len(self._event_queue)

        message = build_status_update_message(
            active_sensors=active,
            pending_alerts=pending,
            lamport_timestamp=self.clock.send_event(),
        )

        with self._sensors_lock:
            sensors = list(self._sensors)

        failures = broadcast_message(sensors, message)
        for address, exc in failures:
            self.logger.warning(
                "Failed to broadcast status to %s: %s", address, exc
            )

    def _join_client_threads(self) -> None:
        """Wait for client handler threads to finish."""
        with self._client_threads_lock:
            threads = list(self._client_threads)

        for worker in threads:
            worker.join(timeout=0.5)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the DEAN Central Monitoring server."
    )
    parser.add_argument(
        "--host", default=CENTRAL_MONITOR_HOST, help="Host/IP address to bind."
    )
    parser.add_argument(
        "--port", type=int, default=CENTRAL_MONITOR_PORT, help="TCP port to listen on."
    )
    parser.add_argument("--naming-host", default=None, help="Naming Server host.")
    parser.add_argument(
        "--naming-port", type=int, default=None, help="Naming Server port."
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    monitor = CentralMonitor(
        host=args.host,
        port=args.port,
        naming_host=args.naming_host,
        naming_port=args.naming_port,
    )

    try:
        monitor.start()
    except KeyboardInterrupt:
        monitor.logger.info(
            "Keyboard interrupt received. Shutting down Central Monitor."
        )
        monitor.shutdown()


if __name__ == "__main__":
    main()
