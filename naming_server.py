"""Naming Server implementation for the DEAN project."""

from __future__ import annotations

import argparse
import socket
import threading
from dataclasses import dataclass
from typing import Any, Mapping

from config import NAMING_SERVER_HOST, NAMING_SERVER_PORT
from lamport_clock import LamportClock
from message_protocol import (
    DEREGISTER,
    LOOKUP,
    REGISTER,
    ProtocolError,
    build_deregister_response,
    build_error_message,
    build_lookup_response,
    build_register_response,
)
from network_layer import ConnectionClosedError, MessageConnection, accept_connection, create_server_socket
from utils import configure_logging


@dataclass(frozen=True, slots=True)
class RegistryEntry:
    logical_name: str
    ip: str
    port: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "logical_name": self.logical_name,
            "ip": self.ip,
            "port": self.port,
        }


class NamingRegistry:
    """Thread-safe logical-name registry for the Naming Server."""

    def __init__(self) -> None:
        self._entries: dict[str, RegistryEntry] = {}
        self._lock = threading.Lock()

    def register(self, logical_name: str, ip: str, port: int) -> tuple[RegistryEntry, bool]:
        self._validate_logical_name(logical_name)
        self._validate_endpoint(ip, port)

        with self._lock:
            updated = logical_name in self._entries
            entry = RegistryEntry(logical_name=logical_name, ip=ip, port=port)
            self._entries[logical_name] = entry
            return entry, updated

    def lookup(self, logical_name: str) -> RegistryEntry | None:
        self._validate_logical_name(logical_name)

        with self._lock:
            return self._entries.get(logical_name)

    def deregister(self, logical_name: str) -> RegistryEntry | None:
        self._validate_logical_name(logical_name)

        with self._lock:
            return self._entries.pop(logical_name, None)

    def snapshot(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return {
                logical_name: entry.to_dict()
                for logical_name, entry in sorted(self._entries.items())
            }

    @staticmethod
    def _validate_logical_name(logical_name: str) -> None:
        if not isinstance(logical_name, str) or not logical_name.strip():
            raise ValueError("logical_name must be a non-empty string.")

    @staticmethod
    def _validate_endpoint(ip: str, port: int) -> None:
        if not isinstance(ip, str) or not ip.strip():
            raise ValueError("ip must be a non-empty string.")
        if not isinstance(port, int) or not 1 <= port <= 65535:
            raise ValueError("port must be an integer between 1 and 65535.")


class NamingServer:
    """TCP Naming Server for registering and resolving logical names."""

    def __init__(
        self,
        host: str = NAMING_SERVER_HOST,
        port: int = NAMING_SERVER_PORT,
        *,
        registry: NamingRegistry | None = None,
        clock: LamportClock | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.registry = registry or NamingRegistry()
        self.clock = clock or LamportClock()
        self.logger = configure_logging("DEAN.NamingServer")
        self._shutdown_event = threading.Event()
        self._server_socket: socket.socket | None = None
        self._client_threads: set[threading.Thread] = set()
        self._client_threads_lock = threading.Lock()

    def serve_forever(self) -> None:
        self._server_socket = create_server_socket(self.host, self.port)
        self.logger.info("Naming Server listening on %s:%s", self.host, self.port)

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

                worker = threading.Thread(
                    target=self._serve_connection,
                    args=(connection,),
                    daemon=True,
                    name=f"naming-client-{connection.address}",
                )
                with self._client_threads_lock:
                    self._client_threads.add(worker)
                worker.start()
        finally:
            self.shutdown()
            self._join_client_threads()
            self.logger.info("Naming Server stopped.")

    def shutdown(self) -> None:
        self._shutdown_event.set()
        if self._server_socket is not None:
            try:
                self._server_socket.close()
            except OSError:
                pass
            finally:
                self._server_socket = None

    def handle_request(self, request: Mapping[str, Any]) -> dict[str, Any]:
        request_type = request.get("type")
        self.clock.receive_event(self._extract_timestamp(request))

        try:
            if request_type == REGISTER:
                entry, updated = self.registry.register(
                    request["logical_name"],
                    request["ip"],
                    request["port"],
                )
                response_timestamp = self.clock.send_event()
                status = "updated" if updated else "registered"
                self.logger.info(
                    "Registered %s -> %s:%s (%s)",
                    entry.logical_name,
                    entry.ip,
                    entry.port,
                    status,
                )
                return build_register_response(
                    entry.logical_name,
                    entry.ip,
                    entry.port,
                    status,
                    response_timestamp,
                )

            if request_type == LOOKUP:
                logical_name = request["logical_name"]
                entry = self.registry.lookup(logical_name)
                if entry is None:
                    return self._error_response(
                        f"Logical name '{logical_name}' is not registered.",
                        details="lookup_miss",
                    )

                response_timestamp = self.clock.send_event()
                self.logger.info("Resolved %s -> %s:%s", entry.logical_name, entry.ip, entry.port)
                return build_lookup_response(
                    entry.logical_name,
                    entry.ip,
                    entry.port,
                    response_timestamp,
                )

            if request_type == DEREGISTER:
                logical_name = request["logical_name"]
                removed_entry = self.registry.deregister(logical_name)
                if removed_entry is None:
                    return self._error_response(
                        f"Logical name '{logical_name}' is not registered.",
                        details="deregister_miss",
                    )

                response_timestamp = self.clock.send_event()
                self.logger.info("Deregistered %s", removed_entry.logical_name)
                return build_deregister_response(
                    removed_entry.logical_name,
                    "deregistered",
                    response_timestamp,
                )
        except ValueError as exc:
            return self._error_response(str(exc), details="invalid_registry_request")

        return self._error_response(
            f"Naming Server does not handle message type '{request_type}'.",
            details="unsupported_message_type",
        )

    def _serve_connection(self, connection: MessageConnection) -> None:
        try:
            with connection:
                while not self._shutdown_event.is_set():
                    try:
                        request = connection.receive_message()
                    except socket.timeout:
                        continue
                    except ConnectionClosedError:
                        break
                    except ProtocolError as exc:
                        connection.send_message(
                            self._error_response(
                                str(exc),
                                details="protocol_error",
                                advance_clock_without_receive=True,
                            )
                        )
                        break

                    response = self.handle_request(request)
                    connection.send_message(response)
        except OSError as exc:
            self.logger.warning("Connection error: %s", exc)
        finally:
            current = threading.current_thread()
            with self._client_threads_lock:
                self._client_threads.discard(current)

    def _error_response(
        self,
        error: str,
        *,
        details: str,
        advance_clock_without_receive: bool = False,
    ) -> dict[str, Any]:
        if advance_clock_without_receive:
            response_timestamp = self.clock.local_event()
        else:
            response_timestamp = self.clock.send_event()
        self.logger.warning("%s (%s)", error, details)
        return build_error_message(error, response_timestamp, details=details)

    @staticmethod
    def _extract_timestamp(request: Mapping[str, Any]) -> int:
        for key in ("timestamp", "lamport_timestamp"):
            value = request.get(key)
            if isinstance(value, int):
                return value
        return 0

    def _join_client_threads(self) -> None:
        with self._client_threads_lock:
            threads = list(self._client_threads)

        for worker in threads:
            worker.join(timeout=0.2)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the DEAN Naming Server.")
    parser.add_argument("--host", default=NAMING_SERVER_HOST, help="Host/IP address to bind.")
    parser.add_argument(
        "--port",
        type=int,
        default=NAMING_SERVER_PORT,
        help="TCP port to listen on.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    server = NamingServer(host=args.host, port=args.port)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.logger.info("Keyboard interrupt received. Shutting down Naming Server.")
        server.shutdown()


if __name__ == "__main__":
    main()
