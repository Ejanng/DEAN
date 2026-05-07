"""Shared configuration for the DEAN project."""

from __future__ import annotations

from pathlib import Path

PROJECT_NAME = "DEAN_Project"
PROJECT_ROOT = Path(__file__).resolve().parent

DEFAULT_HOST = "127.0.0.1"

NAMING_SERVER_HOST = DEFAULT_HOST
NAMING_SERVER_PORT = 8000

CENTRAL_MONITOR_LOGICAL_NAME = "emergency.monitor.main"
CENTRAL_MONITOR_HOST = DEFAULT_HOST
CENTRAL_MONITOR_PORT = 9000

SOCKET_BACKLOG = 20
SOCKET_TIMEOUT_SECONDS = 1.0
NETWORK_BUFFER_SIZE = 4096
JSON_ENCODING = "utf-8"
MESSAGE_DELIMITER = "\n"

HEARTBEAT_INTERVAL_SECONDS = 5.0
STATUS_BROADCAST_INTERVAL_SECONDS = 5.0
CLIENT_RECONNECT_DELAY_SECONDS = 2.0

SIMULATED_LAG_MIN_SECONDS = 0.5
SIMULATED_LAG_MAX_SECONDS = 2.0

DEFAULT_ALERT_SEVERITY = "critical"
DEFAULT_HEARTBEAT_STATUS = "online"
DEFAULT_SYSTEM_STATUS = "normal"

Address = tuple[str, int]


def naming_server_address() -> Address:
    """Return the bind/lookup address for the Naming Server."""

    return NAMING_SERVER_HOST, NAMING_SERVER_PORT


def central_monitor_bind_address() -> Address:
    """Return the default bind address for the Central Monitoring server."""

    return CENTRAL_MONITOR_HOST, CENTRAL_MONITOR_PORT
