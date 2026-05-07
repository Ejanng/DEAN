"""JSON message builders, validation, and wire encoding for DEAN."""

from __future__ import annotations

import json
from typing import Any, Mapping, Sequence

from config import (
    DEFAULT_HEARTBEAT_STATUS,
    DEFAULT_SYSTEM_STATUS,
    JSON_ENCODING,
    MESSAGE_DELIMITER,
)
from utils import utc_now_iso

REGISTER = "REGISTER"
REGISTER_RESPONSE = "REGISTER_RESPONSE"
LOOKUP = "LOOKUP"
LOOKUP_RESPONSE = "LOOKUP_RESPONSE"
DEREGISTER = "DEREGISTER"
DEREGISTER_RESPONSE = "DEREGISTER_RESPONSE"
ALERT = "ALERT"
ACK = "ACK"
HEARTBEAT = "HEARTBEAT"
STATUS_UPDATE = "STATUS_UPDATE"
EMERGENCY_SEQUENCE = "EMERGENCY_SEQUENCE"
ERROR = "ERROR"

KNOWN_MESSAGE_TYPES = frozenset(
    {
        REGISTER,
        REGISTER_RESPONSE,
        LOOKUP,
        LOOKUP_RESPONSE,
        DEREGISTER,
        DEREGISTER_RESPONSE,
        ALERT,
        ACK,
        HEARTBEAT,
        STATUS_UPDATE,
        EMERGENCY_SEQUENCE,
        ERROR,
    }
)

REQUIRED_FIELDS = {
    REGISTER: {"type", "logical_name", "ip", "port", "timestamp"},
    REGISTER_RESPONSE: {"type", "logical_name", "ip", "port", "status", "timestamp"},
    LOOKUP: {"type", "logical_name", "timestamp"},
    LOOKUP_RESPONSE: {"type", "logical_name", "ip", "port", "timestamp"},
    DEREGISTER: {"type", "logical_name", "timestamp"},
    DEREGISTER_RESPONSE: {"type", "logical_name", "status", "timestamp"},
    ALERT: {
        "type",
        "sensor_id",
        "location",
        "severity",
        "lamport_timestamp",
        "physical_time",
        "payload",
    },
    ACK: {"type", "alert_id", "sensor_id", "status", "lamport_timestamp"},
    HEARTBEAT: {"type", "sensor_id", "lamport_timestamp", "status"},
    STATUS_UPDATE: {
        "type",
        "active_sensors",
        "pending_alerts",
        "system_status",
        "lamport_timestamp",
    },
    EMERGENCY_SEQUENCE: {"type", "sequence", "lamport_timestamp", "message"},
    ERROR: {"type", "error", "timestamp"},
}

INTEGER_FIELDS = {
    "timestamp",
    "port",
    "lamport_timestamp",
    "active_sensors",
    "pending_alerts",
}


class ProtocolError(ValueError):
    """Raised when a message does not match the DEAN JSON protocol."""


def _copy_message(message: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(message, Mapping):
        raise ProtocolError("Messages must be dictionary-like mappings.")
    return dict(message)


def validate_message(message: Mapping[str, Any]) -> dict[str, Any]:
    """Validate a DEAN protocol message and return a defensive copy."""

    payload = _copy_message(message)
    message_type = payload.get("type")

    if not isinstance(message_type, str) or message_type not in KNOWN_MESSAGE_TYPES:
        raise ProtocolError(f"Unknown message type: {message_type!r}")

    missing_fields = REQUIRED_FIELDS[message_type] - payload.keys()
    if missing_fields:
        missing = ", ".join(sorted(missing_fields))
        raise ProtocolError(f"Message type {message_type} is missing fields: {missing}")

    for field in INTEGER_FIELDS.intersection(payload.keys()):
        if not isinstance(payload[field], int):
            raise ProtocolError(f"Field '{field}' must be an integer.")

    if "sequence" in payload and not isinstance(payload["sequence"], list):
        raise ProtocolError("Field 'sequence' must be a list.")

    return payload


def serialize_message(message: Mapping[str, Any]) -> str:
    """Serialize a validated message into compact JSON."""

    payload = validate_message(message)
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def encode_message(message: Mapping[str, Any]) -> bytes:
    """Serialize a message for line-delimited transport over TCP."""

    serialized = serialize_message(message)
    return f"{serialized}{MESSAGE_DELIMITER}".encode(JSON_ENCODING)


def decode_message(raw_message: str | bytes) -> dict[str, Any]:
    """Parse and validate a raw JSON message from the network."""

    if isinstance(raw_message, bytes):
        try:
            raw_message = raw_message.decode(JSON_ENCODING)
        except UnicodeDecodeError as exc:
            raise ProtocolError("Message bytes were not valid UTF-8.") from exc

    try:
        decoded = json.loads(raw_message.strip())
    except json.JSONDecodeError as exc:
        raise ProtocolError("Received malformed JSON.") from exc

    if not isinstance(decoded, dict):
        raise ProtocolError("JSON payloads must decode into an object.")

    return validate_message(decoded)


def build_register_message(
    logical_name: str,
    ip: str,
    port: int,
    timestamp: int,
) -> dict[str, Any]:
    return validate_message(
        {
            "type": REGISTER,
            "logical_name": logical_name,
            "ip": ip,
            "port": port,
            "timestamp": timestamp,
        }
    )


def build_register_response(
    logical_name: str,
    ip: str,
    port: int,
    status: str,
    timestamp: int,
) -> dict[str, Any]:
    return validate_message(
        {
            "type": REGISTER_RESPONSE,
            "logical_name": logical_name,
            "ip": ip,
            "port": port,
            "status": status,
            "timestamp": timestamp,
        }
    )


def build_lookup_message(logical_name: str, timestamp: int) -> dict[str, Any]:
    return validate_message(
        {
            "type": LOOKUP,
            "logical_name": logical_name,
            "timestamp": timestamp,
        }
    )


def build_lookup_response(
    logical_name: str,
    ip: str,
    port: int,
    timestamp: int,
) -> dict[str, Any]:
    return validate_message(
        {
            "type": LOOKUP_RESPONSE,
            "logical_name": logical_name,
            "ip": ip,
            "port": port,
            "timestamp": timestamp,
        }
    )


def build_deregister_message(logical_name: str, timestamp: int) -> dict[str, Any]:
    return validate_message(
        {
            "type": DEREGISTER,
            "logical_name": logical_name,
            "timestamp": timestamp,
        }
    )


def build_deregister_response(
    logical_name: str,
    status: str,
    timestamp: int,
) -> dict[str, Any]:
    return validate_message(
        {
            "type": DEREGISTER_RESPONSE,
            "logical_name": logical_name,
            "status": status,
            "timestamp": timestamp,
        }
    )


def build_alert_message(
    sensor_id: str,
    location: str,
    severity: str,
    lamport_timestamp: int,
    payload: str,
    physical_time: str | None = None,
) -> dict[str, Any]:
    return validate_message(
        {
            "type": ALERT,
            "sensor_id": sensor_id,
            "location": location,
            "severity": severity,
            "lamport_timestamp": lamport_timestamp,
            "physical_time": physical_time or utc_now_iso(),
            "payload": payload,
        }
    )


def build_ack_message(
    alert_id: str,
    sensor_id: str,
    status: str,
    lamport_timestamp: int,
) -> dict[str, Any]:
    return validate_message(
        {
            "type": ACK,
            "alert_id": alert_id,
            "sensor_id": sensor_id,
            "status": status,
            "lamport_timestamp": lamport_timestamp,
        }
    )


def build_heartbeat_message(
    sensor_id: str,
    lamport_timestamp: int,
    status: str = DEFAULT_HEARTBEAT_STATUS,
) -> dict[str, Any]:
    return validate_message(
        {
            "type": HEARTBEAT,
            "sensor_id": sensor_id,
            "lamport_timestamp": lamport_timestamp,
            "status": status,
        }
    )


def build_status_update_message(
    active_sensors: int,
    pending_alerts: int,
    lamport_timestamp: int,
    system_status: str = DEFAULT_SYSTEM_STATUS,
) -> dict[str, Any]:
    return validate_message(
        {
            "type": STATUS_UPDATE,
            "active_sensors": active_sensors,
            "pending_alerts": pending_alerts,
            "system_status": system_status,
            "lamport_timestamp": lamport_timestamp,
        }
    )


def build_emergency_sequence_message(
    sequence: Sequence[Mapping[str, Any]],
    lamport_timestamp: int,
    message: str,
) -> dict[str, Any]:
    return validate_message(
        {
            "type": EMERGENCY_SEQUENCE,
            "sequence": [dict(item) for item in sequence],
            "lamport_timestamp": lamport_timestamp,
            "message": message,
        }
    )


def build_error_message(
    error: str,
    timestamp: int,
    details: str | None = None,
) -> dict[str, Any]:
    payload = {
        "type": ERROR,
        "error": error,
        "timestamp": timestamp,
    }
    if details:
        payload["details"] = details
    return validate_message(payload)
