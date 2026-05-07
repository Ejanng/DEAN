"""Lamport logical clock primitives and ordering helpers for DEAN."""

from __future__ import annotations

from threading import Lock
from typing import Any, Mapping


def _normalize_timestamp(timestamp: int) -> int:
    if not isinstance(timestamp, int):
        raise TypeError("Lamport timestamps must be integers.")
    if timestamp < 0:
        raise ValueError("Lamport timestamps cannot be negative.")
    return timestamp


class LamportClock:
    """Thread-safe Lamport logical clock."""

    def __init__(self, initial_time: int = 0) -> None:
        self._clock = _normalize_timestamp(initial_time)
        self._lock = Lock()

    @property
    def time(self) -> int:
        with self._lock:
            return self._clock

    def peek(self) -> int:
        """Return the current logical time without mutating the clock."""

        return self.time

    def local_event(self) -> int:
        """Rule 1: increment the clock for a local event."""

        with self._lock:
            self._clock += 1
            return self._clock

    def send_event(self) -> int:
        """Rule 2: increment before sending a message."""

        return self.local_event()

    def receive_event(self, received_timestamp: int) -> int:
        """Rule 3: merge a received timestamp into the local clock."""

        normalized_timestamp = _normalize_timestamp(received_timestamp)
        with self._lock:
            self._clock = max(self._clock, normalized_timestamp) + 1
            return self._clock

    def sync_with(self, other: "LamportClock") -> int:
        """Merge this clock with another LamportClock instance."""

        return self.receive_event(other.peek())

    def __repr__(self) -> str:
        return f"LamportClock(time={self.time})"


def event_sort_key(lamport_timestamp: int, sensor_id: str) -> tuple[int, str]:
    """Return the deterministic priority key for a Lamport-ordered event."""

    if not sensor_id:
        raise ValueError("sensor_id is required for Lamport event ordering.")
    return _normalize_timestamp(lamport_timestamp), str(sensor_id)


def alert_priority_key(alert: Mapping[str, Any]) -> tuple[int, str]:
    """Return the heap key used by the Central Monitoring event queue."""

    try:
        timestamp = alert["lamport_timestamp"]
        sensor_id = alert["sensor_id"]
    except KeyError as exc:
        raise KeyError("Alerts must include 'lamport_timestamp' and 'sensor_id'.") from exc

    return event_sort_key(timestamp, sensor_id)


def compare_logical_order(
    left_timestamp: int,
    left_sensor_id: str,
    right_timestamp: int,
    right_sensor_id: str,
) -> int:
    """Compare two Lamport events using timestamp and sensor ID tie-breaking."""

    left_key = event_sort_key(left_timestamp, left_sensor_id)
    right_key = event_sort_key(right_timestamp, right_sensor_id)

    if left_key < right_key:
        return -1
    if left_key > right_key:
        return 1
    return 0
