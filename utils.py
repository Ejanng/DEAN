"""Small shared helpers for identifiers, timestamps, and logging."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from itertools import count

_alert_counter = count(1)


def utc_now_iso() -> str:
    """Return a UTC timestamp formatted for JSON messages."""

    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def next_alert_id(prefix: str = "alert") -> str:
    """Return a predictable alert identifier for demo-friendly output."""

    return f"{prefix}_{next(_alert_counter):03d}"


def configure_logging(name: str) -> logging.Logger:
    """Create or retrieve a logger with a simple shared format."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    )
    return logging.getLogger(name)
