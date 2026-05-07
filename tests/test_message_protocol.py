from __future__ import annotations

import unittest

from message_protocol import (
    ALERT,
    HEARTBEAT,
    ProtocolError,
    build_alert_message,
    build_heartbeat_message,
    build_register_message,
    decode_message,
    encode_message,
    validate_message,
)


class MessageProtocolTests(unittest.TestCase):
    def test_register_message_round_trip(self) -> None:
        message = build_register_message("emergency.monitor.main", "127.0.0.1", 9000, 1)

        decoded = decode_message(encode_message(message))

        self.assertEqual(decoded, message)

    def test_alert_builder_generates_physical_time(self) -> None:
        message = build_alert_message(
            sensor_id="sensor_01",
            location="Building A, Floor 2",
            severity="critical",
            lamport_timestamp=5,
            payload="Smoke detected",
        )

        self.assertEqual(message["type"], ALERT)
        self.assertIn("physical_time", message)
        self.assertTrue(message["physical_time"].endswith("Z"))

    def test_validate_message_rejects_missing_fields(self) -> None:
        with self.assertRaises(ProtocolError):
            validate_message({"type": HEARTBEAT, "sensor_id": "sensor_01"})

    def test_validate_message_rejects_wrong_integer_type(self) -> None:
        with self.assertRaises(ProtocolError):
            validate_message(
                {
                    "type": HEARTBEAT,
                    "sensor_id": "sensor_01",
                    "lamport_timestamp": "9",
                    "status": "online",
                }
            )

    def test_heartbeat_builder_uses_default_status(self) -> None:
        message = build_heartbeat_message("sensor_02", 8)

        self.assertEqual(message["status"], "online")


if __name__ == "__main__":
    unittest.main()
