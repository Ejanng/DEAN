from __future__ import annotations

import unittest

from lamport_clock import (
    LamportClock,
    alert_priority_key,
    compare_logical_order,
    event_sort_key,
)


class LamportClockTests(unittest.TestCase):
    def test_clock_rules_follow_lamport_algorithm(self) -> None:
        clock = LamportClock()

        self.assertEqual(clock.local_event(), 1)
        self.assertEqual(clock.send_event(), 2)
        self.assertEqual(clock.receive_event(5), 6)
        self.assertEqual(clock.peek(), 6)

    def test_receive_event_rejects_negative_timestamp(self) -> None:
        clock = LamportClock()

        with self.assertRaises(ValueError):
            clock.receive_event(-1)

    def test_compare_logical_order_uses_sensor_id_as_tie_breaker(self) -> None:
        self.assertEqual(compare_logical_order(3, "sensor_01", 3, "sensor_02"), -1)
        self.assertEqual(compare_logical_order(4, "sensor_03", 3, "sensor_04"), 1)
        self.assertEqual(compare_logical_order(5, "sensor_01", 5, "sensor_01"), 0)

    def test_alert_priority_key_matches_heap_order(self) -> None:
        alert = {"lamport_timestamp": 7, "sensor_id": "sensor_09"}

        self.assertEqual(alert_priority_key(alert), (7, "sensor_09"))
        self.assertEqual(event_sort_key(7, "sensor_09"), (7, "sensor_09"))


if __name__ == "__main__":
    unittest.main()
