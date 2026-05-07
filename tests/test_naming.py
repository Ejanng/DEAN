from __future__ import annotations

import unittest

from message_protocol import (
    DEREGISTER_RESPONSE,
    ERROR,
    LOOKUP_RESPONSE,
    REGISTER_RESPONSE,
    build_deregister_message,
    build_lookup_message,
    build_register_message,
)
from naming_server import NamingRegistry, NamingServer


class NamingRegistryTests(unittest.TestCase):
    def test_register_lookup_and_snapshot(self) -> None:
        registry = NamingRegistry()

        entry, updated = registry.register("emergency.monitor.main", "127.0.0.1", 9000)

        self.assertFalse(updated)
        self.assertEqual(entry.logical_name, "emergency.monitor.main")
        self.assertEqual(entry.ip, "127.0.0.1")
        self.assertEqual(entry.port, 9000)
        self.assertEqual(
            registry.snapshot(),
            {
                "emergency.monitor.main": {
                    "logical_name": "emergency.monitor.main",
                    "ip": "127.0.0.1",
                    "port": 9000,
                }
            },
        )

    def test_register_updates_existing_mapping(self) -> None:
        registry = NamingRegistry()
        registry.register("emergency.monitor.main", "127.0.0.1", 9000)

        entry, updated = registry.register("emergency.monitor.main", "127.0.0.1", 9100)

        self.assertTrue(updated)
        self.assertEqual(entry.port, 9100)
        self.assertEqual(registry.lookup("emergency.monitor.main").port, 9100)

    def test_deregister_removes_existing_mapping(self) -> None:
        registry = NamingRegistry()
        registry.register("emergency.monitor.main", "127.0.0.1", 9000)

        removed = registry.deregister("emergency.monitor.main")

        self.assertIsNotNone(removed)
        self.assertIsNone(registry.lookup("emergency.monitor.main"))

    def test_register_rejects_invalid_port(self) -> None:
        registry = NamingRegistry()

        with self.assertRaises(ValueError):
            registry.register("emergency.monitor.main", "127.0.0.1", 0)


class NamingServerRequestTests(unittest.TestCase):
    def test_register_request_returns_register_response(self) -> None:
        server = NamingServer()
        request = build_register_message("emergency.monitor.main", "127.0.0.1", 9000, 1)

        response = server.handle_request(request)

        self.assertEqual(response["type"], REGISTER_RESPONSE)
        self.assertEqual(response["logical_name"], "emergency.monitor.main")
        self.assertEqual(response["ip"], "127.0.0.1")
        self.assertEqual(response["port"], 9000)
        self.assertEqual(response["status"], "registered")
        self.assertGreater(response["timestamp"], request["timestamp"])

    def test_lookup_request_returns_lookup_response_for_registered_name(self) -> None:
        server = NamingServer()
        server.handle_request(build_register_message("emergency.monitor.main", "127.0.0.1", 9000, 1))

        response = server.handle_request(build_lookup_message("emergency.monitor.main", 2))

        self.assertEqual(response["type"], LOOKUP_RESPONSE)
        self.assertEqual(response["logical_name"], "emergency.monitor.main")
        self.assertEqual(response["ip"], "127.0.0.1")
        self.assertEqual(response["port"], 9000)

    def test_lookup_missing_name_returns_error_response(self) -> None:
        server = NamingServer()

        response = server.handle_request(build_lookup_message("missing.service", 1))

        self.assertEqual(response["type"], ERROR)
        self.assertIn("missing.service", response["error"])
        self.assertEqual(response["details"], "lookup_miss")

    def test_deregister_returns_deregister_response(self) -> None:
        server = NamingServer()
        server.handle_request(build_register_message("emergency.monitor.main", "127.0.0.1", 9000, 1))

        response = server.handle_request(build_deregister_message("emergency.monitor.main", 2))

        self.assertEqual(response["type"], DEREGISTER_RESPONSE)
        self.assertEqual(response["logical_name"], "emergency.monitor.main")
        self.assertEqual(response["status"], "deregistered")
        self.assertIsNone(server.registry.lookup("emergency.monitor.main"))

    def test_register_same_name_twice_marks_second_response_as_updated(self) -> None:
        server = NamingServer()
        server.handle_request(build_register_message("emergency.monitor.main", "127.0.0.1", 9000, 1))

        response = server.handle_request(
            build_register_message("emergency.monitor.main", "127.0.0.1", 9100, 2)
        )

        self.assertEqual(response["type"], REGISTER_RESPONSE)
        self.assertEqual(response["status"], "updated")
        self.assertEqual(server.registry.lookup("emergency.monitor.main").port, 9100)


if __name__ == "__main__":
    unittest.main()
