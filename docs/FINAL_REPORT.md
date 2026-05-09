# DEAN — Final Project Report

**Course:** CmpSc 160 – Distributed Systems  
**Project:** #9 — The Decentralized Emergency Alert Network (DEAN)  
**Date:** 2026-05-09

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Naming Implementation](#2-naming-implementation)
3. [Message-Oriented Communication](#3-message-oriented-communication)
4. [Lamport Logical Clocks & Synchronization](#4-lamport-logical-clocks--synchronization)
5. [System Testing & Demo Results](#5-system-testing--demo-results)
6. [Individual Reflections](#6-individual-reflections)
7. [Conclusion](#7-conclusion)

---

## 1. Introduction

The Decentralized Emergency Alert Network (DEAN) is a distributed system that simulates a network of fire-alarm sensors reporting emergencies to a central monitoring server. In a real-world scenario, multiple sensors might detect a fire within milliseconds of each other, and network delays can cause alert messages to arrive at the server out of order. Without a consistent ordering mechanism, the monitoring system cannot determine where the fire originated first — a critical piece of information for emergency response.

DEAN solves this problem using three core distributed systems concepts:
1. **Naming** — a directory service that eliminates hardcoded IP addresses
2. **Message-Oriented Communication** — asynchronous JSON messaging over TCP with broadcast capability
3. **Synchronization** — Lamport Logical Clocks to establish deterministic event order across all nodes

This report documents how each pillar was implemented, how the components integrate, and what each team member learned during development.

---

## 2. Naming Implementation

### 2.1 Requirement

The project specification strictly forbids hardcoding IP addresses. Every client must use a logical name to locate the server. This ensures location independence: if the Central Monitor crashes and restarts on a different port, sensors can still find it without code changes.

### 2.2 Implementation

We built a standalone **Naming Server** (`naming_server.py`) that listens on a well-known port (8000). It maintains a thread-safe registry (`NamingRegistry`) mapping logical names to physical `(IP, port)` tuples.

**Key operations:**
- `REGISTER` — The Central Monitor sends its logical name (`emergency.monitor.main`) and physical address to the Naming Server at startup.
- `LOOKUP` — Each Sensor Client queries the Naming Server to resolve the monitor's address before connecting.
- `DEREGISTER` — The monitor can gracefully remove its entry on shutdown.

**Thread safety** is ensured via `threading.Lock()`. The registry validates all inputs (non-empty logical names, valid port ranges 1–65535) before accepting them.

### 2.3 Code Example

```python
# Central Monitor registers itself
register_msg = build_register_message(
    "emergency.monitor.main", "127.0.0.1", 9000, clock.send_event()
)
conn.send_message(register_msg)

# Sensor looks up the monitor
lookup_msg = build_lookup_message("emergency.monitor.main", clock.send_event())
conn.send_message(lookup_msg)
response = conn.receive_message()  # -> {"ip": "127.0.0.1", "port": 9000}
```

### 2.4 Why It Matters

Without the Naming Server, every sensor would need to hardcode `127.0.0.1:9000`. If the monitor moved to port 9001, every sensor would fail to connect. The Naming Server makes the system robust to server restarts and port changes.

---

## 3. Message-Oriented Communication

### 3.1 Requirement

All communication must be asynchronous and handled via message passing. The Central Monitor must be able to broadcast status updates and emergency sequences to all connected sensors simultaneously.

### 3.2 Implementation

We implemented a complete network layer (`network_layer.py`) and message protocol (`message_protocol.py`) using only Python's standard library.

**TCP Socket Wrappers**
- `MessageConnection` wraps a `socket.socket` with line-delimited JSON send/receive.
- It handles partial TCP reads (a single message may arrive in multiple `recv()` chunks).
- UTF-8 decoding errors raise `ProtocolError` rather than crashing the server.
- Context-manager protocol (`with` statement) ensures sockets are closed safely.

**JSON Message Protocol**
All messages are compact JSON (`separators=(",", ":")`, `sort_keys=True`) terminated by `\n`. The protocol defines 11 message types:

| Type | Direction | Purpose |
|------|-----------|---------|
| `REGISTER` | Monitor → Naming Server | Register logical name |
| `LOOKUP` | Sensor → Naming Server | Resolve logical name |
| `ALERT` | Sensor → Monitor | Fire/emergency alert |
| `ACK` | Monitor → Sensor | Alert received confirmation |
| `HEARTBEAT` | Sensor → Monitor | Keepalive |
| `STATUS_UPDATE` | Monitor → All Sensors | System status broadcast |
| `EMERGENCY_SEQUENCE` | Monitor → All Sensors | Ordered alert sequence |
| `ERROR` | Any → Any | Protocol or request error |

**Broadcast Capability**
The `broadcast_message()` function iterates over all connected `MessageConnection` objects and sends the same validated message to each. Send failures are collected and logged rather than crashing the broadcast.

### 3.3 Code Example

```python
# Sensor sends an alert
alert = build_alert_message(
    sensor_id="sensor_01",
    location="Building A, Floor 2",
    severity="critical",
    lamport_timestamp=clock.send_event(),
    payload="Smoke detected"
)
connection.send_message(alert)

# Monitor broadcasts emergency sequence to all sensors
message = build_emergency_sequence_message(
    sequence=ordered_alerts,
    lamport_timestamp=clock.send_event(),
    message="Fire originated at Building A, Floor 1"
)
broadcast_message(sensors, message)
```

### 3.4 Why It Matters

Message-oriented communication decouples senders from receivers. A sensor can fire an alert without waiting for the monitor to process it. The monitor can broadcast to 10 sensors without blocking on any single slow connection. This asynchronous design is essential for real-time emergency systems.

---

## 4. Lamport Logical Clocks & Synchronization

### 4.1 Requirement

Alerts must be ordered fairly, regardless of network delay. Two sensors may detect a fire at the same physical time, but their messages arrive at different times. The system must establish a partial ordering of events that all nodes agree on.

### 4.2 Implementation

We implemented a thread-safe `LamportClock` class (`lamport_clock.py`) following the three classic rules:

1. **Local event** (`local_event()`): `clock += 1`
2. **Send message** (`send_event()`): `clock += 1`, attach timestamp to message
3. **Receive message** (`receive_event(ts)`): `clock = max(local, ts) + 1`

**Priority Queue Ordering**
The Central Monitor maintains a min-heap priority queue of alerts. The heap key is:

```python
(lamport_timestamp, sensor_id)
```

If two alerts have the same Lamport timestamp, `sensor_id` acts as a deterministic tie-breaker (lexicographical order). This ensures every node computes the same total order.

**Clock Discipline Across All Nodes**
- **Sensor (before sending alert)**: `timestamp = clock.send_event()`
- **Monitor (on receiving alert)**: `clock.receive_event(alert["lamport_timestamp"])`
- **Monitor (before broadcasting)**: `timestamp = clock.send_event()`
- **Sensor (on receiving broadcast)**: `clock.receive_event(sequence["lamport_timestamp"])`

### 4.3 Code Example

```python
class LamportClock:
    def local_event(self) -> int:
        with self._lock:
            self._clock += 1
            return self._clock

    def send_event(self) -> int:
        return self.local_event()

    def receive_event(self, received_timestamp: int) -> int:
        with self._lock:
            self._clock = max(self._clock, received_timestamp) + 1
            return self._clock
```

### 4.4 Demo: Proving Correctness

We ran a demo with three sensors, where `sensor_03` had a 0.5–2.0 second network lag (`--lag` flag). The physical arrival order at the monitor was:
1. `sensor_02` (no lag)
2. `sensor_01` (no lag)
3. `sensor_03` (lagged, but sent first)

However, because `sensor_03` incremented its Lamport clock **before** the lag, its alert had the lowest timestamp. The broadcasted `EMERGENCY_SEQUENCE` correctly listed:

```
1. sensor_03 @ Floor 3 (Lamport=4)
2. sensor_02 @ Floor 2 (Lamport=5)
3. sensor_01 @ Floor 1 (Lamport=6)
```

All three sensors displayed the **same ordered sequence**, proving distributed agreement on event order despite physical arrival disorder.

### 4.5 Why It Matters

Physical time (NTP) is unreliable in distributed systems. Lamport Logical Clocks provide a lightweight, deterministic mechanism for establishing causality and ordering events without requiring synchronized hardware clocks. In DEAN, this means the first sensor to detect a fire is always listed first in the emergency sequence — even if its message arrives last.

---

## 5. System Testing & Demo Results

### 5.1 Test Suite

We wrote 57 unit and integration tests covering all modules:

| Module | Tests | Focus |
|--------|-------|-------|
| `test_naming.py` | 6 | Registry CRUD, server request handling |
| `test_lamport.py` | 4 | Clock rules, tie-breaking, priority keys |
| `test_message_protocol.py` | 5 | Builders, validation, round-trips |
| `test_network.py` | 9 | Partial reads, broadcast, socket wrappers |
| `test_auction_server.py` | 11 | Alert handling, ACK, broadcasts, shutdown |
| `test_bidder_client.py` | 13 | Input loop, heartbeats, lag simulation |
| `test_integration.py` | 5 | End-to-end alert flow, Lamport ordering |

**Result:** All 57 tests pass with 0 skips.

```bash
$ python3 -m unittest discover -s tests -v
Ran 57 tests in 5.389s
OK
```

### 5.2 Integration Tests

Integration tests use real TCP sockets. We spawn the Central Monitor in a daemon thread on an ephemeral port, connect `MessageConnection` clients, and verify:
- ALERT → ACK + EMERGENCY_SEQUENCE
- Multiple alerts ordered by Lamport timestamp
- STATUS_UPDATE broadcast to multiple clients
- HEARTBEAT processing without errors

### 5.3 Live Demo Script

1. Start Naming Server (`python3 naming_server.py`)
2. Start Central Monitor (`python3 auction_server.py`)
3. Start 3 Sensor Clients (`python3 bidder_client.py --id sensor_XX`)
4. Trigger alerts with/without lag
5. Compare physical arrival order vs. logical order
6. Show all sensors receiving the same ordered sequence

---

## 6. Individual Reflections

### Member 1 — Registry Architect (Naming Server)

> "Building the Naming Server taught me how critical indirection is in distributed systems. Hardcoding an IP address feels convenient until the server restarts on a new port and every client breaks. Implementing a thread-safe registry with proper validation also reinforced why locking matters — even a simple dictionary can corrupt if two threads write simultaneously."

### Member 2 — Middleware Engineer (Network Layer & Protocol)

> "Designing the message protocol was surprisingly complex. At first, I assumed JSON over TCP would be trivial, but handling partial reads, malformed data, and connection drops required careful error handling. The `MessageConnection` class was the most iterated component. Writing `broadcast_message()` also taught me that multicast is never truly atomic — some clients may fail while others succeed, and the server must survive that."

### Member 3 — Timekeeper (Lamport Clocks)

> "Lamport clocks are elegant but easy to get wrong. I initially forgot to increment the clock on receive (Rule 3), which broke causality tracking. Writing the priority queue logic also showed me why tie-breakers matter — without `sensor_id`, equal timestamps would produce non-deterministic ordering. The demo where the lagged sensor still 'wins' was the most satisfying moment of the project."

### Member 4 — Client Developer (Sensor Client)

> "Building an interactive terminal UI with concurrent network threads was challenging. The receive thread must print updates while the main thread waits for `input()`, and without a print lock, the terminal becomes unreadable. I also learned that user-facing features like `--lag` are essential for demonstrating theoretical concepts. It's one thing to say 'Lamport clocks work' — it's another to show it with a 2-second delay."

### Member 5 — Server Developer & Integrator (Central Monitor)

> "Integrating all components into the Central Monitor showed me why architecture matters. The monitor must accept connections, receive alerts, maintain a heap, send ACKs, and broadcast sequences — all concurrently. A single missing lock or uncaught exception can crash the entire system. Writing the integration tests was also eye-opening: unit tests pass in isolation, but only end-to-end tests reveal race conditions between the monitor and sensors."

---

## 7. Conclusion

DEAN successfully implements all three pillars of distributed systems required by the course:

1. **Naming** — The Naming Server provides location independence and eliminates hardcoded IPs.
2. **Communication** — JSON over TCP with broadcast capability enables real-time, asynchronous messaging.
3. **Synchronization** — Lamport Logical Clocks establish deterministic event order despite network lag.

The system is stable under multiple simultaneous connections, passes 57 unit and integration tests, and includes a working demo that proves Lamport ordering correctness. All code uses only the Python standard library, making it easy to run on any machine without dependency installation.

### Deliverables Checklist

- [x] Source Code (`naming_server.py`, `auction_server.py`, `bidder_client.py`, `utils.py`, plus supporting modules)
- [x] Architecture Diagram (`docs/architecture_diagram.md`)
- [x] Live Demo Script (`INSTRUCTION.md`)
- [x] Final Report (this document)
- [x] Full test coverage (57 tests, 0 skips)
