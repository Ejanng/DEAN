# DEAN — Decentralized Emergency Alert Network

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Standard Library Only](https://img.shields.io/badge/dependencies-none-green.svg)]()

> **DEAN** is a distributed-systems coursework project that simulates a network of fire-alarm sensors reporting emergencies to a central monitoring server. It uses **Lamport Logical Clocks** to establish a consistent partial ordering of events despite network latency and out-of-order message delivery.

---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Demo: Proving Lamport Ordering](#demo-proving-lamport-ordering)
- [Running Tests](#running-tests)
- [Project Structure](#project-structure)
- [Team Roles](#team-roles)
- [References](#references)

---

## Features

- **Naming Service** — Location-independent directory service; no hardcoded IPs
- **Lamport Logical Clocks** — Deterministic event ordering across all nodes
- **JSON Message Protocol** — Line-delimited JSON over TCP with full validation
- **Priority Queue Ordering** — Alerts sorted by `(lamport_timestamp, sensor_id)`
- **Real-time Broadcasts** — Emergency sequences and status updates pushed to all sensors
- **Simulated Network Lag** — Optional delay for demonstrating clock correctness
- **Graceful Disconnections** — Thread-safe connection management with heartbeat monitoring

---

## Architecture

```
+--------------------------------------------------+
|  Naming Server  (naming_server.py : port 8000)   |
|  - Registry: logical name -> (IP, port)          |
+--------------------------------------------------+
         ^ | REGISTER / LOOKUP / DEREGISTER
         | v
+----------------------+      +--------------------+
| Central Monitoring   |<---->|  Sensor Clients    |
| (central_monitor.py) |ALERTS| (sensor_client.py) |
|  port 9000           |      |  - sensor_01       |
|                      |      |  - sensor_02       |
+----------------------+      |  - sensor_03       |
                              +--------------------+
```

### Communication Flow

1. **Startup** — Naming Server starts on a well-known port. Central Monitor registers as `emergency.monitor.main`. Sensors look up that name and connect.
2. **Normal Operation** — Sensors send periodic `HEARTBEAT` messages. Monitor broadcasts `STATUS_UPDATE` messages.
3. **Emergency** — A sensor increments its Lamport clock, builds an `ALERT`, and sends it. The monitor updates its clock, places the alert in a priority queue, and broadcasts an `EMERGENCY_SEQUENCE` to all sensors.

---

## Prerequisites

- **Python 3.10** or newer
- **No external dependencies** — the project uses only the Python standard library

---

## Quick Start

### 1. Start the Naming Server

```bash
python3 naming_server.py
```

### 2. Start the Central Monitor

```bash
python3 central_monitor.py
```

You should see a log confirming registration with the Naming Server.

### 3. Start Sensor Clients

Open one terminal per sensor:

```bash
python3 sensor_client.py --id sensor_01 --location "Building A, Floor 1"
python3 sensor_client.py --id sensor_02 --location "Building A, Floor 2"
python3 sensor_client.py --id sensor_03 --location "Building A, Floor 3"
```

### Interactive Commands

Each sensor shows a live prompt:

```
[sensor_01 | Lamport=5] >
```

| Key | Action |
|-----|--------|
| `Enter` or `f` | Trigger a fire alert |
| `q` | Quit the sensor |

### Sensor Options

| Flag | Description | Default |
|------|-------------|---------|
| `--id` | Sensor identifier | `sensor_01` |
| `--location` | Physical location | `Building A, Floor 1` |
| `--naming-host` | Naming Server IP | `127.0.0.1` |
| `--naming-port` | Naming Server port | `8000` |
| `--lag` | Simulate network lag | disabled |

---

## Demo: Proving Lamport Ordering

This demo shows that **logical timestamps correctly order events** even when physical arrival order is reversed by network lag.

### Setup

Start the Naming Server, Monitor, and three sensors. Add `--lag` to the third sensor:

```bash
# Terminal 1
python3 naming_server.py

# Terminal 2
python3 central_monitor.py

# Terminal 3
python3 sensor_client.py --id sensor_01 --location "Floor 1"

# Terminal 4
python3 sensor_client.py --id sensor_02 --location "Floor 2"

# Terminal 5
python3 sensor_client.py --id sensor_03 --location "Floor 3" --lag
```

### Trigger Alerts

1. In **sensor_03** (lagged), press `Enter` first.
2. Immediately in **sensor_02**, press `Enter`.
3. Immediately in **sensor_01**, press `Enter`.

### Expected Result

Because of the lag, the monitor may physically receive alerts in order: `sensor_02 → sensor_01 → sensor_03`. However, the broadcasted `EMERGENCY_SEQUENCE` will list them in **Lamport timestamp order**:

```
[EMERGENCY SEQUENCE] Emergency sequence contains 3 alert(s).
  1. sensor_03 @ Floor 3 (Lamport=4, critical)
  2. sensor_02 @ Floor 2 (Lamport=5, critical)
  3. sensor_01 @ Floor 1 (Lamport=6, critical)
```

All sensors display the **same ordered sequence**, demonstrating distributed agreement on event order.

---

## Running Tests

Run the full test suite:

```bash
python3 -m unittest discover -s tests -v
```

Run a specific module:

```bash
python3 -m unittest tests.test_naming -v
python3 -m unittest tests.test_lamport -v
python3 -m unittest tests.test_message_protocol -v
python3 -m unittest tests.test_network -v
python3 -m unittest tests.test_central_monitor -v
python3 -m unittest tests.test_sensor_client -v
python3 -m unittest tests.test_integration -v
```

---

## Project Structure

```
.
├── config.py                 # Ports, timeouts, addresses
├── lamport_clock.py          # Lamport clock + ordering helpers
├── message_protocol.py       # JSON message builders & validation
├── network_layer.py          # TCP socket helpers & broadcast
├── naming_server.py          # Full Naming Server implementation
├── central_monitor.py        # Central Monitoring server
├── sensor_client.py          # Interactive Sensor clients
├── utils.py                  # Logging, timestamps, alert IDs
├── tests/
│   ├── test_naming.py
│   ├── test_lamport.py
│   ├── test_message_protocol.py
│   ├── test_network.py
│   ├── test_central_monitor.py
│   ├── test_sensor_client.py
│   └── test_integration.py
├── docs/
│   ├── message_protocol.md
│   ├── setup_instructions.md
│   └── 2026-05-06_naming_server_01.md
├── README.md
├── INSTRUCTION.md            # Detailed usage & demo guide
├── DEAN_Project_Context.md   # Full project specification
├── AGENTS.md                 # Agent development guide
└── requirements.txt          # Empty (standard library only)
```

---

## Team Roles

| Member | Role | File(s) |
|--------|------|---------|
| 1 | Registry Architect (Naming) | `naming_server.py` |
| 2 | Middleware Engineer | `network_layer.py`, `message_protocol.py` |
| 3 | Timekeeper (Synchronization) | `lamport_clock.py` |
| 4 | Client Developer | `sensor_client.py` |
| 5 | Server Developer & Integrator | `central_monitor.py` |

---

## References

- `INSTRUCTION.md` — Step-by-step usage and troubleshooting
- `DEAN_Project_Context.md` — Full specification, architecture diagram, and grading rubric
- `docs/message_protocol.md` — Concise protocol reference
- `AGENTS.md` — Development guide for AI coding agents

---

*CmpSc 160 – Distributed Systems Final Project*
