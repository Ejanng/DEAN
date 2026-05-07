# DEAN — System Usage Instructions

## Prerequisites

- **Python 3.10** or newer
- **No external dependencies** — the project uses only the Python standard library

## Running Tests

Run the full test suite before starting the system:

```bash
python3 -m unittest discover -s tests -v
```

Run a specific test module:

```bash
python3 -m unittest tests.test_naming -v
python3 -m unittest tests.test_lamport -v
python3 -m unittest tests.test_message_protocol -v
python3 -m unittest tests.test_network -v
python3 -m unittest tests.test_central_monitor -v
python3 -m unittest tests.test_sensor_client -v
python3 -m unittest tests.test_integration -v
```

## Architecture Overview

```
+--------------------------------------------------+
|  Naming Server  (naming_server.py : port 8000)   |
+--------------------------------------------------+
         ^ | REGISTER / LOOKUP / DEREGISTER
         | v
+----------------------+      +--------------------+
| Central Monitoring   |<---->|  Sensor Clients    |
| (central_monitor.py) |ALERTS| (sensor_client.py) |
|  port 9000           |      |  - sensor_01       |
+----------------------+      |  - sensor_02       |
                              |  - ...             |
                              +--------------------+
```

**Communication Flow**
1. Naming Server starts on a well-known port.
2. Central Monitor registers itself under `emergency.monitor.main`.
3. Each Sensor looks up that name and connects to the resolved address.
4. Sensors send `HEARTBEAT` messages periodically.
5. On fire detection, a sensor sends an `ALERT` with a Lamport timestamp.
6. The Monitor orders alerts in a priority queue and broadcasts `EMERGENCY_SEQUENCE` to all sensors.

## Starting the System

### Step 1 — Naming Server

Open a terminal and run:

```bash
python3 naming_server.py
```

Optional: custom bind address

```bash
python3 naming_server.py --host 127.0.0.1 --port 8000
```

### Step 2 — Central Monitor

Open a **second** terminal and run:

```bash
python3 central_monitor.py
```

You should see a log message confirming registration with the Naming Server.

### Step 3 — Sensor Clients

Open a **third** terminal and run the first sensor:

```bash
python3 sensor_client.py --id sensor_01 --location "Building A, Floor 1"
```

Open additional terminals for more sensors:

```bash
python3 sensor_client.py --id sensor_02 --location "Building A, Floor 2"
python3 sensor_client.py --id sensor_03 --location "Building A, Floor 3"
```

### Sensor Client Options

| Flag | Description | Default |
|------|-------------|---------|
| `--id` | Sensor identifier | `sensor_01` |
| `--location` | Physical location string | `Building A, Floor 1` |
| `--naming-host` | Naming Server IP | `127.0.0.1` |
| `--naming-port` | Naming Server port | `8000` |
| `--lag` | Simulate network lag before sending alerts | disabled |

## Interactive Commands

Each sensor client shows a prompt:

```
[sensor_01 | Lamport=5] >
```

| Key | Action |
|-----|--------|
| `Enter` or `f` | Trigger a fire alert |
| `q` | Quit the sensor client |

When an alert is sent, the sensor displays:

```
[ALERT SENT] Smoke and heat detected at Building A, Floor 1 (Lamport=6)
```

Live updates from the monitor appear automatically:

```
[ACK] Alert alert_001 received (Lamport=7)

[EMERGENCY SEQUENCE] Emergency sequence contains 2 alert(s).
  1. sensor_02 @ Building A, Floor 2 (Lamport=5, critical)
  2. sensor_01 @ Building A, Floor 1 (Lamport=6, critical)

[STATUS] Sensors: 3 | Pending: 2 | Status: normal (Lamport=8)
```

## Demo Script: Proving Lamport Ordering

This demo shows that logical timestamps correctly order events even when physical arrival order is reversed by network lag.

### Setup
Start the Naming Server, Central Monitor, and three sensors:

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

**`--lag` on sensor_03** adds a random 0.5–2.0 second delay before each alert.

### Trigger Alerts

1. In **sensor_03** (lagged), press `Enter` first.
2. Immediately in **sensor_02**, press `Enter`.
3. Immediately in **sensor_01**, press `Enter`.

### Expected Result

Because of the lag, the monitor may physically receive the alerts in order: sensor_02, sensor_01, sensor_03. However, the `EMERGENCY_SEQUENCE` broadcast will list them in **Lamport timestamp order**, proving that the lagged sensor's early event is correctly prioritized.

Example output:

```
[EMERGENCY SEQUENCE] Emergency sequence contains 3 alert(s).
  1. sensor_03 @ Floor 3 (Lamport=4, critical)
  2. sensor_02 @ Floor 2 (Lamport=5, critical)
  3. sensor_01 @ Floor 1 (Lamport=6, critical)
```

All sensors will display the **same ordered sequence**, demonstrating distributed agreement on event order.

## Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| `Connection refused` | Monitor or Naming Server not running | Start servers in the correct order |
| `Address already in use` | Previous process still holding the port | Wait a few seconds or use a different port |
| Sensor hangs on startup | Naming Server unreachable | Check `--naming-host` and `--naming-port` flags |
| Garbled terminal output | Multiple threads printing simultaneously | This is expected; press Enter to refresh the prompt |
| Alerts not ordered correctly | Clocks not synchronized | Ensure all nodes use `send_event()` and `receive_event()` correctly |

## Shutdown

- Press `q` + `Enter` in each sensor client
- Press `Ctrl+C` in the Central Monitor terminal
- Press `Ctrl+C` in the Naming Server terminal

## File Reference

| File | Purpose |
|------|---------|
| `naming_server.py` | DNS-like directory service (port 8000) |
| `central_monitor.py` | Core server that orders alerts (port 9000) |
| `sensor_client.py` | Interactive sensor node |
| `lamport_clock.py` | Logical clock implementation |
| `message_protocol.py` | JSON message builders and validators |
| `network_layer.py` | TCP socket wrappers |
| `config.py` | Ports, timeouts, and constants |
| `utils.py` | Logging, timestamps, and alert IDs |
