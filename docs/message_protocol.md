# INSTRUCTION.md â€” How the Program Works

## 1. System Overview

The **Decentralized Emergency Alert Network (DEAN)** is a distributed simulation where multiple sensor clients detect environmental hazards and report them to a central monitoring server. The system uses **Lamport logical clocks** to establish causal ordering of events, and a **naming server** for service discovery.

All communication happens over plain TCP sockets using JSON messages.

---

## 2. Architecture & Components

There are **three main roles** in the system:

| Role | File | Default Port | Purpose |
|------|------|-------------|---------|
| **Naming Server** | `naming_server.py` | 5050 | Service registry â€” stores and resolves name-to-address mappings |
| **Monitoring Server** | `monitoring_server.py` | 6060 | Receives alerts, sorts them by causal order, broadcasts emergency updates |
| **Sensor Client** | `sensor_client.py` | â€” | Interactive sensor â€” sends alerts on user input and receives broadcasts |

**Shared modules:**
- `lamport.py` â€” Lamport logical clock implementation
- `config.py` â€” Shared configuration and environment-variable parsing

---

## 3. Startup Sequence (Step-by-Step)

The system **must** be started in this exact order:

### Step 1: Start the Naming Server
```bash
python naming_server.py
```
- Binds to `0.0.0.0:5050` (or values from `config.py`)
- Waits for incoming TCP connections
- Maintains an in-memory `registry` dictionary: `name -> (host, port)`

### Step 2: Start the Monitoring Server
```bash
python monitoring_server.py
```
- Starts its own TCP server on `0.0.0.0:6060`
- **Immediately connects to the Naming Server** and registers itself:
  ```json
  {
    "type": "register",
    "name": "monitoring.server.main",
    "host": "<advertised-ip>",
    "port": 6060
  }
  ```
- Initializes its Lamport clock (`clock = LamportClock()`)
- Initializes empty lists: `clients = []` and `alerts = []`
- Enters a loop accepting persistent TCP connections from sensors

### Step 3: Start Sensor Clients
```bash
python sensor_client.py
```
- Prompts for a **Sensor ID** (e.g., `SensorA`, `SensorB`, etc.)
- **Queries the Naming Server** with a lookup message:
  ```json
  {
    "type": "lookup",
    "name": "monitoring.server.main"
  }
  ```
- Receives the monitoring server's `host` and `port`
- Opens a **persistent TCP connection** to the monitoring server
- Spawns a background daemon thread to listen for broadcasts
- Main thread blocks on user input (`Press Enter to send an alert`)

---

## 4. Runtime Communication Flow

### 4.1 Sensor Sends an Alert

When the user presses **Enter** in a sensor terminal:

1. **Capture wall-clock time**: `detected_at_ns = time.time_ns()`
2. **Increment Lamport clock**: `timestamp = clock.send_event()`
3. **Apply artificial delay** (simulated network lag based on sensor ID):
   - `SensorB` â†’ 2s, `SensorC` â†’ 3s, `SensorD` â†’ 4s, `SensorE` â†’ 5s
   - All others â†’ 0s
4. **Send JSON alert** to the monitoring server:
   ```json
   {
     "type": "alert",
     "sensor_id": "SensorA",
     "event": "fire_detected",
     "location": "Room 101",
     "timestamp": 3,
     "detected_at_ns": 1778343227968146600
   }
   ```

### 4.2 Monitoring Server Receives an Alert

For each incoming alert:

1. **Update Lamport clock**:
   ```python
   observed_time = clock.receive_event(sensor_timestamp)
   ```
   This advances the server's logical time past the sensor's timestamp.

2. **Store the alert** in the `alerts` list with:
   - `sensor_id`
   - `sensor_timestamp` (Lamport time from sensor)
   - `observed_time` (server's updated Lamport time)
   - `detected_at_ns` (wall-clock nanoseconds)

3. **Re-sort all alerts** using a three-level sort key:
   ```python
   sorted_alerts = sorted(alerts, key=lambda x: (
       x["sensor_timestamp"],           # 1. Causal order (Lamport time)
       x["detected_at_ns"],             # 2. Wall-clock tiebreaker
       x["sensor_id"],                  # 3. Deterministic final tiebreaker
   ))
   ```

4. **Print logical event order** to console:
   ```
   === LOGICAL EVENT ORDER ===
   Sensor SensorA | timestamp=2025-... | lamport=3
   Sensor SensorB | timestamp=2025-... | lamport=5
   ...
   First detected by sensor SensorA (lamport=3)
   ```

5. **Broadcast emergency update** to **all connected sensors**:
   ```json
   {
     "type": "emergency_update",
     "status": "confirmed_fire",
     "first_detected_by": "SensorA",
     "lamport_time": 11
   }
   ```

### 4.3 Sensor Receives Emergency Update

The background listening thread on each sensor:

1. Receives the `emergency_update` JSON
2. Updates its local Lamport clock:
   ```python
   clock.receive_event(message["lamport_time"])
   ```
3. Prints the update to the console:
   ```
   EMERGENCY UPDATE: {'type': 'emergency_update', 'status': 'confirmed_fire', ...}
   ```

---

## 5. Threading Model

| Component | Threading Behavior |
|-----------|------------------|
| **Naming Server** | One thread per incoming connection (`handle_client`) |
| **Monitoring Server** | One thread per persistent sensor connection (`handle_sensor`) |
| **Sensor Client** | Main thread handles user input (`send_alerts`); daemon thread listens for broadcasts (`listen_for_updates`) |

**Note:** The `clients` and `alerts` lists in `monitoring_server.py` are shared mutable global state accessed by multiple threads without explicit locks. This is acceptable for the educational prototype scale.

---

## 6. Lamport Clock Rules

The system follows standard Lamport logical clock rules (`lamport.py`):

| Event | Action | Code |
|-------|--------|------|
| Internal event | Increment local time | `clock.tick()` |
| Send message | Increment, then send timestamp | `clock.send_event()` |
| Receive message | `max(local, received) + 1` | `clock.receive_event(ts)` |

**Usage in the system:**
- **Sensor (send)**: `clock.send_event()` is called **before** the artificial delay
- **Monitoring Server (receive)**: `clock.receive_event(sensor_timestamp)` advances past the sensor's time
- **Sensor (receive broadcast)**: `clock.receive_event(message["lamport_time"])` advances past the server's time

---

## 7. Message Format Summary

| Direction | Message Type | Key Fields |
|-----------|-------------|-----------|
| Monitoring â†’ Naming | `register` | `name`, `host`, `port` |
| Sensor â†’ Naming | `lookup` | `name` |
| Naming â†’ Sensor | `ok` / `error` | `host`, `port` (on success) |
| Sensor â†’ Monitoring | `alert` | `sensor_id`, `event`, `timestamp`, `detected_at_ns` |
| Monitoring â†’ All Sensors | `emergency_update` | `status`, `first_detected_by`, `lamport_time` |

See `JSON_MESSAGE_FORMATS.txt` for complete examples.

---

## 8. Configuration

All values are in `config.py` and can be overridden via environment variables:

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `NAMING_SERVER_HOST` | `localhost` | Naming server address (used by monitoring & sensors) |
| `NAMING_SERVER_PORT` | `5050` | Naming server port |
| `NAMING_SERVER_BIND_HOST` | `0.0.0.0` | Naming server bind address |
| `MONITORING_SERVER_BIND_HOST` | `0.0.0.0` | Monitoring server bind address |
| `MONITORING_SERVER_PORT` | `6060` | Monitoring server port |
| `MONITORING_SERVER_ADVERTISE_HOST` | Auto-detected IP | Address advertised to sensors |

---

## 9. Multi-Machine Deployment

To run across multiple machines:

1. **On the Naming Server machine:**
   ```bash
   python naming_server.py
   ```

2. **On the Monitoring Server machine:**
   ```bash
   export NAMING_SERVER_HOST=<naming-server-ip>
   export MONITORING_SERVER_ADVERTISE_HOST=<this-machine-ip>
   python monitoring_server.py
   ```

3. **On each Sensor machine:**
   ```bash
   export NAMING_SERVER_HOST=<naming-server-ip>
   python sensor_client.py
   ```

Ensure firewalls allow TCP on ports `5050` and `6060`.

---

## 10. Shutdown & Cleanup

- **Ctrl+C** on any component terminates it immediately
- The monitoring server automatically removes disconnected sensors from the `clients` list when a `broadcast()` or `recv()` fails
- The naming server has no persistent state â€” restarting it clears the registry
- The monitoring server loses all alert history on restart