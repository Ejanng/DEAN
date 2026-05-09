# DEAN Architecture Diagram

## System Topology

```
+-------------------------------------------------------------+
|                    NAMING SERVER                             |
|              (naming_server.py : Port 8000)                  |
|  +-----------------------------------------+                |
|  |  Registry:                              |                |
|  |  "emergency.monitor.main" -> 127.0.0.1:9000 |           |
|  +-----------------------------------------+                |
+-------------------------+-----------------------------------+
                          | REGISTER / LOOKUP / DEREGISTER
          +---------------+---------------+
          |                               |
          v                               v
+----------------------+      +--------------------------+
|  CENTRAL MONITORING  |      |      SENSOR CLIENTS      |
|   (auction_server.py)|<---->|    (bidder_client.py)    |
|    Port 9000         |ALERTS|   - sensor_01             |
|                      |      |   - sensor_02             |
|  +----------------+  |      |   - sensor_03             |
|  | Event Queue    |  |      |   - ...                   |
|  | (Min-Heap by   |  |      +--------------------------+
|  |  Lamport Time) |  |
|  +----------------+  |
|  +----------------+  |
|  | Broadcast to   |  |
|  | all sensors    |  |
|  +----------------+  |
+----------------------+
```

## The 3 Pillars

### Pillar 1: Naming (Directory Service)
- **Component**: `naming_server.py`
- **Purpose**: Maps logical names (`emergency.monitor.main`) to physical `(IP, port)` addresses
- **Key Messages**: `REGISTER`, `LOOKUP`, `DEREGISTER`
- **Why it matters**: No hardcoded IPs anywhere in the system. If the monitor restarts on a new port, sensors automatically find it.

### Pillar 2: Message-Oriented Communication
- **Components**: `network_layer.py`, `message_protocol.py`
- **Purpose**: Asynchronous JSON message passing over TCP sockets with line-delimited encoding
- **Key Features**:
  - `MessageConnection` class handles partial TCP reads and UTF-8 decoding
  - `broadcast_message()` sends to all connected sensors simultaneously
  - All messages validated through `validate_message()` before serialization
- **Message Types**: `ALERT`, `ACK`, `HEARTBEAT`, `STATUS_UPDATE`, `EMERGENCY_SEQUENCE`, `ERROR`

### Pillar 3: Synchronization (Fair Event Ordering)
- **Component**: `lamport_clock.py`
- **Purpose**: Establish deterministic event order across distributed nodes
- **Lamport Rules**:
  1. **Local event**: `clock += 1`
  2. **Send message**: attach current `clock` timestamp
  3. **Receive message**: `clock = max(local, received) + 1`
- **Ordering**: Min-heap sorted by `(lamport_timestamp, sensor_id)` — sensor ID acts as tie-breaker for equal timestamps

## Data Flow

### Startup Phase
```
1. Naming Server starts on port 8000
2. Central Monitor starts → sends REGISTER("emergency.monitor.main", 127.0.0.1, 9000)
3. Sensor starts → sends LOOKUP("emergency.monitor.main") → receives (127.0.0.1, 9000)
4. Sensor connects to Monitor via TCP
```

### Emergency Phase
```
1. Sensor detects fire → increments Lamport clock → builds ALERT message
2. Sensor sends ALERT to Monitor (with optional --lag delay)
3. Monitor receives ALERT → updates its own Lamport clock
4. Monitor pushes alert into priority queue (min-heap)
5. Monitor sends ACK back to originating sensor
6. Monitor broadcasts EMERGENCY_SEQUENCE to ALL connected sensors
7. All sensors display the same ordered sequence
```

## Threading Model

### Naming Server
- Main thread: `accept()` loop
- Per-client thread: daemon worker for each connection

### Central Monitor
- Main thread: `accept()` loop + status broadcast daemon
- Per-client thread: daemon worker for each sensor connection

### Sensor Client
- Main thread: interactive `input()` loop
- Receive thread: daemon listening for server messages
- Heartbeat thread: daemon sending periodic HEARTBEAT

## Port Assignment

| Component | Default Port | Config Variable |
|-----------|-------------|-----------------|
| Naming Server | 8000 | `NAMING_SERVER_PORT` |
| Central Monitor | 9000 | `CENTRAL_MONITOR_PORT` |

## File-to-Component Mapping

| Deliverable File | Role | Team Member |
|-----------------|------|-------------|
| `naming_server.py` | Directory service (Pillar 1) | Member 1 - Registry Architect |
| `network_layer.py` | TCP socket wrappers (Pillar 2) | Member 2 - Middleware Engineer |
| `message_protocol.py` | JSON builders & validation (Pillar 2) | Member 2 - Middleware Engineer |
| `lamport_clock.py` | Logical clock & ordering (Pillar 3) | Member 3 - Timekeeper |
| `bidder_client.py` | Interactive sensor node | Member 4 - Client Developer |
| `auction_server.py` | Core monitor + integrator | Member 5 - Server Developer |
| `utils.py` | Shared helpers | All |
| `config.py` | Centralized constants | All |
