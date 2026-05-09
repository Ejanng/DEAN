# Member 4: The Client Developer (Lead Bidder UI/UX)

**Role:** Build the interactive Sensor Client terminal interface that users interact with to trigger alerts and view live updates.

**Files Owned:** `bidder_client.py`

**Supporting Files Used:** `config.py`, `lamport_clock.py`, `message_protocol.py`, `network_layer.py`, `utils.py`

---

## Overview

The Sensor Client (`bidder_client.py`) is the user-facing component of DEAN. It is an interactive terminal application that:

1. **Resolves** the Central Monitor's address via the Naming Server at startup.
2. **Connects** to the monitor and maintains a persistent TCP connection.
3. **Displays** a live prompt showing the sensor's ID and current Lamport clock value.
4. **Sends** `ALERT` messages when the user triggers a fire detection.
5. **Sends** periodic `HEARTBEAT` messages to keep the connection alive.
6. **Receives** live updates from the monitor: ACKs, Emergency Sequences, and Status Updates.
7. **Simulates** network lag (optional `--lag` flag) for demo purposes.

The client demonstrates **all three pillars** from the user's perspective: it uses the Naming Server (Pillar 1), sends validated JSON messages (Pillar 2), and maintains a Lamport Clock (Pillar 3).

---

## Architecture

```
+--------------------------------------------------+
|                 SensorClient                      |
|                                                   |
|  Main Thread          Receive Thread (daemon)     |
|  ----------------     ------------------------    |
|  input() loop         receive_message() loop      |
|  |                    |                           |
|  v                    v                           |
|  _send_alert()        _handle_message()           |
|  _send_heartbeat()    _handle_ack()               |
|                       _handle_emergency_sequence() |
|                       _handle_status_update()     |
|                       _handle_error()             |
|                                                   |
|  Heartbeat Thread (daemon)                        |
|  ------------------------                         |
|  periodic HEARTBEAT messages                      |
+--------------------------------------------------+
```

---

## Key Components

### `SensorClient`

```python
class SensorClient:
    def __init__(
        self,
        sensor_id: str,
        location: str,
        naming_address: tuple[str, int] | None = None,
        lag: bool = False,
        *,
        clock: LamportClock | None = None,
    ) -> None:
        self.sensor_id = sensor_id
        self.location = location
        self.naming_address = naming_address or naming_server_address()
        self.lag = lag
        self.clock = clock or LamportClock()
        self.connection: MessageConnection | None = None
        self._shutdown_event = threading.Event()
        self._print_lock = threading.Lock()
```

**Threading Model:**
- **Main thread:** Runs `_input_loop()`, blocking on `input()` for user commands.
- **Receive thread (daemon):** Runs `_receive_loop()`, blocking on `connection.receive_message()`.
- **Heartbeat thread (daemon):** Runs `_heartbeat_loop()`, sleeping between `HEARTBEAT` sends.

**Why daemon threads?** If the main thread exits (user presses `q`), daemon threads are automatically terminated. This prevents zombie threads from keeping the process alive.

### Startup Flow

```python
def run(self) -> None:
    monitor_address = self._lookup_monitor_address()  # Step 1: Naming Server
    self.connection = self._connect_to_monitor(monitor_address)  # Step 2: TCP connect
    self._receive_thread.start()
    self._heartbeat_thread.start()
    self._input_loop()  # Step 3: Interactive session
```

#### Step 1: Naming Server Lookup

```python
def _lookup_monitor_address(self) -> tuple[str, int]:
    with create_client_connection(*self.naming_address) as conn:
        lookup_msg = build_lookup_message(
            CENTRAL_MONITOR_LOGICAL_NAME,
            self.clock.send_event(),  # Rule 2: increment before sending
        )
        conn.send_message(lookup_msg)
        response = conn.receive_message()
        self.clock.receive_event(
            response.get("timestamp", response.get("lamport_timestamp", 0))
        )  # Rule 3: merge received timestamp
        return response["ip"], response["port"]
```

This is the **Naming Pillar** in action. The sensor has no idea where the monitor is until it asks the Naming Server.

#### Step 2: Connect to Monitor

```python
def _connect_to_monitor(self, address: tuple[str, int]) -> MessageConnection:
    return create_client_connection(*address)
```

A persistent TCP connection is established. All subsequent messages (alerts, heartbeats) travel over this single connection.

### Interactive Input Loop

```python
def _input_loop(self) -> None:
    print(f"\nSensor {self.sensor_id} ready at {self.location}.")
    print("Commands: [Enter/f] trigger alert | [q] quit")

    while not self._shutdown_event.is_set():
        try:
            user_input = input(
                f"[{self.sensor_id} | Lamport={self.clock.time}] > "
            )
        except EOFError:
            break

        command = user_input.strip().lower()
        if command in ("", "f", "fire"):
            self._send_alert()
        elif command == "q":
            break
```

**Why `input()`?** It provides a simple, blocking interface that works in any terminal without external libraries like `curses`. The prompt displays the current Lamport clock value so the user can see it advance in real time.

### Sending an Alert

```python
def _send_alert(self, payload: str = "Smoke and heat detected") -> None:
    # Optional network lag simulation
    if self.lag:
        lag_seconds = random.uniform(
            SIMULATED_LAG_MIN_SECONDS, SIMULATED_LAG_MAX_SECONDS
        )
        time.sleep(lag_seconds)

    msg = build_alert_message(
        sensor_id=self.sensor_id,
        location=self.location,
        severity=DEFAULT_ALERT_SEVERITY,
        lamport_timestamp=self.clock.send_event(),  # Rule 2
        payload=payload,
    )
    self.connection.send_message(msg)
    print(f"\n[ALERT SENT] {payload} at {self.location} (Lamport={msg['lamport_timestamp']})")
```

**The `--lag` flag** is crucial for the demo. It adds a random 0.5–2.0 second delay before sending. This simulates real-world network congestion and allows the team to prove that Lamport timestamps — not physical arrival time — determine event order.

### Heartbeat Loop

```python
def _heartbeat_loop(self) -> None:
    while not self._shutdown_event.is_set():
        self._shutdown_event.wait(HEARTBEAT_INTERVAL_SECONDS)
        if self._shutdown_event.is_set():
            break
        self._send_heartbeat()
```

- `HEARTBEAT_INTERVAL_SECONDS` is defined in `config.py` as `5.0`.
- The heartbeat message includes the sensor's current Lamport timestamp.
- Heartbeats serve two purposes: (1) keep the connection alive through NATs and firewalls, and (2) allow the monitor to detect disconnected sensors.

### Receiving Messages

```python
def _receive_loop(self) -> None:
    while not self._shutdown_event.is_set():
        try:
            message = self.connection.receive_message()
        except socket.timeout:
            continue
        except ConnectionClosedError:
            break
        self._handle_message(message)
```

The receive loop blocks on `receive_message()` until the monitor sends something. When a message arrives, the Lamport clock is updated (Rule 3) and the message is dispatched by type.

### Message Handlers

| Message Type | Handler | What It Does |
|-------------|---------|--------------|
| `ACK` | `_handle_ack()` | Prints confirmation that the alert was received |
| `EMERGENCY_SEQUENCE` | `_handle_emergency_sequence()` | Prints the ordered list of all alerts |
| `STATUS_UPDATE` | `_handle_status_update()` | Prints active sensor count and system status |
| `ERROR` | `_handle_error()` | Prints the error message and details |

All handlers use `self._print_lock` to prevent garbled output when the receive thread prints while the main thread is waiting for `input()`.

---

## CLI Arguments

```bash
python3 bidder_client.py \
    --id sensor_01 \
    --location "Building A, Floor 1" \
    --naming-host 127.0.0.1 \
    --naming-port 8000 \
    --lag
```

| Flag | Default | Purpose |
|------|---------|---------|
| `--id` | `sensor_01` | Unique sensor identifier |
| `--location` | `Building A, Floor 1` | Physical location string |
| `--naming-host` | `127.0.0.1` | Naming Server IP |
| `--naming-port` | `8000` | Naming Server port |
| `--lag` | disabled | Simulate network lag before sending alerts |

---

## Integration with Other Members

- **Member 1 (Registry Architect):** Uses the Naming Server to resolve `emergency.monitor.main` before connecting.
- **Member 2 (Middleware Engineer):** Uses `MessageConnection`, `create_client_connection`, and all `build_*` message functions.
- **Member 3 (Timekeeper):** Uses `LamportClock` to timestamp every outgoing message and merge incoming timestamps.
- **Member 5 (Server Developer):** Connects to the Central Monitor and exchanges ALERT, HEARTBEAT, ACK, and EMERGENCY_SEQUENCE messages.

---

## Why This Design Works

1. **Three Threads:** The main thread handles user input, the receive thread handles server pushes, and the heartbeat thread maintains liveness. None blocks the others.
2. **Thread-Safe Printing:** `_print_lock` prevents interleaved output from multiple threads.
3. **Graceful Shutdown:** `threading.Event` signals all threads to stop. Sockets are closed safely.
4. **Demo-Ready:** The `--lag` flag makes it trivial to demonstrate Lamport ordering correctness.
5. **No Dependencies:** Pure standard library means it runs on any Python 3.10+ installation.
