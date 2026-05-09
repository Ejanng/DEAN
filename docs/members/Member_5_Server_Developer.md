# Member 5: The Server Developer & Integrator (Lead Auctioneer)

**Role:** Build the core Central Monitoring server and integrate all components into a working distributed system.

**Files Owned:** `auction_server.py`

**Supporting Files Used:** `config.py`, `lamport_clock.py`, `message_protocol.py`, `network_layer.py`, `utils.py`

---

## Overview

The Central Monitor (`auction_server.py`) is the brain of the DEAN system. It is the **integration point** for all three pillars:

1. **Naming (Pillar 1):** Registers itself with the Naming Server at startup so sensors can find it.
2. **Communication (Pillar 2):** Accepts multiple simultaneous sensor connections over TCP, receives JSON messages, and broadcasts ordered sequences back to all sensors.
3. **Synchronization (Pillar 3):** Maintains a Lamport Clock and a min-heap priority queue to sort incoming alerts by logical timestamp, ensuring fair event ordering.

The server must be **thread-safe**, **fault-tolerant**, and **responsive**. If one sensor disconnects or sends bad data, the server must continue serving all other sensors.

---

## Architecture

```
+--------------------------------------------------+
|              CentralMonitor                        |
|                                                    |
|  Main Thread (accept loop)                         |
|  --------------------------                        |
|  accept_connection() -> spawn worker thread        |
|                                                    |
|  Per-Client Threads (daemon)                       |
|  ---------------------------                       |
|  receive_message() -> _handle_message()            |
|    ALERT  -> _handle_alert()                       |
|      1. Update Lamport clock                       |
|      2. Push to priority queue (min-heap)          |
|      3. Send ACK to originating sensor             |
|      4. Broadcast EMERGENCY_SEQUENCE to all        |
|    HEARTBEAT -> _handle_heartbeat()                |
|      1. Update Lamport clock                       |
|      2. Log debug message                          |
|                                                    |
|  Status Broadcast Thread (daemon)                  |
|  -------------------------------                   |
|  Every 5 seconds: broadcast STATUS_UPDATE          |
|                                                    |
|  Data Structures                                   |
|  -----------------                                 |
|  _sensors: set[MessageConnection] (thread-safe)    |
|  _event_queue: min-heap of alerts (thread-safe)    |
|  _global_log: append-only list of all alerts       |
+--------------------------------------------------+
```

---

## Key Components

### `CentralMonitor`

```python
class CentralMonitor:
    def __init__(
        self,
        host: str = CENTRAL_MONITOR_HOST,
        port: int = CENTRAL_MONITOR_PORT,
        logical_name: str = CENTRAL_MONITOR_LOGICAL_NAME,
        naming_host: str | None = None,
        naming_port: int | None = None,
        *,
        clock: LamportClock | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.logical_name = logical_name
        self.naming_address = (naming_host or NAMING_SERVER_HOST,
                               naming_port or NAMING_SERVER_PORT)
        self.clock = clock or LamportClock()
        self._sensors: set[MessageConnection] = set()
        self._sensors_lock = threading.Lock()
        self._event_queue: list[tuple[tuple[int, str], dict]] = []
        self._queue_lock = threading.Lock()
        self._global_log: list[dict] = []
        self._log_lock = threading.Lock()
        self._shutdown_event = threading.Event()
```

**Threading Model:**
- **Main thread:** Runs `start()`, which calls `_register_with_naming_server()`, creates the server socket, and loops on `accept_connection()`.
- **Per-client threads:** Each accepted connection spawns a daemon thread (`_serve_sensor`) that reads messages until disconnect.
- **Status thread:** A daemon thread (`_status_broadcast_loop`) broadcasts `STATUS_UPDATE` every 5 seconds.

**Why daemon threads?** If the main thread exits (e.g., `KeyboardInterrupt`), daemon threads are killed automatically. This prevents the process from hanging on zombie workers.

### Startup Sequence

```python
def start(self) -> None:
    # Step 1: Register with Naming Server
    self._register_with_naming_server()

    # Step 2: Create TCP server socket
    self._server_socket = create_server_socket(self.host, self.port)

    # Step 3: Start status broadcast thread
    self._status_thread.start()

    # Step 4: Accept incoming sensor connections forever
    while not self._shutdown_event.is_set():
        try:
            connection = accept_connection(self._server_socket)
        except socket.timeout:
            continue
        worker = threading.Thread(target=self._serve_sensor, args=(connection,), daemon=True)
        worker.start()
```

#### Naming Server Registration

```python
def _register_with_naming_server(self) -> None:
    with create_client_connection(*self.naming_address) as conn:
        register_msg = build_register_message(
            self.logical_name, self.host, self.port, self.clock.send_event()
        )
        conn.send_message(register_msg)
        response = conn.receive_message()
        self.clock.receive_event(
            response.get("timestamp", response.get("lamport_timestamp", 0))
        )
```

The monitor registers under the logical name `emergency.monitor.main`. If registration fails (e.g., Naming Server is down), a warning is logged but the monitor still starts. This is defensive: the monitor can accept direct connections even if the Naming Server is temporarily unavailable.

### Serving a Sensor

```python
def _serve_sensor(self, connection: MessageConnection) -> None:
    with self._sensors_lock:
        self._sensors.add(connection)

    try:
        with connection:
            while not self._shutdown_event.is_set():
                try:
                    message = connection.receive_message()
                except socket.timeout:
                    continue
                except ConnectionClosedError:
                    break
                except Exception as exc:
                    self.logger.warning("Protocol error: %s", exc)
                    break

                self._handle_message(message, connection)
    except OSError as exc:
        self.logger.warning("Connection error: %s", exc)
    finally:
        with self._sensors_lock:
            self._sensors.discard(connection)
```

**Key design decisions:**
- The connection is added to `_sensors` **before** entering the receive loop. This ensures broadcasts include the new sensor immediately.
- `ConnectionClosedError` breaks the loop cleanly.
- `OSError` is caught and logged; the worker thread exits without crashing the server.
- The `finally` block ensures the connection is removed from `_sensors` even if an exception occurs.

### Handling an Alert

```python
def _handle_alert(self, alert: Mapping[str, Any], connection: MessageConnection) -> None:
    # Rule 3: Merge the sensor's Lamport timestamp into our clock
    self.clock.receive_event(alert["lamport_timestamp"])

    # Assign a unique alert ID for tracking
    alert_copy = dict(alert)
    alert_copy["alert_id"] = next_alert_id()

    # Push into the priority queue (min-heap)
    with self._queue_lock:
        heapq.heappush(self._event_queue, (alert_priority_key(alert_copy), alert_copy))

    # Append to the global event log
    with self._log_lock:
        self._global_log.append(alert_copy)

    # Send ACK back to the originating sensor
    ack = build_ack_message(
        alert_copy["alert_id"],
        alert_copy["sensor_id"],
        "received",
        self.clock.send_event(),  # Rule 2
    )
    connection.send_message(ack)

    # Broadcast the updated emergency sequence to ALL sensors
    self._broadcast_emergency_sequence()
```

**Why broadcast immediately?** The spec says "eventually broadcasts." For responsiveness and simplicity, we broadcast after every alert. This ensures all sensors see the latest ordered sequence as soon as possible.

### Broadcasting the Emergency Sequence

```python
def _broadcast_emergency_sequence(self) -> None:
    with self._queue_lock:
        ordered = [
            {
                "order": idx + 1,
                "sensor_id": alert["sensor_id"],
                "location": alert["location"],
                "lamport_timestamp": alert["lamport_timestamp"],
                "severity": alert["severity"],
            }
            for idx, (_, alert) in enumerate(
                sorted(self._event_queue, key=lambda item: item[0])
            )
        ]

    if not ordered:
        return

    message = build_emergency_sequence_message(
        sequence=ordered,
        lamport_timestamp=self.clock.send_event(),  # Rule 2
        message=f"Emergency sequence contains {len(ordered)} alert(s).",
    )

    with self._sensors_lock:
        sensors = list(self._sensors)

    failures = broadcast_message(sensors, message)
    for address, exc in failures:
        self.logger.warning("Failed to broadcast to %s: %s", address, exc)
```

**Important:** `sorted(self._event_queue, key=lambda item: item[0])` sorts by the heap key `(lamport_timestamp, sensor_id)`. We sort by `item[0]` (the key) rather than using `heapq.nsmallest` because `sorted` gives us the full ordered list for the broadcast.

### Status Broadcast Loop

```python
def _status_broadcast_loop(self) -> None:
    while not self._shutdown_event.is_set():
        self._shutdown_event.wait(STATUS_BROADCAST_INTERVAL_SECONDS)
        if self._shutdown_event.is_set():
            break
        self._broadcast_status()
```

- `STATUS_BROADCAST_INTERVAL_SECONDS` is `5.0` (from `config.py`).
- Uses `Event.wait()` instead of `time.sleep()` so the loop can be interrupted immediately on shutdown.

### Graceful Shutdown

```python
def shutdown(self) -> None:
    self._shutdown_event.set()
    if self._server_socket is not None:
        try:
            self._server_socket.close()
        except OSError:
            pass
```

- Setting the event signals all threads to exit.
- Closing the server socket unblocks the main thread's `accept_connection()` call.

---

## Thread Safety Analysis

| Shared Resource | Lock | Accessed By |
|-----------------|------|-------------|
| `_sensors` | `_sensors_lock` | Main thread (add), worker threads (remove), broadcast methods (read) |
| `_event_queue` | `_queue_lock` | Worker threads (push), broadcast methods (read) |
| `_global_log` | `_log_lock` | Worker threads (append) |
| `_client_threads` | `_client_threads_lock` | Main thread (add), worker threads (remove) |

**Why separate locks?** Using one global lock would create a bottleneck. `_sensors_lock` and `_queue_lock` can be held by different threads simultaneously, maximizing concurrency.

**Deadlock prevention:** Locks are always acquired in a consistent order, and never held while calling external code (like `connection.send_message()`).

---

## Testing

`tests/test_auction_server.py` covers:
- Initialization defaults
- Naming Server registration (mocked)
- Alert handling (clock, queue, log, ACK, broadcast)
- Multiple alert ordering by Lamport timestamp
- Heartbeat handling
- Status broadcast to multiple sensors
- Sensor connection tracking (add/remove)
- Graceful shutdown

`tests/test_integration.py` covers end-to-end scenarios:
- Alert → ACK + Emergency Sequence
- Multiple alerts with different Lamport timestamps
- Sensor Client sending alert to monitor
- Heartbeat processing without error
- Status broadcast reaching multiple clients

---

## Integration with Other Members

- **Member 1 (Registry Architect):** Registers with the Naming Server at startup; sensors use the Naming Server to find this monitor.
- **Member 2 (Middleware Engineer):** Uses `MessageConnection`, `create_server_socket`, `accept_connection`, `broadcast_message`, and all `build_*` message functions.
- **Member 3 (Timekeeper):** Uses `LamportClock` for all send/receive events and `alert_priority_key` / `event_sort_key` for heap ordering.
- **Member 4 (Client Developer):** Receives broadcasts from this server; sends ALERT and HEARTBEAT messages to it.

---

## Why This Design Works

1. **Integration Leadership:** The monitor ties together all three pillars. It is the only component that interacts with every other part of the system.
2. **Scalability:** Thread-per-client model allows simultaneous connections without blocking.
3. **Fault Tolerance:** Bad sensors cannot crash the server. Protocol errors are caught, logged, and the worker exits cleanly.
4. **Real-Time Broadcasting:** Sensors receive ordered emergency sequences immediately after an alert is processed.
5. **Deterministic Ordering:** The min-heap + tie-breaker guarantees all sensors see the exact same sequence.
