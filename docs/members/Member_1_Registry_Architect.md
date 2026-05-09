# Member 1: The Registry Architect (Lead Naming Developer)

**Role:** Design, build, and maintain the standalone Naming Server that acts as the system's DNS-like directory service.

**Files Owned:** `naming_server.py`

**Supporting Files Used:** `config.py`, `message_protocol.py`, `network_layer.py`, `lamport_clock.py`, `utils.py`

---

## Overview

The Naming Server is the foundation of **Pillar 1: Naming**. Its job is simple but critical: **no component in the DEAN system is allowed to hardcode an IP address or port**. Every node — whether the Central Monitor or a Sensor Client — must ask the Naming Server to resolve logical names into physical network addresses.

This ensures **location independence**. If the Central Monitor crashes and restarts on a different port, sensors do not need to be reconfigured. They simply query the Naming Server again and receive the updated address.

---

## Architecture

```
+--------------------------------------------------+
|  Naming Server  (naming_server.py : port 8000)   |
|                                                  |
|  +-----------------------------------------+     |
|  |  NamingRegistry (thread-safe dict)      |     |
|  |  "emergency.monitor.main" ->            |     |
|  |    RegistryEntry(ip, port)              |     |
|  +-----------------------------------------+     |
|                                                  |
|  TCP Listener (per-client daemon threads)        |
+--------------------------------------------------+
```

---

## Key Components

### `RegistryEntry` (dataclass)

An immutable, hashable record that stores a single name-to-address mapping.

```python
@dataclass(frozen=True, slots=True)
class RegistryEntry:
    logical_name: str
    ip: str
    port: int
```

- `frozen=True` prevents accidental mutation after creation.
- `slots=True` reduces memory overhead (minor but good practice).

### `NamingRegistry`

The core in-memory dictionary wrapped in a thread-safe API.

```python
class NamingRegistry:
    def __init__(self) -> None:
        self._entries: dict[str, RegistryEntry] = {}
        self._lock = threading.Lock()
```

**Why a Lock?**  
The Naming Server spawns one daemon thread per client connection. If two sensors try to register or lookup the same name simultaneously, the dictionary could corrupt without locking.

**Public Methods:**

| Method | Returns | Description |
|--------|---------|-------------|
| `register(name, ip, port)` | `(RegistryEntry, bool)` | Adds or updates an entry. `bool` is `True` if the name already existed. |
| `lookup(name)` | `RegistryEntry \| None` | Resolves a logical name to its physical address. |
| `deregister(name)` | `RegistryEntry \| None` | Removes an entry. Returns `None` if the name was not found. |
| `snapshot()` | `dict` | Returns a defensive copy of the entire registry for debugging. |

**Validation:**
- `logical_name` must be a non-empty string.
- `ip` must be a non-empty string.
- `port` must be an integer between 1 and 65535.

If validation fails, a `ValueError` is raised and the server translates it into an `ERROR` response message.

### `NamingServer`

The TCP server that accepts connections and dispatches requests.

```python
class NamingServer:
    def __init__(self, host, port, registry=None, clock=None) -> None:
        self.host = host
        self.port = port
        self.registry = registry or NamingRegistry()
        self.clock = clock or LamportClock()
        self._shutdown_event = threading.Event()
```

**Threading Model:**
- **Main thread:** Runs `serve_forever()`, which loops on `accept_connection()`.
- **Per-client threads:** Each incoming connection spawns a daemon thread (`self._serve_connection`) that reads messages until the client disconnects or a protocol error occurs.

**Request Handling:**

```python
def handle_request(self, request: Mapping[str, Any]) -> dict[str, Any]:
    # 1. Update Lamport clock from the incoming message
    self.clock.receive_event(self._extract_timestamp(request))

    # 2. Dispatch by message type
    if request_type == REGISTER:
        entry, updated = self.registry.register(...)
        return build_register_response(..., self.clock.send_event())
    elif request_type == LOOKUP:
        entry = self.registry.lookup(...)
        return build_lookup_response(..., self.clock.send_event())
    elif request_type == DEREGISTER:
        removed = self.registry.deregister(...)
        return build_deregister_response(..., self.clock.send_event())
```

**Clock Discipline:**
- On **receive**: `clock.receive_event(timestamp_from_request)`
- On **send**: `clock.send_event()` (which internally calls `local_event()`)

This ensures the Naming Server's Lamport clock advances whenever it processes any request.

---

## Protocol Messages Handled

### `REGISTER` (Monitor → Naming Server)

```json
{
  "type": "REGISTER",
  "logical_name": "emergency.monitor.main",
  "ip": "127.0.0.1",
  "port": 9000,
  "timestamp": 1
}
```

**Response:** `REGISTER_RESPONSE` with status `"registered"` or `"updated"`.

### `LOOKUP` (Sensor → Naming Server)

```json
{
  "type": "LOOKUP",
  "logical_name": "emergency.monitor.main",
  "timestamp": 2
}
```

**Response:** `LOOKUP_RESPONSE` with resolved `ip` and `port`. If the name is missing, an `ERROR` response is returned.

### `DEREGISTER` (Monitor → Naming Server)

```json
{
  "type": "DEREGISTER",
  "logical_name": "emergency.monitor.main",
  "timestamp": 99
}
```

**Response:** `DEREGISTER_RESPONSE` with status `"deregistered"`. If the name was not found, an `ERROR` response is returned.

---

## Error Handling

The Naming Server never crashes on malformed input. Instead:

1. `ProtocolError` (bad JSON or unknown message type) → sends `ERROR` message and closes the connection.
2. `ValueError` (invalid name, IP, or port) → sends `ERROR` message with details.
3. `OSError` (socket failure) → logs a warning and continues accepting new connections.

---

## Integration with Other Members

- **Member 5 (Server Developer):** The Central Monitor calls `_register_with_naming_server()` at startup to insert its address into the registry.
- **Member 4 (Client Developer):** Sensor Clients call `_lookup_monitor_address()` at startup to query the registry.
- **Member 2 (Middleware Engineer):** The Naming Server uses `MessageConnection`, `accept_connection`, and `create_server_socket` from `network_layer.py`.
- **Member 3 (Timekeeper):** The Naming Server maintains its own `LamportClock` and advances it on every request.

---

## Why This Design Works

1. **Location Independence:** The registry is dynamic. Entries can be added, updated, or removed at runtime.
2. **Thread Safety:** All shared state is protected by `threading.Lock()`.
3. **Validation at Boundaries:** Every incoming request is validated before touching the registry.
4. **Graceful Degradation:** Errors are logged and returned to the client; the server never stops.
