# Member 2: The Middleware Engineer (Lead Communications Developer)

**Role:** Design the underlying network communication layer and JSON message protocol that all nodes use to talk to each other.

**Files Owned:** `network_layer.py`, `message_protocol.py`

**Supporting Files Used:** `config.py`

---

## Overview

The Middleware layer is the backbone of **Pillar 2: Message-Oriented Communication**. Every node in DEAN — Naming Server, Central Monitor, and Sensor Clients — relies on these two files to send and receive data. Without a solid middleware layer, messages would be lost, corrupted, or delivered out of order.

Our design goals were:
1. **Reliability** — Handle partial TCP reads gracefully.
2. **Validation** — Reject malformed messages before they reach business logic.
3. **Broadcast** — Allow the server to push the same message to many clients efficiently.
4. **Zero Dependencies** — Use only the Python standard library.

---

## File 1: `message_protocol.py`

### Purpose

Defines every message type in the DEAN system, provides builder functions, and enforces validation rules. **No node should ever construct a raw dictionary and send it manually.** All messages must be built using the `build_*` functions so validation runs automatically.

### Message Types

There are 11 message types defined as module-level constants:

```python
REGISTER = "REGISTER"
REGISTER_RESPONSE = "REGISTER_RESPONSE"
LOOKUP = "LOOKUP"
LOOKUP_RESPONSE = "LOOKUP_RESPONSE"
DEREGISTER = "DEREGISTER"
DEREGISTER_RESPONSE = "DEREGISTER_RESPONSE"
ALERT = "ALERT"
ACK = "ACK"
HEARTBEAT = "HEARTBEAT"
STATUS_UPDATE = "STATUS_UPDATE"
EMERGENCY_SEQUENCE = "EMERGENCY_SEQUENCE"
ERROR = "ERROR"
```

### Validation Rules

Every message must pass `validate_message()` before it can be serialized or deserialized. The validator checks:

1. **Type existence:** The `type` field must be one of the 11 known types.
2. **Required fields:** Each type has a set of mandatory fields. Missing fields raise `ProtocolError`.
3. **Integer types:** Fields like `timestamp`, `port`, `lamport_timestamp` must be `int`, not `float` or `str`.
4. **List types:** The `sequence` field in `EMERGENCY_SEQUENCE` must be a list.

Example required fields for `ALERT`:

```python
ALERT: {
    "type",
    "sensor_id",
    "location",
    "severity",
    "lamport_timestamp",
    "physical_time",
    "payload",
}
```

### Serialization

Messages are serialized into **compact JSON** with deterministic key ordering:

```python
def serialize_message(message: Mapping[str, Any]) -> str:
    payload = validate_message(message)
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)
```

- `separators=(",", ":")` removes unnecessary whitespace.
- `sort_keys=True` ensures identical dictionaries always produce identical JSON strings.

### Wire Encoding

Each message is terminated by `\n` (line feed) for line-delimited framing:

```python
def encode_message(message: Mapping[str, Any]) -> bytes:
    serialized = serialize_message(message)
    return f"{serialized}{MESSAGE_DELIMITER}".encode(JSON_ENCODING)
```

### Decoding

```python
def decode_message(raw_message: str | bytes) -> dict[str, Any]:
    # 1. Decode bytes to UTF-8 if necessary
    # 2. Strip whitespace and parse JSON
    # 3. Validate the resulting dictionary
```

If any step fails, a `ProtocolError` is raised with a descriptive message.

### Builder Functions

For every message type, there is a `build_*` function:

```python
def build_alert_message(
    sensor_id: str,
    location: str,
    severity: str,
    lamport_timestamp: int,
    payload: str,
    physical_time: str | None = None,
) -> dict[str, Any]:
    return validate_message({
        "type": ALERT,
        "sensor_id": sensor_id,
        "location": location,
        "severity": severity,
        "lamport_timestamp": lamport_timestamp,
        "physical_time": physical_time or utc_now_iso(),
        "payload": payload,
    })
```

**Why builders matter:**
- They ensure no field is forgotten.
- They run validation immediately, catching bugs at the call site rather than at the network boundary.
- They centralize message structure so changes only happen in one place.

---

## File 2: `network_layer.py`

### Purpose

Wraps Python's `socket` module with line-delimited JSON send/receive helpers. Provides server socket creation, client connection creation, and broadcast utilities.

### `MessageConnection`

The core class. Wraps a `socket.socket` and provides:

```python
class MessageConnection:
    def __init__(self, sock, address=None, *, buffer_size, delimiter, encoding, timeout):
        self.socket = sock
        self.address = address
        self._buffer = ""  # Internal read buffer for partial messages
```

#### Sending

```python
def send_message(self, message: Mapping[str, Any]) -> None:
    payload = encode_message(validate_message(message))
    self.socket.sendall(payload)
```

- `validate_message()` runs first (defense in depth).
- `sendall()` ensures the entire payload is transmitted, even if the kernel buffer is full.

#### Receiving

```python
def receive_message(self) -> dict[str, Any]:
    while True:
        # 1. Check if a complete message is already in the buffer
        if self.delimiter in self._buffer:
            raw_message, self._buffer = self._buffer.split(self.delimiter, 1)
            if not raw_message.strip():
                continue
            return decode_message(raw_message)

        # 2. Read more data from the socket
        chunk = self.socket.recv(self.buffer_size)
        if not chunk:
            raise ConnectionClosedError("Connection closed by peer.")

        # 3. Append to buffer
        self._buffer += chunk.decode(self.encoding)
```

**Why this design works:**
- TCP is a stream protocol, not a message protocol. A single `send()` may be split across multiple `recv()` calls, or multiple sends may arrive in a single `recv()`.
- The internal `_buffer` accumulates bytes until a full `\n`-terminated message is available.
- Empty lines are skipped (defensive against stray newlines).

#### Context Manager

```python
def __enter__(self) -> "MessageConnection":
    return self

def __exit__(self, exc_type, exc, tb) -> None:
    self.close()
```

This allows safe usage:

```python
with create_client_connection("127.0.0.1", 8000) as conn:
    conn.send_message(lookup_msg)
    response = conn.receive_message()
```

### Server Socket Helper

```python
def create_server_socket(host, port, *, backlog=20, timeout=1.0) -> socket.socket:
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))
    server_socket.listen(backlog)
    server_socket.settimeout(timeout)
    return server_socket
```

- `SO_REUSEADDR` allows rapid restart without "Address already in use" errors.
- `settimeout(1.0)` makes `accept()` non-blocking for shutdown checks.

### Client Connection Helper

```python
def create_client_connection(host, port, *, timeout=1.0) -> MessageConnection:
    client_socket = socket.create_connection((host, port), timeout=timeout)
    return MessageConnection(client_socket, address=(host, port), timeout=timeout)
```

### Broadcast Helper

```python
def broadcast_message(connections, message) -> list[tuple[address, Exception]]:
    validated = validate_message(message)
    failures = []
    for connection in connections:
        try:
            connection.send_message(validated)
        except (OSError, ProtocolError) as exc:
            failures.append((connection.address, exc))
    return failures
```

**Key design decision:** Failures are collected, not raised. If one sensor has a dead socket, the broadcast continues to all others. The caller (e.g., the Central Monitor) logs the failures but stays alive.

---

## Error Handling

| Exception | Cause | Handling |
|-----------|-------|----------|
| `ProtocolError` | Bad JSON, unknown type, missing field, wrong type | Raised by `validate_message`; caught by servers and sent back as `ERROR` message |
| `ConnectionClosedError` | Peer closed socket mid-message | Raised by `receive_message`; caught by server workers to exit gracefully |
| `socket.timeout` | No data within timeout period | Caught by callers to continue loops (e.g., check shutdown event) |
| `OSError` | Broken pipe, network failure | Caught by broadcast and logged; caught by workers to clean up |

---

## Testing

- `tests/test_message_protocol.py` — Builder round-trips, validation rejection, physical time generation.
- `tests/test_network.py` — Partial read reassembly, broadcast to multiple connections, broadcast failure collection, socket creation mocks.

A `FakeSocket` class is used to mock socket behavior without real TCP connections.

---

## Integration with Other Members

- **Member 1 (Registry Architect):** Uses `MessageConnection` and `accept_connection` in `naming_server.py`.
- **Member 5 (Server Developer):** Uses `broadcast_message`, `create_server_socket`, and `accept_connection` in `auction_server.py`.
- **Member 4 (Client Developer):** Uses `create_client_connection` in `bidder_client.py`.
- **Member 3 (Timekeeper):** The timestamp fields validated by `message_protocol.py` are generated by `LamportClock`.

---

## Why This Design Works

1. **Separation of Concerns:** Message structure (protocol) is decoupled from transport (network layer).
2. **Defense in Depth:** Messages are validated at build time, at send time, and at receive time.
3. **Resilience:** Partial reads, malformed data, and dead connections are handled without crashing the server.
4. **Simplicity:** Zero external dependencies means the system runs anywhere Python 3.10+ is installed.
