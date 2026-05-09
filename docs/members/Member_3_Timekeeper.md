# Member 3: The Timekeeper (Lead Synchronization Developer)

**Role:** Implement Lamport Logical Clocks across all nodes and design the fair event ordering logic inside the Central Monitor.

**Files Owned:** `lamport_clock.py`

**Supporting Files Used:** `config.py` (constants only)

---

## Overview

The Timekeeper is responsible for **Pillar 3: Synchronization**. In a distributed system, physical time (wall-clock time) is unreliable for ordering events because:

- Different machines have slightly different clocks.
- Network delays cause messages to arrive out of order.
- Two events may appear simultaneous when they are not.

**Lamport Logical Clocks** solve this by attaching a monotonic integer timestamp to every event. These timestamps establish a **partial ordering** of events that all nodes can agree on, regardless of network latency.

In DEAN, this means that even if `sensor_03`'s alert arrives at the monitor **after** `sensor_01`'s alert (because of network lag), the monitor can still determine that `sensor_03` detected the fire first — as long as `sensor_03`'s Lamport timestamp is lower.

---

## Architecture

```
+--------------------------------------------------+
|              LamportClock (per node)               |
|                                                    |
|  Rule 1: local_event()  -> clock += 1              |
|  Rule 2: send_event()   -> clock += 1              |
|  Rule 3: receive_event(ts) -> clock = max(clock,ts)+1 |
|                                                    |
|  Thread-safe via threading.Lock()                  |
+--------------------------------------------------+
```

Every node in DEAN maintains its own `LamportClock` instance:
- **Naming Server** — advances on every request/response.
- **Central Monitor** — advances on every alert received and every broadcast sent.
- **Sensor Clients** — advance on every alert sent, every heartbeat sent, and every message received.

---

## Key Components

### `LamportClock`

```python
class LamportClock:
    def __init__(self, initial_time: int = 0) -> None:
        self._clock = _normalize_timestamp(initial_time)
        self._lock = threading.Lock()
```

**Thread Safety:**
- Every method that reads or writes `self._clock` acquires `self._lock`.
- This is essential because sensors and monitors access their clocks from multiple threads (e.g., heartbeat thread + receive thread + main thread).

**The Three Rules:**

#### Rule 1: Local Event

```python
def local_event(self) -> int:
    with self._lock:
        self._clock += 1
        return self._clock
```

Used when something happens locally that needs to be ordered relative to other events. In DEAN, this is rarely used directly; most events are sends or receives.

#### Rule 2: Send Event

```python
def send_event(self) -> int:
    return self.local_event()
```

Before sending any message, the sender increments its clock and attaches the new value to the message as `lamport_timestamp`.

**Example in Sensor Client:**

```python
msg = build_alert_message(
    sensor_id="sensor_01",
    location="Floor 2",
    severity="critical",
    lamport_timestamp=self.clock.send_event(),  # Rule 2
    payload="Smoke detected"
)
```

#### Rule 3: Receive Event

```python
def receive_event(self, received_timestamp: int) -> int:
    normalized = _normalize_timestamp(received_timestamp)
    with self._lock:
        self._clock = max(self._clock, normalized) + 1
        return self._clock
```

When a node receives a message, it updates its clock to be **at least** as large as the sender's timestamp, plus one. This ensures causality is preserved: if event A happened before event B (and A's timestamp was sent to B), then B's clock will be strictly greater than A's.

**Example in Central Monitor:**

```python
def _handle_alert(self, alert, connection):
    # Rule 3: Merge the sensor's timestamp into our clock
    self.clock.receive_event(alert["lamport_timestamp"])
    ...
    # Rule 2: Increment before sending ACK
    ack = build_ack_message(..., lamport_timestamp=self.clock.send_event())
```

### `sync_with()` Helper

```python
def sync_with(self, other: "LamportClock") -> int:
    return self.receive_event(other.peek())
```

Synchronizes this clock with another `LamportClock` instance. Useful for testing and for merging clocks during integration.

---

## Event Ordering

### `event_sort_key()`

```python
def event_sort_key(lamport_timestamp: int, sensor_id: str) -> tuple[int, str]:
    if not sensor_id:
        raise ValueError("sensor_id is required for Lamport event ordering.")
    return _normalize_timestamp(lamport_timestamp), str(sensor_id)
```

Returns a tuple that can be used for sorting or as a dictionary key. Tuples in Python compare lexicographically: first element, then second element.

### `alert_priority_key()`

```python
def alert_priority_key(alert: Mapping[str, Any]) -> tuple[int, str]:
    timestamp = alert["lamport_timestamp"]
    sensor_id = alert["sensor_id"]
    return event_sort_key(timestamp, sensor_id)
```

Extracts the sort key from an alert dictionary. This is the key function used by the Central Monitor's min-heap.

### `compare_logical_order()`

```python
def compare_logical_order(
    left_timestamp: int, left_sensor_id: str,
    right_timestamp: int, right_sensor_id: str
) -> int:
    left_key = event_sort_key(left_timestamp, left_sensor_id)
    right_key = event_sort_key(right_timestamp, right_sensor_id)
    if left_key < right_key: return -1
    if left_key > right_key: return 1
    return 0
```

Returns `-1`, `0`, or `1` for strict three-way comparison. Used in tests and for deterministic ordering logic.

---

## Tie-Breaking: Why `sensor_id` Matters

Two alerts can have the **same Lamport timestamp** if:
- Two sensors detect a fire at truly the same logical moment, or
- The sensors' clocks happen to converge to the same value.

If timestamps are equal, the ordering would be non-deterministic — violating the rubric's requirement for fair event ordering. We solve this by using `sensor_id` as a **deterministic tie-breaker**:

```python
heapq.heappush(event_queue, (alert_priority_key(alert), alert))
# alert_priority_key returns: (lamport_timestamp, sensor_id)
```

Since `sensor_id` is a string and strings are comparable in Python, the heap always produces a consistent total order:

1. Lower Lamport timestamp wins.
2. If timestamps are equal, lower `sensor_id` (lexicographically) wins.

**Example:**
- `sensor_02` sends alert at timestamp `5`
- `sensor_01` sends alert at timestamp `5`

Heap order: `sensor_01` comes before `sensor_02` because `"sensor_01" < "sensor_02"`.

---

## The Central Monitor's Priority Queue

The Timekeeper's work culminates in the Central Monitor's event queue:

```python
self._event_queue: list[tuple[tuple[int, str], dict[str, Any]]] = []
```

When an alert arrives:

```python
heapq.heappush(self._event_queue, (alert_priority_key(alert_copy), alert_copy))
```

When broadcasting the emergency sequence:

```python
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
```

This produces a deterministic, globally agreed-upon ordering of all alerts.

---

## Testing

`tests/test_lamport.py` covers:
- Clock rules 1, 2, and 3
- `receive_event` with negative timestamps (rejected)
- Tie-breaking with `sensor_id`
- Priority key matching heap order

---

## Integration with Other Members

- **Member 5 (Server Developer):** The Central Monitor imports `LamportClock`, `alert_priority_key`, and `event_sort_key` to maintain its queue and broadcast ordered sequences.
- **Member 4 (Client Developer):** Sensor Clients import `LamportClock` to timestamp every `ALERT` and `HEARTBEAT`.
- **Member 1 (Registry Architect):** The Naming Server imports `LamportClock` to timestamp `REGISTER` and `LOOKUP` responses.
- **Member 2 (Middleware Engineer):** `message_protocol.py` validates that `lamport_timestamp` fields are integers, ensuring clocks cannot send malformed timestamps.

---

## Why This Design Works

1. **Thread Safety:** Every clock operation is atomic thanks to `threading.Lock()`.
2. **Determinism:** The tie-breaker guarantees a total order even when timestamps collide.
3. **Simplicity:** Lamport clocks are lightweight (a single integer) compared to vector clocks or physical timestamps.
4. **Causality Tracking:** Rule 3 ensures that if event A causally precedes event B, then `timestamp(A) < timestamp(B)`.
