# Setup Instructions

## Prerequisites

- Python 3.10 or newer

## Project Setup

1. Open a terminal in the project root.
2. Run the test suite:



## Current Milestone

The DEAN foundation and Naming Server are ready:

- configuration constants
- Lamport clock logic
- JSON protocol helpers
- TCP messaging layer
- Naming Server registry and request handling

Try the Naming Server with:

```bash
python3 naming_server.py
```

The next milestone is implementing the Central Monitoring server and Sensor
client on top of that foundation.
