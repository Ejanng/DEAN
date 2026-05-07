# DEAN Message Protocol

DEAN uses line-delimited JSON over TCP sockets. Each complete message ends with
a newline so the receiver can reconstruct messages even when TCP packets arrive
in partial chunks.

## Core Types

- `REGISTER`
- `REGISTER_RESPONSE`
- `LOOKUP`
- `LOOKUP_RESPONSE`
- `DEREGISTER`
- `DEREGISTER_RESPONSE`
- `ALERT`
- `ACK`
- `HEARTBEAT`
- `STATUS_UPDATE`
- `EMERGENCY_SEQUENCE`
- `ERROR`

## Validation Rules

- Every message must contain a recognized `type`.
- Integer fields such as `timestamp`, `port`, and `lamport_timestamp` must be
  real integers.
- `EMERGENCY_SEQUENCE.sequence` must be a list.
- The Naming Server replies to successful registrations and deregistrations
  with explicit registry response messages.

The canonical builders and validators live in [message_protocol.py](/home/ejang/Desktop/DS/final/message_protocol.py).
