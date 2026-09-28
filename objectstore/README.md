# Objectstore

The objectstore scheduler persists objects using the protobuf definitions in [cta.proto](cta.proto). Each object has a common `ObjectHeader` identifying its type and an encoded payload using that type's schema.

Processes generate unique identifiers from their type, optional identifying information, hostname, PID, and process start time. Object names combine their type, optional identifying information, the creating process identifier, and a process-local sequence number. The timestamp identifies process startup, not object creation.

See [Scheduler internals](../docs/content/dev/internals/components/scheduler/index.md) for backend context. For read-only inspection of objects, queue shards, and agent-owned requests, see [cta-objectstore-dump-object](../docs/content/ops/tools/cta-objectstore-dump-object.md#inspecting-queues-and-requests).
