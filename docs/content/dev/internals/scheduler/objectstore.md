# Objectstore Scheduler Backend

!!! info "Documentation outline"
    Detailed procedures will be completed and validated during the content review.

The objectstore backend stores scheduler work and coordination state as objects. Its implementation is under `objectstore/`; the PostgreSQL backend has a [separate guide](postgresql/index.md).

## Objects and queues

Explain the root entry, request and queue objects, registers, and the relationships used to locate scheduled work.

## Ownership and concurrency

Explain agent ownership, locking, and updates across objects, including how interrupted operations are handled.

## Reporting and garbage collection

Trace completed work, reporting queues, orphan recovery, and queue cleanup. Connect these mechanisms to the [Maintenance Daemon](../maintenance-daemon.md).

## Storage backends and tests

Describe supported storage implementations and how to exercise them in tests. Deployment settings belong in [Scheduler Configuration](../../../ops/configuration/scheduler.md).
