# Scheduler

The scheduler coordinates archive, retrieve, and repack work across CTA services.

## Responsibilities

It tracks queued requests and combines catalogue policies with resource availability to select eligible work and determine which tapes to mount. Tape daemons perform the actual transfers. See [Scheduling](../data-management/scheduling.md) for batching, priorities, and mount selection.

## Relationships with services

The Workflow API queues requests, tape daemons select and process work through the scheduler, and the maintenance daemon handles reporting and background maintenance. Operators inspect and manage scheduler state through the Admin API.

## Backends

CTA supports objectstore and PostgreSQL scheduler backends. They persist requests, queues, and coordination state so work survives service restarts. This state is separate from the [Catalogue](catalogue.md), even when both use PostgreSQL. A deployment can use independent scheduler backends sharing one catalogue, for example to separate repack from ordinary archival and retrieval. Each backend has its own queued work and associated service connections; see [Catalogue and scheduler topology](index.md#catalogue-and-scheduler-topology).

See [Scheduler Configuration](../../ops/configuration/scheduler.md) for setup and [Scheduling and Queues](../../ops/administration/requests.md) for operator procedures.
