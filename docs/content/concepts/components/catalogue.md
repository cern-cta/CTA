# CTA Catalogue

The **catalogue** is CTA's persistent record of tape copies, resources and policies. Services normally share one logical catalogue, even when they use separate scheduler backends; see [Catalogue and scheduler topology](index.md#catalogue-and-scheduler-topology).

## Responsibilities

It stores file sizes and checksums, the tape and position of each copy, and information about tapes, drives, and libraries. It also holds storage classes, tape pools, archive routes, and mount policies; see [Storage Model](../data-management/storage-model.md) for their relationships.

The catalogue stores metadata, not file contents or the disk system's namespace. Requests, queues, and work coordination belong to the [Scheduler](scheduler.md).

## Relationships with services

| Service | Uses the catalogue to… |
| --- | --- |
| Workflow Frontend | Validate file identities and policies. |
| Admin Frontend | Inspect and manage resources and policies. |
| Tape daemon | Locate copies, record successful writes and update drive/tape state. |
| Maintenance daemon | Obtain metadata needed for reporting, repack and background work. |

## Database backends

The catalogue uses a relational database, with Oracle and PostgreSQL backends. See [Catalogue Configuration](../../ops/deploy-and-configure/configuration/catalogue.md) for setup, [Catalogue Upgrades](../../ops/run-and-maintain/upgrades/catalogue-schema/index.md) for schema migration, and the [developer reference](../../dev/internals/components/catalogue/index.md#catalogue-description) for the database schema.

[Open catalogue schema ↗](../../dev/internals/components/catalogue/db-schema.svg){ .md-button target="_blank" rel="noopener" title="Open the catalogue schema in a new tab" }
