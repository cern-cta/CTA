# CTA Catalogue

The catalogue records CTA's tape copies, resources, and storage policies. A deployment normally shares one logical catalogue across its services, including services using different scheduler backends; see [Catalogue and scheduler topology](index.md#catalogue-and-scheduler-topology).

## Responsibilities

It stores file sizes and checksums, the tape and position of each copy, and information about tapes, drives, and libraries. It also holds storage classes, tape pools, archive routes, and mount policies; see [Storage Model](../data-management/storage-model.md) for their relationships.

The catalogue stores metadata, not file contents or the disk system's namespace. Requests, queues, and work coordination belong to the [Scheduler](scheduler.md).

## Relationships with services

The Workflow Frontend uses catalogue metadata to validate requests, while the Admin Frontend provides access to resource and policy configuration. Tape daemons record tape copies and update resource state; the maintenance daemon uses catalogue information for its background work.

## Database backends

The catalogue uses a relational database, with Oracle and PostgreSQL backends. See [Catalogue Configuration](../../ops/deploy-and-configure/configuration/catalogue.md) for setup, [Catalogue Upgrades](../../ops/run-and-maintain/upgrades/catalogue-schema/index.md) for schema migration, and the [developer reference](../../dev/reference/internals/catalogue/index.md#catalogue-description) for the database schema.

[Open catalogue schema ↗](../../dev/reference/internals/catalogue/db-schema.svg){ .md-button target="_blank" rel="noopener" title="Open the catalogue schema in a new tab" }
