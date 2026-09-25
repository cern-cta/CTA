# CTA Catalogue

The catalogue records CTA's tape copies, resources, and storage policies.

## Responsibilities

It stores file sizes and checksums, the tape and position of each copy, and information about tapes, drives, and libraries. It also holds storage classes, tape pools, archive routes, and mount policies; see [Storage Model](../data-management/storage-model.md) for their relationships.

The catalogue stores metadata, not file contents or the disk system's namespace. Requests, queues, and work coordination belong to the [Scheduler](scheduler.md).

## Relationships with services

The Workflow API uses catalogue metadata to validate requests, while the Admin API provides access to resource and policy configuration. Tape daemons record tape copies and update resource state; the Maintenance Daemon uses catalogue information for its background work.

## Database backends

The catalogue uses a relational database, with Oracle and PostgreSQL backends. See [Catalogue Configuration](../../ops/configuration/catalogue.md) for setup, [Catalogue Upgrades](../../ops/upgrades/catalogue-schema/index.md) for schema migration, and the [developer reference](../../dev/internals/catalogue/index.md#catalogue-description) for the database schema.

[Open catalogue schema ↗](../../dev/internals/catalogue/db-schema.svg){ .md-button target="_blank" rel="noopener" title="Open the catalogue schema in a new tab" }
