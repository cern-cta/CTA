# Tool Index

Choose a tool by task. Follow the linked reference for syntax, prerequisites, and options. End-to-end procedures belong in [Administration](../administration/index.md); see [Installation and Shared Configuration](installation-and-configuration.md) for tool installation.

Schema deletion and scheduler repair/reset commands modify or remove state. Their inclusion here does not make them routine inspection commands. Scheduler tools depend on the backend selected when CTA was built.

## Administration

| Tool | Purpose |
| --- | --- |
| [cta-admin](cta-admin.md) | Inspect and manage CTA resources, policies, and requests. |
| [cta-ops-admin](cta-ops-admin.md) | Perform higher-level operator tasks through the CTA administration wrapper. |
| [cta-versionlock](cta-versionlock.md) | Inspect and manage RPM dependency version locks. |

## Database & Schema Management

| Tool | Purpose |
| --- | --- |
| [cta-catalogue-admin-user-create](cta-catalogue-admin-user-create.md) | Create an initial catalogue administrator. |
| [cta-catalogue-schema-create](cta-catalogue-schema-create.md) | Create a catalogue schema. |
| [cta-database-poll](cta-database-poll.md) | Wait for database connectivity. |
| [cta-scheduler-schema-create](cta-scheduler-schema-create.md) | Create a PostgreSQL scheduler schema. |
| [cta-scheduler-schema-drop](cta-scheduler-schema-drop.md) | Delete a PostgreSQL scheduler schema and its data. |
| [cta-catalogue-schema-set-production](cta-catalogue-schema-set-production.md) | Set the catalogue production-protection flag. |
| [cta-catalogue-schema-verify](cta-catalogue-schema-verify.md) | Check catalogue schema consistency. |
| [cta-catalogue-schema-drop](cta-catalogue-schema-drop.md) | Delete a catalogue schema and its data. |

## Tape Operations

| Tool | Purpose |
| --- | --- |
| [cta-readtp](cta-readtp.md) | Read files directly from tape. |
| [cta-tape-label](cta-tape-label.md) | Write tape labels. |
| [cta-smc](cta-smc.md) | Control tape-library media moves. |
| [cta-ops-repack (ATRESYS)](repack-automation.md) | Coordinate automated tape repacking. |
| [cta-ops-drive-config-generate](cta-ops-drive-config-generate.md) | Generate drive configuration from hardware and library information. |
| [cta-ops-pool-supply](cta-ops-pool-supply.md) | Supply tapes to destination tape pools. |
| [cta-ops-tape-verify](tape-verification.md) | Coordinate tape-data verification. |

## Monitoring & Statistics

| Tool | Purpose |
| --- | --- |
| [cta-statistics-save](cta-statistics-save.md) | Export catalogue statistics as JSON. |
| [cta-statistics-update](cta-statistics-update.md) | Refresh cached catalogue tape statistics. |
| [cta-ops-drive-environmentals](cta-ops-drive-environmentals.md) | Collect drive temperature and humidity information. |
| [cta-ops-data-monitoring](cta-ops-data-monitoring.md) | Monitor data transferred during CTA sessions. |

## Scheduler Management & Diagnostics

| Tool | Purpose |
| --- | --- |
| [cta-objectstore-collect-orphaned-object](cta-objectstore-collect-orphaned-object.md) | Garbage-collect an orphaned scheduler object; may modify or delete data. |
| [cta-objectstore-create-missing-repack-index](cta-objectstore-create-missing-repack-index.md) | Recreate the scheduler repack index; changes scheduler metadata. |
| [cta-objectstore-dereference-removed-queues](cta-objectstore-dereference-removed-queues.md) | Remove references to missing scheduler queues. |
| [cta-objectstore-initialize](cta-objectstore-initialize.md) | Initialise a new scheduler objectstore. |
| [cta-objectstore-list](cta-objectstore-list.md) | List scheduler object names for inspection. |
| [cta-objectstore-reset](cta-objectstore-reset.md) | Delete all objects in the selected scheduler store. |
| [cta-objectstore-dump-object](cta-objectstore-dump-object.md) | Inspect a scheduler object and its contents. |

## Service Manuals

| Tool | Purpose |
| --- | --- |
| [cta-frontend](service-manuals/cta-frontend.md) | Command and option reference for the current shared API implementation. |
| [cta-taped](service-manuals/cta-taped.md) | Command and option reference for the tape daemon. |
| [cta-maintd](service-manuals/cta-maintd.md) | Command and option reference for the maintenance daemon. |
| [cta-rmcd](service-manuals/cta-rmcd.md) | Command and option reference for the media changer daemon. |

## Disk-system tools

See [EOS Operator Utilities](../integrations/eos/tools.md) for tools specific to EOS metadata and integration.
