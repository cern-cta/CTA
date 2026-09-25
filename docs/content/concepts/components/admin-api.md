# Admin API

The Admin API serves operator commands from `cta-admin` and other administrative tools. It provides access to CTA resource management and operational state independently of disk-system workflow requests.

## Responsibilities

Operators use the API to inspect and manage tapes, drives, libraries, and storage policies, and to inspect queues and initiate operations such as repacking. Commands query or update the catalogue and scheduler; accepting a command that schedules work does not mean that work has finished.

The API provides the administrative interface rather than performing tape transfers or controlling library robotics itself. Those actions are carried out by the corresponding services.

## Relationships with other components

The [Catalogue](catalogue.md) holds resource and policy records, while the [Scheduler](scheduler.md) tracks queued work. The [Workflow API](workflow-api.md) handles the disk system's archive, retrieve, and delete requests separately.

Administrative access has its own [authentication boundary](authentication.md). See [Admin API Configuration](../../ops/configuration/admin-api.md) for settings and [cta-admin](../../ops/tools/cta-admin.md) for command reference.
