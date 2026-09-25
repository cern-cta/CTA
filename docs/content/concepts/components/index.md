# Component Overview

CTA services coordinate tape storage through shared catalogue and scheduler backends. The disk buffer is a separate system integrated with CTA.

## Services

- [Workflow API](workflow-api.md): accepts archive, retrieve, and delete requests from the disk system.
- [Admin API](admin-api.md): serves operator commands, including those from `cta-admin`.
- [Tape Daemon](tape-daemon.md): selects and executes tape work and transfers files between tape and disk.
- [Maintenance Daemon](maintenance-daemon.md): runs background reporting, repack, and scheduler maintenance routines.
- [Media Changer Daemon](media-changer-daemon.md): provides access to tape-library robotics.

## Databases and backends

- [Catalogue](catalogue.md): stores persistent metadata for files, tapes, libraries, and policies.
- [Scheduler](scheduler.md): queues archive and retrieve work and coordinates its assignment to tape drives.

## Integration and access boundaries

The [Disk Buffer](disk-buffer.md) manages the client-facing namespace and disk replicas. [Authentication](authentication.md) explains access boundaries between the disk system, CTA services, and operators.

See [System Architecture](../architecture.md) for the connections between these components.
