# Workflow Frontend {#workflow-api}

The Workflow Frontend connects the disk system to CTA. It accepts workflow requests over gRPC, while the disk system provides the client-facing namespace and interface.

## Responsibilities

The API handles file registration, archival, retrieval, deletion, and retrieval cancellation. It validates requests against catalogue metadata and policies, and passes transfer work to the scheduler. See [Workflow events](../data-management/index.md#workflow-events) for the supported operations and their meaning.

Acceptance of an archive or retrieve request means it has been queued, not that the transfer is complete. Tape daemons move the file data directly between disk and tape; the data does not pass through the API. Results reach the disk system through the integration's reporting or transfer-completion mechanism, as described under [Disk Buffer](disk-buffer.md).

## Relationships with other components

The [Catalogue](catalogue.md) records file identities, tape copies, and policies; the [Scheduler](scheduler.md) coordinates pending work. Operator commands use the separate [Admin Frontend](admin-api.md).

Workflow requests have their own [authentication boundary](authentication.md). See [Workflow Frontend Configuration](../../ops/configuration/workflow-api.md) for settings and examples.
