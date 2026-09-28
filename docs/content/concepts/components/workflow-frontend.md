# Workflow Frontend {#workflow-api}

The **Workflow Frontend** accepts disk-system workflow requests over gRPC. The disk system retains ownership of the namespace and client-facing interface; file data travels directly between the disk buffer and tape daemons.

## Responsibilities

The frontend validates file registration, archival, retrieval, deletion and retrieval-cancellation requests against catalogue metadata and policies. It passes transfer work to the scheduler; see [Workflow events](../data-management/index.md#workflow-events).

`CREATE` allocates an archive file ID after validation; it does not create a tape copy. For non-empty archival and for retrieval, acceptance means work has been queued, not completed. A permitted [zero-length submission](../data-management/data-integrity.md#zero-length-files) returns success without queueing a tape write.
The disk system learns transfer outcomes through its reporting or transfer-completion mechanism; see [Disk System](disk-system.md#reporting-results).

## Relationships with other components

The [Catalogue](catalogue.md) holds file identities, recorded copies and policies. The [Scheduler](scheduler.md) tracks pending work, and tape daemons transfer the data. Operator commands use the separate [Admin Frontend](admin-frontend.md).

See [Authentication](authentication.md) for workflow identities and [Workflow Frontend Configuration](../../ops/deploy-and-configure/configuration/workflow-frontend.md) for service settings.
