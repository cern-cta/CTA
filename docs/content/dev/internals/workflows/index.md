!!! info "Documentation incomplete"

    This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.

# Workflow Implementation

These pages trace requests through CTA code and persistent state. For user-visible behaviour, start with [Concepts](../../../concepts/index.md); disk-system protocols belong in [Disk System Integrations](../../guides/integrations/index.md).

A typical archive or retrieve request enters through the workflow frontend, uses catalogue metadata, becomes scheduler work, is executed by the tape daemon, and is reported back to the disk system. Backend-specific queues and ownership mechanisms are described under [Scheduler](../components/scheduler/index.md).

| Operation | Implementation guide |
| --- | --- |
| Archive data to tape | [Archive](archival.md) |
| Retrieve data from tape | [Retrieve](retrieval.md) |
| Select and assign tape work | [Scheduling](scheduling.md) |
| Remove file metadata and pending work | [Delete](deletion.md) |
| Move or recreate tape copies | [Repack](repack.md) |
| Retain metadata for deleted or repacked copies | [Recycle Bin](recycle-bin.md) |

TODO: Add a verified request-path diagram with code entry points and state transitions shared across backends. Each workflow should explain its failure, retry, cancellation, and completion-reporting behaviour alongside its normal path.
