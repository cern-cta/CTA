# Scheduling

CTA schedules tape mounts to balance throughput and request latency. Mounting and positioning a tape take time, so serving many files during one mount is more efficient than mounting a tape for each file. Scheduling determines which work gets a drive, while the tape daemon carries out the transfers.

## Queues and batching

An accepted request is queued; it does not immediately reserve a drive or start a transfer. The key distinction is **archival queues work for a tape pool; retrieval queues work for a specific tape**.

| Operation | Queue target | Why? |
| --- | --- | --- |
| **Archival** | **Tape pool** | The copy has not yet been written, so its destination tape can be chosen from the pool's eligible writable tapes. |
| **Retrieval** | **Tape (VID)** | The copy already exists at a recorded location, so CTA must mount the tape holding it. |

For archival, each required copy is queued for the pool selected by its archive route. CTA chooses the actual tape when scheduling a mount. Work queued for the same pool can therefore be served by different tapes and, within resource limits, multiple drives.

For retrieval, CTA selects a tape copy and queues the request for that tape. Requests for different files on the same tape can share a mount; requests for files on different tapes require separate mounts, even if those tapes belong to the same pool.

[Repack](repack.md) follows both patterns: reads are queued for the source tape, and writes are queued for the destination tape pools.

A mount is a read or write session on one tape in one drive. The daemon obtains work in batches during the session, allowing many files to be served without unloading the cartridge between them. Mount selection is therefore not a global first-in, first-out ordering of individual files.

## Mount selection, priorities, and request age

When a drive is available, its tape daemon uses the scheduler to select a suitable mount. The decision considers queued work and mount policies, which tapes the drive can access through its logical library, whether those tapes are usable for the requested operation, and whether the [VO’s drive limits](storage-model.md#disk-instances-and-virtual-organisations) allow another mount.

### When a batch warrants a mount

Configurable thresholds allow a mount to become eligible when there are enough queued bytes, enough queued files, or sufficiently old work. The age condition allows small queues to be served without having to accumulate a large batch.

Despite its name, a mount policy's **minimum request age** is not a mandatory delay for every request: enough queued data or files can warrant a mount earlier. For archival, the scheduler also accounts for mounts already serving the pool before allocating additional drives; the age condition alone does not add another archive mount to a pool already being served.

Larger batching thresholds favour efficient use of tape hardware, while shorter age thresholds favour responsiveness for small workloads. Reaching a threshold makes work eligible for consideration; it does not guarantee that a mount starts immediately.

### Priority and resource sharing

Mount policies specify separate archive and retrieve priorities. Higher numerical priorities favour a candidate mount over lower-priority candidates. When work with different policies shares a queue, the candidate uses the highest applicable priority and the lowest applicable minimum request age.

The scheduler also considers existing mounts and the share of a VO's drive allowance already in use. A VO's read and write limits cap concurrent resource use; they do not reserve those drives or guarantee a completion time. Priorities apply to mount selection, rather than interrupting a transfer whenever a higher-priority request arrives.

### Resource eligibility

Queued work must have usable resources before it can run:

- The drive and its logical library must be available, and the tape must be accessible through that library.
- The tape's state must permit the operation. Archival also needs a writable tape with remaining capacity in the destination pool.
- A tape already assigned to another mount cannot be mounted concurrently in a second drive.
- The relevant VO drive limit must allow another mount.
- Where disk-space reservations are configured, insufficient space in the destination disk buffer can temporarily defer retrieval work.

A high priority cannot override these constraints. See [Tape Lifecycle](../tape/lifecycle.md) for tape-state restrictions and [Tape Libraries](../tape/libraries.md) for logical-library membership.

## Scheduler workflow

1. The **Workflow API** validates the request using catalogue metadata and queues the work in the scheduler backend.
2. An available **tape daemon** asks the scheduler for work. The scheduler coordinates mount allocation so that concurrent daemons do not claim the same tape.
3. The daemon mounts the selected tape, obtains batches of jobs, and transfers data between tape and the disk buffer. Successful writes are recorded in the catalogue.
4. Transfer outcomes enter the reporting workflow. The **maintenance daemon** processes the corresponding reports to the disk system so it can complete its side of the operation.

Queueing, transferring, and reporting are distinct stages. A completed tape transfer can still be awaiting notification to the disk system; see [Disk Buffer](../components/disk-buffer.md) for its role in completing archival and retrieval.

## Ordering within a retrieval batch

After a tape has been selected, [Recommended Access Order (RAO)](../tape/rao.md) can reorder reads within a retrieval batch to reduce positioning time. This is a separate decision from choosing which tape to mount: RAO does not change mount priorities, eliminate mounts across multiple tapes, or compensate for poorly collocated data.

## Related guides

- [Scheduler component](../components/scheduler.md): responsibilities and backend choices.
- [Scheduling and Queues](../../ops/administration/requests.md): operator inspection and intervention procedures.
- [Storage Policies](../../ops/administration/storage-policies.md): managing mount policies and requester rules.
