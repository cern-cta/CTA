# Scheduler

The CTA Scheduler is at the heart of the system, responsible for prioritising requests and deciding which tape will be 
mounted into which tape drive.

The scheduler relies on a transient data store to hold the metadata for all requests and queues, the Scheduler Database.
The Scheduler DB supports multiple backend technologies. Currently the only backend supported in a production environment
is an objectstore on a Ceph cluster. An alternative PostgreSQL Scheduler DB is expected to be available during H2 2026.

The central design principle of the scheduler is to maximise read and write throughput. Effectively this means keeping
mount time and seek time to a minimum in order to maximise read/write time. To achieve this, multiple data transfer
requests (archival and retrieval) are accumulated in many queues that group requests by operations on the same tape.
The system waits for a suitable number of requests to be accumulated before deciding to mount a tape.

Together with this major objective, the Scheduler serves as the central authority for several scheduling-related tasks such as:

- Queueing archive and retrieve requests from the CTA Frontends.
- Batching jobs to optimize tape mount utilization.
- Deciding on which tapes to mount, based on queue contents, mount policies, and available resources.
- Queueing successful transfers, before reporting them back to the Disk Instance.
- Managing repack operations (see [Repack Workflow](../../dev/architecture/workflows/repack.md) for more details).

## Scheduler Workflow

The main scheduler workflow involves the following steps:

1. On the CTA Frontend:
  - Receiving requests from the Disk Instance and queueing them on the Scheduler DB.
2. On the Tape Daemon:
  - Checking the queues on the Scheduler DB, to decide which tape to mount next.
3. On the Tape Daemon:
  - Mounting the tape, popping the requests from the Scheduler DB, and reading/writing the data from/to tape.
  - Queueing back into the Scheduler DB the success or failure of each transfer.
4. On the Maintenance Daemon:
  - Reporting the success or failure back to the Disk Instance, so that it can finish the full data transfer workflow.

The scheduler workflow is explained with more detail on [this link](../../dev/architecture/workflows/scheduling.md).

## Scheduler Architecture

The scheduling workflow involves most parts of the CTA infrastructure:
- CTA Frontend, Disk Instance, Tape Daemon, Maintenance Daemon, Catalogue and SchedulerDB backend for metadata operations.

For more information, please check the [Scheduler Architecture](../../dev/architecture/components/scheduler.md#scheduler) in the development section of this documentation.
