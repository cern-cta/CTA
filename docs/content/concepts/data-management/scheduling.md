# Scheduling

CTA groups requests into tape work to balance throughput and request latency. Mounting and positioning tape have a cost, so processing a batch can be more efficient than handling each request independently.

## Queues and batching

Requests are queued for archival or retrieval and grouped into work that can be served by suitable tapes and drives. Queueing a request does not mean a tape will be mounted immediately.

## Mount selection, priorities, and request age

Mount selection considers queued work, mount policies, and available resources. [Storage Model and Policies](storage-model.md) defines the policy objects and requester mappings used in these decisions.

!!! info "Documentation outline"
    Explain batching thresholds, priorities, request age, and resource eligibility, including the tradeoff between throughput and latency. Describe the scheduling rules without backend implementation details or operator commands.

## Scheduler Workflow

The main scheduler workflow involves the following steps:

1. On the Workflow API:
  - Receiving requests from the Disk Instance and queueing them on the Scheduler DB.
2. On the Tape Daemon:
  - Checking the queues on the Scheduler DB, to decide which tape to mount next.
3. On the Tape Daemon:
  - Mounting the tape, popping the requests from the Scheduler DB, and reading/writing the data from/to tape.
  - Queueing back into the Scheduler DB the success or failure of each transfer.
4. On the Maintenance Daemon:
  - Reporting the success or failure back to the Disk Instance, so that it can finish the full data transfer workflow.

See [Scheduling and Queues](../../ops/administration/requests.md) for operator procedures.

## Ordering within a retrieval batch

Mount selection determines which tape work runs. [Recommended Access Order](../tape/rao.md) determines the order of reads within a retrieval batch to reduce positioning time; it does not replace queue or mount policy.

## Related guides

See the [Scheduler component](../components/scheduler.md) for responsibilities and backend choices, and [Scheduling and Queues](../../ops/administration/requests.md) for inspection and intervention procedures.
