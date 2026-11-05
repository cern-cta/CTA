# Scheduling

CTA schedules tape mounts to balance throughput and request latency. Tape drives are few, and mounting and positioning a cartridge takes a minute or more, so serving many files during one mount is far more efficient than mounting a tape for each file. CTA therefore does not execute requests one by one: it queues them and lets work build up until a mount is worth it.

Scheduling answers three questions, over and over:

- **When** is there enough queued work to justify a mount? Enough bytes, enough files, or a request that has waited long enough.
- **Which** queue should a free drive serve next, given priorities, VO drive limits and the tapes it can reach?
- **In what order** should the files of the chosen queue be transferred?

Scheduling decides which work gets a drive; the tape daemon carries out the transfers. There is no central dispatcher: each idle drive selects its own next mount from the shared queue and drive state. For how this is implemented, see the [request path](../../dev/internals/workflows/index.md#the-request-path) and the [Scheduling Algorithm](../../dev/internals/shared-state/scheduler-db/scheduling-algorithm.md).

## How a request flows

```mermaid
flowchart LR
--8<-- "docs/includes/mermaid-classes.txt"

  DS["<b>Disk system</b><br/>EOS, dCache"]
  WF["<b>Workflow API</b>"]
  S[("<b>Scheduler DB</b><br/>waiting requests,<br/>results")]
  TD["<b><code>taped</code></b><br/>one per drive"]
  MD["<b><code>maintd</code></b>"]

  DS -->|"(1) request"| WF -->|"store"| S
  S -->|"(2) mount and<br/>batches of jobs"| TD
  TD -->|"results"| S
  TD <==>|"(3) file data"| DS
  S -->|"results"| MD -->|"(4) report"| DS

  class DS ext
  class WF api
  class S store
  class TD,MD daemon
```

1. The disk system sends an archive or retrieve request to the Frontend's **Workflow API**, which checks it against the catalogue and stores it in the Scheduler DB. The disk system is acknowledged straight away - no tape work has happened yet.
2. When a drive is free, its **tape daemon** (`taped`) polls the Scheduler DB for work and chooses a mount, as described in [Mount selection](#mount-selection-priorities-and-request-age). Drives coordinate through the Scheduler DB, so two of them never choose the same tape.
3. The tape daemon mounts the tape, takes the requests in batches and copies the data between the tape and the disk system. Files written to tape are recorded in the catalogue.
4. The **maintenance daemon** (`maintd`) reports each result back to the disk system, which then completes its side of the operation.

These are separate stages: a file can already be on tape while the disk system has not yet been told. See [Disk System](../components/disk-system.md) for its role in completing archival and retrieval.

## Queues and batching

When an accepted request is queued, it does not immediately reserve a drive or start a transfer. That is left to the daemons running in each tape drive
(`taped`).

There is an important key distinction between request types: **archival queues work for a tape pool** while **retrieval queues work for a specific tape**.

| Operation | Queue target | |
| --- | --- | --- |
| **Archival** | **Tape pool** | The copy has not yet been written, so its destination tape can be chosen from the pool's eligible writable tapes. |
| **Retrieval** | **Tape (VID)** | The copy already exists at a recorded location, so CTA must mount the tape[^1] holding it. |

[^1]: or "one of the tapes", if the file was archived with a number of copies greater than one

For **archival**, each required copy is queued for the pool selected by its archive route. `taped` chooses the actual tape when scheduling a mount. Work queued for the same pool can therefore be served by different tapes and, within resource limits, multiple drives.

For **retrieval**, CTA selects a tape copy and queues the request for that tape. Requests for different files on the same tape can share a mount; requests for files on different tapes require separate mounts, even if those tapes belong to the same pool.

In the example below, the two copies of file A wait in the queues of two different pools, and either queue can be served by any writable tape of its pool. Files B and C wait in the queue of tape T7, because that is where their copies are; one mount of T7 serves both.

```mermaid
flowchart LR
--8<-- "docs/includes/mermaid-classes.txt"

  A["Archive file A<br/>(2 copies)"]
  B["Retrieve file B"]
  C["Retrieve file C"]

  subgraph QUEUES["Scheduler queues"]
    direction TB
    QP1["Pool P1 queue"]
    QP2["Pool P2 queue"]
    QT7["Tape T7 queue"]
  end

  D1["Drive<br/>any writable tape of P1"]
  D2["Drive<br/>any writable tape of P2"]
  D3["Drive<br/>tape T7 only"]

  A -->|"copy 1"| QP1
  A -->|"copy 2"| QP2
  B -->|"copy on T7"| QT7
  C -->|"copy on T7"| QT7
  QP1 -.->|"mount"| D1
  QP2 -.->|"mount"| D2
  QT7 -.->|"mount"| D3

  class A,B,C ext
  class QP1,QP2,QT7 store
  class D1,D2,D3 daemon
  class QUEUES group
```

[Repack](repack.md) follows both patterns: reads are queued for the source tape, and writes are queued for the destination tape pools.

A **mount** is a read or write session on one tape in one drive. `taped` obtains work in batches during the session, allowing many files to be served without unloading the cartridge between them. Mount selection is therefore not a global first-in, first-out ordering of individual files.

## Mount selection, priorities, and request age

When a drive is available, its tape daemon uses the scheduling algorithm to select a suitable mount. The decision considers queued work and mount policies, which tapes the drive can access through its logical library, whether those tapes are usable for the requested operation, and whether the [VO’s drive limits](storage-model.md#disk-instances-and-virtual-organisations) allow another mount.

The diagram summarises the checks described in the following sections. The requests waiting for one tape pool or tape under the same mount policy form a **candidate mount**. A candidate is mounted only if it passes all three checks: it reaches a mount threshold, the resources it needs are available, and it ranks highest among the candidates that passed both.

```mermaid
flowchart TD
--8<-- "docs/includes/mermaid-classes.txt"

  Q["Retrieve work for a<br/>tape pool or tape"]
  T["<b>Check Thresholds:</b><br/>enough bytes or files,<br/>or oldest request old enough"]
  R["<b>Check Resources:</b><br/>drive and library usable,<br/>tape usable and not in use,<br/>VO under its drive limit,<br/>disk space available"]
  P["<b>Check Ranking:</b><br/>priority, then request age"]
  M["Mount it on<br/>the free drive"]
  W["Keep waiting"]

  Q --> T -->|"met"| R -->|"available"| P -->|"highest-ranked"| M
  T -->|"not met"| W
  R -->|"missing"| W
  P -->|"otherwise"| W

  class Q,W ext
  class T,R,P store
  class M daemon
```

### When a batch warrants a mount

**Configurable thresholds** allow a mount to become eligible when there are enough queued bytes, enough queued files, or sufficiently old work. The age condition allows small queues to be served without having to accumulate a large batch. Thresholds are evaluated separately for work under different mount policies (and, for retrieval, different activities or destination disk systems).

Despite its name, a mount policy's **minimum request age** is not a mandatory delay for every request: enough queued data or files can warrant a mount earlier. For archival, the scheduling algorithm also accounts for mounts already serving the pool before allocating additional drives; the age condition alone does not add another archive mount to a pool already being served.

Larger batching thresholds favour efficient use of tape hardware, while shorter age thresholds favour responsiveness for small workloads. Reaching a threshold makes work eligible for consideration; it does not guarantee that a mount starts immediately.

### Priority and resource sharing

Every request gets a mount policy when it is queued, chosen by the [mount rules](storage-model.md#mount-policies-and-requesters) of its disk instance. A mount policy sets a priority, separately for archive and for retrieve. When several candidates are ready to be mounted, the one with the highest priority goes first; and between equal priorities, the one whose oldest request has waited longest.

Requests for the same tape pool or tape can have different mount policies, and then form separate candidates, each judged with its own policy's priority and minimum request age. As soon as one of them qualifies, the tape is mounted, and the mount serves all the requests for that tape pool or tape, whatever their policy.

In the example below, the mount threshold is 500 files and tape T7 has 300 requests under each of two policies. Neither candidate reaches 500 files, although together they would. Once the oldest request of candidate A is more than an hour old, it qualifies, T7 is mounted, and the mount serves all 600 requests.

```mermaid
flowchart LR
--8<-- "docs/includes/mermaid-classes.txt"

  subgraph T7["Tape T7"]
    direction TB
    A["<b>Candidate A</b><br/>policy A, 300 files<br/>minimum age 1 h"]
    B["<b>Candidate B</b><br/>policy B, 300 files<br/>minimum age 4 h"]
  end

  CA["<b>Threshold check A</b><br/>300 &lt; 500 files<br/>oldest request &gt; 1 h: <b>yes</b>"]
  CB["<b>Threshold check B</b><br/>300 &lt; 500 files<br/>oldest request &gt; 4 h: no"]
  M["<b>T7 mounted</b><br/>serves all 600 requests,<br/>policies A and B"]

  A --> CA -->|"qualifies"| M
  B --> CB -.->|"does not qualify,<br/>served anyway"| M

  class A,B ext
  class CA,CB store
  class M daemon
  class T7 group
```

The scheduler always uses a policy's values as they are **now**, not as they were when the request was queued. Changing a mount policy therefore also affects requests that are already waiting.

Drive limits are set per VO, separately for reading and writing. A VO that has reached its limit gets no further mount until one of its mounts ends. The limits only cap how many drives a VO uses at once: they do not reserve drives for it, and they do not guarantee when its requests will complete.

Priority decides which tape is mounted next, never whether a running mount stops. A mount keeps serving its tape pool or tape until no request is left for it, the tape is full, or the session fails. Requests that arrive for that pool or tape in the meantime are served by the same mount, so a steady stream of requests keeps the tape mounted. Higher-priority work for another tape, or another VO waiting for a drive, has to wait for the next free drive.

### Resource eligibility

Queued work must have usable resources before it can run:

- The drive and its logical library must be available, and the tape must be accessible through that library.
- The tape's state must permit the operation. Archival also needs a writable tape with remaining capacity in the destination pool.
- A tape already assigned to another mount cannot be mounted concurrently in a second drive.
- The relevant VO drive limit must allow another mount.
- Where disk-space reservations are configured, insufficient space in the destination disk buffer can temporarily defer retrieval work.

A high priority cannot override these constraints. See [Tape Lifecycle](../tape/lifecycle.md) for tape-state restrictions and [Tape Libraries](../tape/libraries.md) for logical-library membership.

## Ordering within a retrieval batch

After a tape has been selected, [Recommended Access Order (RAO)](../tape/rao.md) can reorder reads within a retrieval batch to reduce positioning time. This is a separate decision from choosing which tape to mount: RAO does not change mount priorities, eliminate mounts across multiple tapes, or compensate for poorly collocated data.

## Related guides

- [Scheduler DB](../components/scheduler-db.md): what the scheduler backend stores and backend choices.
- [Scheduling and Queues](../../ops/run-and-maintain/administration/requests.md): operator inspection and intervention procedures.
- [Storage Policies](../../ops/run-and-maintain/administration/storage-policies.md): managing mount policies and mount rules.
