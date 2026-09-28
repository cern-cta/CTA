# Storage Model

CTA's storage model connects a file in a disk namespace to its copies on tape. The [catalogue](../components/catalogue.md) records those copies, their locations, and the policies governing placement and resource use. The disk system retains ownership of the namespace and disk replicas.

## Files and tape copies

An **archive file** represents one file in CTA. It has a CTA-assigned **archive file ID**, a disk instance and disk file ID identifying the file in its originating namespace, a size, checksums, and a storage class. Paths and filenames are managed by the disk system; CTA does not reproduce its directory hierarchy.

A **tape file** represents one tape copy of an archive file. It records the copy number, the tape's volume identifier (**VID**), and the file's position on that tape. Copy numbers start at 1. Multiple tape copies share the same archive file ID, size, and file checksum information.

The storage class describes the required copies; the tape-file records describe the copies actually recorded. These can differ while archival is still in progress. A disk replica is separate from both: removing it after successful archival does not remove the tape copies.

## Disk instances and virtual organisations

A **disk instance** identifies a disk namespace connected to CTA. The disk instance and disk file ID together uniquely identify the file in its disk namespace. This model is independent of whether the disk system is EOS, dCache, or another integration.

A **virtual organisation (VO)** groups storage resources and policies for an experiment, project, or other administrative owner. Each VO is associated with a disk instance; storage classes and tape pools each belong to a VO. VO limits include the maximum numbers of drives used for reading and writing and the maximum file size.

A disk instance identifies where a file comes from; a VO provides an ownership and resource-allocation boundary.

## Storage classes, tape pools, and archive routes

These objects determine how many copies are required and where they are written:

| Object | Purpose |
| --- | --- |
| **Storage class** | Names a storage policy and specifies the required number of tape copies. Each archive file has one storage class. |
| **Tape pool** | Groups tapes used as destinations for a particular workload or copy. Each tape belongs to one pool. |
| **Archive route** | Maps a storage class and copy number to a destination tape pool. Route types distinguish normal archival from repack-specific placement. |

For example, a storage class named `physics-2copies` can require two copies, with the following normal archive routes:

| Storage class | Copy number | Destination tape pool |
| --- | --- | --- |
| `physics-2copies` | 1 | `physics-primary` |
| `physics-2copies` | 2 | `physics-secondary` |

CTA queues each copy for its destination pool and selects an eligible tape when scheduling the write. The route does not select a particular cartridge. Once written, each copy's tape-file record identifies its actual location; retrieval uses these recorded locations rather than the archive routes.

Tape pools describe data placement. [Logical libraries](../tape/libraries.md#logical-libraries) describe which tapes and drives can be used together. Separate pools do not by themselves guarantee separate physical libraries or sites; that depends on how their tapes are assigned and deployed.

Changing a route does not move existing tape copies. Moving recorded data requires a separate workflow, such as [repack](repack.md).

## Mount policies and requesters

Placement and scheduling are separate decisions. A **mount policy** supplies archive and retrieve priorities and minimum request ages used in mount selection. It influences when work is served, rather than where copies are stored.

**Requester rules** associate users or groups within a disk instance with mount policies. Retrieval can also use activity-specific rules to distinguish workloads from the same requester. These policies operate alongside VO drive limits, batching criteria, and resource availability; they do not guarantee an immediate mount or a fixed completion time.

See [Scheduling](scheduling.md) for how queued work is selected, and [Storage Policies](../../ops/administration/storage-policies.md) for operator procedures to create and maintain these objects.
