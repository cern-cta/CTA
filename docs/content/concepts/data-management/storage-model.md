# Storage Model

CTA's storage model connects a file in a disk namespace to its copies on tape. The [catalogue](../components/catalogue.md) records those copies, their locations, and the policies governing placement and resource use. The disk system owns paths, filenames and disk replicas.

## Files and tape copies

| Record | Meaning |
| --- | --- |
| **Archive file** | One immutable file identity in CTA: archive file ID, originating disk instance/file ID, size, checksums and storage class. |
| **Tape file** | One recorded copy: archive file ID, copy number (starting at 1), tape VID and position. All copies share the file's expected contents. |

Position includes the file's sequence and block location on tape; see [CTA Tape Format](../tape/media/format.md). CTA does not reproduce the disk system's directory hierarchy.

The storage class specifies **required** copies; tape-file records describe **existing** copies. They can differ during [archival](archival.md). Changing a required copy count does not automatically create missing copies of older files; [repack](repack.md#repack-modes) can add them.

A disk replica is separate from these records. Evicting it after successful archival does not remove tape copies.

## Disk instances and virtual organisations

A **disk instance** identifies an originating disk namespace. Its name and the disk file ID together uniquely identify the file in its disk namespace.

A **virtual organisation (VO)** groups resources and policies for an experiment, project or other owner. Each VO is associated with a disk instance; storage classes and tape pools belong to VOs. VO limits include maximum file size and concurrent read/write drive usage. Drive limits cap use; they do not reserve dedicated hardware.

## Storage classes, tape pools, and archive routes

These objects determine how many copies are required and where they are written:

| Object | Purpose |
| --- | --- |
| **Storage class** | Specifies how many tape copies a file requires. |
| **Tape pool** | Groups destination tapes; each tape belongs to one pool. |
| **Archive route** | Maps a storage class and copy number to a pool. Route types distinguish normal archival from repack-specific placement. |

For example:

| Storage class | Copy | Normal destination pool |
| --- | --- | --- |
| `physics-2copies` | 1 | `physics-primary` |
| `physics-2copies` | 2 | `physics-secondary` |

CTA queues each copy for its destination pool and selects an eligible tape when scheduling the write. The route does not select a particular cartridge. Once written, each copy's tape-file record identifies its actual location; retrieval uses these recorded locations rather than the archive routes.

Tape pools describe placement. [Logical libraries](../tape/libraries.md#logical-libraries) describe which tapes and drives can be used together. Different pools do not guarantee different sites or physical libraries. Changing routes affects subsequent placement decisions; it does not relocate existing copies. Use [Repack](repack.md) for that.

## Mount policies and requesters

A **mount policy** specifies separate archive/retrieve priorities and minimum request ages. It influences when work becomes eligible and is selected, rather than where copies are written.

**Requester rules** map users or groups within a disk instance to mount policies. Retrieval can also match requester activities. These rules operate alongside batching thresholds, VO limits and resource availability; no policy guarantees an immediate mount or fixed completion time.

See [Scheduling](scheduling.md) for behavior and [Storage Policies](../../ops/run-and-maintain/administration/storage-policies.md) for administration.
