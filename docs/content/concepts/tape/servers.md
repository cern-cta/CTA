# Tape Servers

A *Tape Server* is a computer connected to at least one tape drive and running the `cta-taped` daemon.
The tape daemon selects archive/retrieve work through the scheduler and transfers file data between the disk system and the drive, buffering it in memory.
A server may host several drives, with a separate tape-daemon instance for each drive. The disk buffer is a separate component; normal tape transfers do not require staging files on the tape server's local disks.

## Connections and shared resources

The tape server needs connectivity to the disk system for file transfers, to the catalogue and scheduler for metadata and work coordination, and to the media changer for mount and dismount requests. These control connections are separate from the file-data path.

Drives attached to the same server share its network bandwidth, memory, CPU, and hardware interfaces. The server must sustain the combined throughput of its active drives to keep them streaming. Losing the server interrupts service for all drives it hosts, so placement also determines the scope of a host failure. See [Deployment Planning](../../ops/deployment/planning.md) for sizing and availability decisions.

## Device access and stable identities

On a tape server the SCSI-connected tape drives are exposed in the file system as devices in `/dev`.
A device is either *automatically rewinding*, such that the tape is rewound back to its beginning when the device is closed, or non-rewinding.

* `/dev/st<X>` - Automatically rewinds
* `/dev/nst<X>` - Does not automatically rewind.

CTA uses the non-rewinding device so it can control tape positioning across operations.

The numbered paths above reflect device discovery order and can change after a reboot or hardware change. Production configurations should use stable device paths, rather than `/dev/nstX`, so each tape-daemon instance continues to address the intended physical drive. A persistent udev symlink should identify the drive by a stable hardware identifier and resolve to its non-rewinding device.

The stable device path, CTA drive name, and library drive address must all refer to the same physical drive. See [Stable Drive Identities and udev Rules](../../ops/deployment/udev-rules.md) for setup and verification.

For configuration and hardware procedures, see [Tape Server Setup](../../ops/deployment/tape-servers.md).
