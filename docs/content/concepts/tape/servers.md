# Tape Servers

A *Tape Server* is a computer connected to at least one tape drive and running the `cta-taped` daemon.
It acts as a tape drive's connection to the outside world, picking up archive/retrieve jobs for the drive to act upon, and buffering data in transit to/from tape on its local disk(s).

On a tape server the SCSI-connected tape drives are exposed in the file system as devices in `/dev`.
A device is either *automatically rewinding*, such that the tape is rewound back to its beginning after each read/write operation, or non-rewinding.

* `/dev/st<X>` - Automatically rewinds
* `/dev/nst0` - Does not automatically rewind.


For configuration and hardware procedures, see [Tape Server Setup](../../ops/deployment/tape-servers.md).
