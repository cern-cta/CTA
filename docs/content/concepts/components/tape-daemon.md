# CTA Tape Daemon

The **tape daemon** (`cta-taped`) runs on a [Tape Server](../tape/servers.md) and controls one drive. It selects work through the scheduler, transfers data between disk and tape, checks file integrity, and records successful tape copies and resource state in the catalogue.

It asks the [Media Changer Daemon](media-changer-daemon.md) to move cartridges. See [Data Integrity](../data-management/data-integrity.md) for transfer checks.

## Tape Sessions

A tape session waits for eligible work, mounts a cartridge, transfers files in batches, and unloads and dismounts the cartridge. The daemon reports its activity so operators can distinguish waiting, transfer and cleanup.

### Drive States

The operator's desired drive state (`UP` or `DOWN`) is separate from the daemon's reported activity. A drive that is down waits for a request to bring it up. The daemon probes the drive before making it available for work; a failed probe leaves it down.

After obtaining a mount, the drive progresses through `STARTING`, `MOUNTING`, and `TRANSFERRING`. Cleanup includes unloading the tape from the drive and unmounting the cartridge through the media changer. The drive then becomes available again, or goes down if requested.

During retrieval, disk-writing threads may still be flushing buffered data after the tape has been unmounted. The daemon reports `DRAINING_TO_DISK` until those threads finish. Tape reading has already ended at this point.

![Drive Status State Diagram](drive_status_state_diagram.png "Drive Status State Diagram")

See [Scheduling](../data-management/scheduling.md) for how the daemon selects a tape mount.
