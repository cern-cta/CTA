# Media Changer Daemon

The **media changer daemon** (`cta-rmcd`) provides library-control access for moving cartridges between storage slots and drives. Each media changer daemon is associated with one media changer.

## Role in tape mounts

After selecting work through the scheduler, a tape daemon asks the media changer to load the chosen cartridge into its drive. The media changer performs the movement and returns the result. The tape daemon requests dismounting when the session ends.

The media changer neither selects queued file work nor transfers file data. Those responsibilities belong to the scheduler and tape daemon.

## Relationships with hardware and services

Tape daemons and administrative clients such as [cta-smc](../../ops/tools/cta-smc.md) use the **Remote Media Changer (RMC)** interface. In the SCSI-library arrangement described here, the daemon accesses a media-changer device to command the robotics.

The drive address in a movement request must identify the same physical drive that the tape daemon controls. See [Tape Libraries](../tape/libraries.md) for hardware relationships and [Media Changer Daemon Configuration](../../ops/deploy-and-configure/configuration/media-changer-daemon.md) for deployment settings.
