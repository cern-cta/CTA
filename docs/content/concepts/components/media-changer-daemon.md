# Media Changer Daemon

The media changer daemon (`cta-rmcd`) provides access to tape-library robotics, moving cartridges between storage slots and tape drives.

## Role in tape mounts

Once a tape daemon has selected work through the scheduler, it requests the required tape to be mounted in its drive. The media changer daemon handles the library movement and returns the result. At the end of the tape session, the tape daemon requests that the cartridge be dismounted.

This separates cartridge movement from data transfer: the tape daemon reads and writes through the tape drive, while the media changer daemon controls the library robot. The media changer does not choose which queued requests to process or transfer file data.

## Relationships with hardware and services

Tape daemons contact the media changer through its RMC interface. Administrative clients such as `cta-smc` also use this interface to inspect the library and request media movements. The daemon needs access to the library's media-changer device to issue the SCSI commands that operate the robotics. The library and drive mapping must identify the physical drive involved in each mount.

See [Tape Libraries](../tape/libraries.md) for the hardware concepts and [Media Changer Daemon Configuration](../../ops/configuration/media-changer-daemon.md) for device access, endpoint settings, and configuration examples.
