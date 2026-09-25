# Stable Drive Identities and udev Rules

Configure persistent device paths so tape daemons address the intended drives after reboots or changes in device discovery order. See [Tape Servers](../../concepts/tape/servers.md) for the distinction between rewinding and non-rewinding devices.

!!! info "Documentation outline"
    The sections below define the workflow; hardware-specific commands and rule examples will be added during the content review.

## Identify each physical drive

Record the drive's serial number or other stable hardware identifier, its library drive address, and its CTA drive name. Verify these against the physical inventory rather than inferring identity from the current `/dev/nstX` number.

## Define persistent device paths

Inspect the udev properties exposed by the host and define rules that match a stable, unique hardware identifier. Create a persistent symlink to each drive's **non-rewinding** device. For drives with multiple hardware paths, document which path the rule selects and how duplicate matches are avoided.

CTA's supplied `99-tape.rules` assigns the `st` group to `nst*` devices; it does not create stable drive names. Site-specific identity rules must complement the required device permissions.

## Configure CTA

Use the persistent paths in the [Tape Daemon Configuration](../configuration/tape-daemon.md). Keep the mapping between the symlink, physical drive, CTA drive name, and library address explicit. Container deployments must expose the corresponding devices and paths to the service.

## Verify before enabling work

Document how to apply rules safely, confirm each symlink's target and permissions, and verify that the mapping survives a reboot or device rediscovery. Confirm that library mounts address the same drive as the configured data device before enabling production traffic.

## Drive replacement

A replacement drive has a different hardware identity even when it occupies the same library position. Update and revalidate the mapping as part of [Hardware Replacement](../administration/tapes-and-drives.md#hardware-replacement).
