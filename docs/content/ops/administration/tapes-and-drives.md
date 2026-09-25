# Tapes, Drives, and Libraries

Day-to-day procedures for tape hardware and inventory.

!!! info "Documentation outline"
    The sections below reserve space for the detailed documentation to be added.

## Register and commission resources

Document libraries, drives, media types, tape registration, and labelling.

## Change resource state

Document drive enable/disable and tape-state transitions. See [Tape Lifecycle](../../concepts/tape/lifecycle.md).

## Verify and reclaim tapes

Document verification, reclamation prerequisites, and validation of the result.

## Hardware maintenance

See [Hardware Replacement](tapes-and-drives.md#hardware-replacement) for draining, replacing, and returning hardware to service.

See [Hardware Installation](../deployment/tape-servers.md#commissioning-hardware) and [Hardware Replacement](tapes-and-drives.md#hardware-replacement).

## Labeling a tape

!!! danger
    Labeling a tape is a destructive action which *overwrites* any data on said tape. Never label a tape with data on it. There is no way to recover the data, apart from having the vendor attempt a recovery.

Before a tape can be written to by CTA, it must have the CTA format applied by *labeling* the tape.
The labeling procedure applies the CTA tape format, and specifically the VOL1 descriptor containing the tape's VID, to the beginning of the tape.
This VID field is used to verify that the tape's content is what one expected, based on the VID printed on the physical cartridge.
Individual tapes may be labeled using the `cta-tape-label` command line tool.
However, we strongly recommend using the wrapper command [cta-ops-admin tape label](../tools/cta-ops-admin.md), which supports bulk-labeling a number of tapes sequentially, and performs safety checks before doing destructive actions.

## Startup probing and stuck media

Document checking whether a tape is already loaded, interpreting probing and cleanup failures, and the conditions for operator intervention. Link drive state checks to [Tape Daemon Concepts](../../concepts/components/tape-daemon.md).

## Hardware replacement

Replace tape servers, drives, or library hardware while preserving CTA metadata and configuration.

!!! info "Documentation outline"
    The sections below reserve space for the detailed documentation to be added.

### Drain and isolate

Document how to stop new work and confirm that active tape sessions are complete.

### Replace and reconfigure

Document identity, device-path, library, and configuration changes.

### Validate and return to service

Document checks and criteria for resuming normal operation.
