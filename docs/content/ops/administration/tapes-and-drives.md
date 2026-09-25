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

See [Media Initialisation](media-initialisation.md) for preparing, registering, and labelling media before use.

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
