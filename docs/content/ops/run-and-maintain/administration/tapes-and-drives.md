!!! info "Documentation incomplete"

    This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.

# Tapes, Drives, and Libraries

Day-to-day procedures for tape hardware and inventory.

## Register and commission resources

TODO: Document libraries, drives, media types, tape registration, and labelling.

## Change resource state

TODO: Document drive enable/disable and tape-state transitions. See [Tape Lifecycle](../../../concepts/tape/lifecycle.md).

## Verify and reclaim tapes

TODO: Document verification, reclamation prerequisites, and validation of the result. See [Tape Verification](../../tools/tape-verification.md) for verification-tool configuration and usage.

## Mounting and Unmounting a tape

TODO: Document prerequisites, mounting and unmounting commands, and checks before returning the drive to service. See [cta-smc](../../tools/cta-smc.md) for command syntax.

## Hardware maintenance

For new hardware, see [Hardware Installation](../../deploy-and-configure/deployment/tape-servers.md#commissioning-hardware). For existing hardware, follow [Hardware Replacement](#hardware-replacement) below.

## Labeling a tape

See [Media Initialisation](media-initialisation.md) for preparing, registering, and labelling media before use.

## Startup probing and stuck media

TODO: Document checking whether a tape is already loaded, interpreting probing and cleanup failures, and the conditions for operator intervention. Link drive state checks to [Tape Daemon Concepts](../../../concepts/components/tape-daemon.md).

## Hardware replacement

Replace tape servers, drives, or library hardware while preserving CTA metadata and configuration.

### Drain and isolate

TODO: Document how to stop new work and confirm that active tape sessions are complete.

### Replace and reconfigure

TODO: Document identity, device-path, library, and configuration changes.

### Validate and return to service

TODO: Document checks and criteria for resuming normal operation.
