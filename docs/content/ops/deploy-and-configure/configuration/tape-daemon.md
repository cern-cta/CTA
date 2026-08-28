# Tape Daemon Configuration

Run one `cta-taped` process per tape drive, with a TOML configuration file for each process.
The packaged service reads `/etc/cta/cta-taped.toml`.
Use the [annotated example](#example-configuration) to configure the service and the [cta-taped manual](../../tools/service-manuals/cta-taped.md) for individual options and defaults.
This page covers operational decisions and interactions between settings.

## Prepare the deployment

See [Tape Server Setup](../deployment/tape-servers.md) for host and hardware preparation.

- **Device identity:** use [stable drive paths](../deployment/udev-rules.md) so device renumbering does not change which drive a process controls.
- **Existing drive:** its registered host and logical library must match; conflicting registration prevents startup.
- **Missing logical library:** the daemon waits for the catalogue entry before starting drive sessions.
- **Multiple drives on one host:** use separate configuration files, runtime directories, and log files for each process. Assign distinct health-server ports if enabled. See [Service Runtime](service-runtime.md) for directory management.
- **Host resources:** size memory and disk concurrency for the total number of taped processes on the host.

## Drive startup and preparation

### Choose when drives come Up

The catalogue state found before registration selects which setting applies:

| Catalogue state | Setting | When `true` | When `false` |
| --- | --- | --- | --- |
| No drive entry | `drive.registration.auto_up` | Request desired `Up`. | Register desired `Down`; wait for an operator Up request. |
| Existing desired `Up` | `drive.startup.recover_existing_up` | Preserve desired `Up`. | Set desired `Down`; require a new operator Up request. |
| Existing desired `Down`, with no reason or a clean-shutdown reason | `drive.startup.auto_up` | Request desired `Up`. | Remain desired `Down`. |
| Existing desired `Down`, with an operator or failure reason | None | Preserve desired `Down` and its reason, regardless of these settings. | Same. |

- `drive.startup.auto_up` does not override disabled recovery or activate newly registered drives.
- Existing drives follow the startup policy even if registration recreates their catalogue entry.
- Registration reports `Down`; desired `Up` permits preparation, but does not mean the drive is ready for work.
- When recovery is disabled for an existing desired-Up drive, the down reason records that an operator Up request is required.

### Choose how to prepare the drive

Set `drive.clean_on_up` to control preparation on every Down-to-Up transition.

| Value | Preparation | Operator action on failure |
| --- | --- | --- |
| `false` | Check that the drive is empty, without changing drive configuration. A cartridge or failed probe puts the drive Down. | Remove any remaining cartridge or resolve the probe failure, then request Up again. |
| `true` | Remove any cartridge and reset drive configuration as necessary, even if the daemon did not mount the cartridge or know its identity. Failed cleanup puts the drive Down. | Resolve the cleanup failure, then request Up again. |

**Automatic recovery with a cartridge still loaded requires both `drive.startup.recover_existing_up = true` and `drive.clean_on_up = true`.**

This setting controls initial preparation only.
Recovery cleanup after an unexpected tape-session failure still runs while the daemon owns the drive.
See [Cleanup guarantees](../../../concepts/components/tape-daemon.md#cleanup-guarantees) and [failure recovery](../../troubleshooting/tape-daemon.md#tape-daemon-failure-recovery).

## Recommended Access Order

RAO reduces positioning during retrieval.
See [RAO concepts](../../../concepts/tape/rao.md) for hardware and software ordering.

### Check prerequisites

- Hardware RAO takes precedence when available; the configured software algorithm is then ignored.
- For software `sltf`, populate `nbwraps`, `minlpos`, and `maxlpos` on the catalogue media type using `cta-admin mediatype`.

### Verify ordering and diagnose fallback

1. Run a retrieval and inspect `executedRAOAlgorithm` in the session logs.
2. If ordering falls back to linear, check the accompanying warning.
3. Check the selected algorithm, catalogue media geometry, and drive capabilities before retrying.

Algorithm creation or execution failures can trigger linear fallback.

## Set up encryption

Enabling encryption in taped does not encrypt every tape automatically.
Configure tape/pool keys and the site's key-management integration before using encrypted media.
Use the `[transfers.encryption]` section of the [annotated example](#example-configuration) for daemon settings and [Tape Drive Concepts](../../../concepts/tape/drives.md) for the encryption model.

## Validate and apply changes

1. Start from the [complete example](#example-configuration) and set the drive identity, connection files, and site policies.
2. Validate with the same strictness used by the service:

   ```sh
   cta-taped --config /etc/cta/cta-taped.toml --config-strict --config-check
   ```

   See [Service Runtime](service-runtime.md#strict-config-checking) for strict-checking requirements.
   Configuration checking exits without starting the daemon; it does not verify database connectivity or hardware operation.
3. Restart the affected service to apply changes. `SIGUSR1` reopens the log file; it does not reload configuration.
4. Check the drive's desired state, reported state, and down reason with `cta-admin dr ls`.
5. If the startup policy leaves the drive Down, request Up when ready:

   ```sh
   cta-admin dr up <drive-name>
   ```

6. Confirm the drive becomes available and completes an archive/retrieve check.

An orderly restart waits for the active tape session to finish.
Allow enough service-stop time for transfers and cleanup; see [Requests to stop](../../../concepts/components/tape-daemon.md#requests-to-stop).
To roll back configuration, restore the previous file, validate it, and restart the affected service.

## Example configuration

???+ example "cta-taped.example.toml"

    ```toml
    --8<--
    taped/cta-taped.example.toml
    --8<--
    ```

## Command reference

See the [cta-taped manual](../../tools/service-manuals/cta-taped.md) for the complete option reference.
