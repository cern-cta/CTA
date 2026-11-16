# Upgrading CTA

Review the [changelog](../../../changelog.md) for the versions covered by your upgrade.
For the major-version migration, see [Upgrading from CTA v5 to CTA v6](cta-v5-to-v6.md).

Upgrading CTA is relatively easy as CTA is the tape backend and separated from direct interactions with archival service users **except when upgrading the workflow `cta-frontend`**.

RPM upgrades preserve whether each packaged service is enabled and restart it only when it is already running. See [RPM Packages and Services](../../deploy-and-configure/deployment/installation/rpm-packages.md#package-lifecycle) for the complete package lifecycle contract.

## Migrating TOML feature activation

When upgrading from configuration files using section-local `enabled` flags or `[experimental]`, migrate them before starting the new binaries.
The old keys are not aliases for the new schema; validate the migrated file with `--config-strict --config-check`.

| Previous setting | Replacement |
| --- | --- |
| `experimental.telemetry_enabled` | Root `enabled_features` contains `"telemetry"`; retain `[telemetry]` only when enabled. |
| `health_server.enabled` | Root `enabled_features` contains `"health_server"`. |
| `health_server.use_unix_domain_socket` | `health_server.transport = "unix"` or `"tcp"`; move TCP host and port into `[health_server.tcp]`. |
| `auth.<method>.enabled` | `auth.enabled_methods` lists selected methods; omit inactive method sections. |
| `experimental.opportunistic_batching_enabled` | Root `enabled_features` contains `"opportunistic_batching"` in PostgreSQL workflow API configurations. |
| `routines.<routine>.enabled` | `routines.enabled_features` lists selected routines; omit inactive sections. The objectstore `garbage_collect` routine needs no section. |
| `transfers.encryption.enabled` | `transfers.enabled_features` contains `"encryption"`; retain the section only when enabled. |
| `transfers.retrieve.rao.enabled` | `transfers.retrieve.enabled_features` contains `"rao"`; retain the section only when enabled. |

Remove `[experimental]` after migrating its flags.
Keep all previously intended capabilities in the appropriate lists, including routines, encryption and RAO that were enabled by default.
An omitted `enabled_features` list defaults to empty in non-strict mode; `auth.enabled_methods` defaults to `["jwt"]` for both APIs.
Supplied inactive sections are rejected in both modes.

Explicitly set `grpc.number_of_threads = 0` to preserve automatic sizing and, in objectstore API configurations, `scheduler.client.thread_stack_size_mb = 0` to preserve the system stack size.
For `defaults.repack.max_files_to_select`, use `0` to select all files, which was the previous omission behaviour despite examples describing a scheduler default.
Retain positive values when a fixed count, size or limit is intended.
Remove placeholder values for genuinely optional files that are not used.

## Test upgrade path

!!! danger
    Upgrading CTA can affect its disk-system integration: test the upgrade path with the disk system used by your deployment before upgrading production.

## Upgrade secondary frontends

Start by upgrading secondary frontends:

- secondary test `cta-frontend` for operators
- repack `cta-frontend`s
- `cta-frontend`s for operators

## Upgrade `cta-maintd`

You can start upgrading `cta-maintd` processes if these are running on separate machines or in containers.
If `cta-maintd` is running on the same OS as `cta-taped` it should be upgraded along with `cta-taped` process.

## Rollout `cta-taped` upgrade

In order to limit user visibility and the impact on archival throughput to/from tape `cta-taped` processes should be upgrade in a rolling window mode.

### Upgrade a fraction of your tape servers

For example upgrade 1-2 `cta-taped` processes per logical library and make sure that everything is OK for these.
Indeed a CTA code change could affect a specific tape drive model and you do not want to loose access to a full library by rolling out a new CTA version on all the tape drives at once in production.

### Upgrade remaining tape servers

Just slowly roll out the new CTA version on all the remaining tape drives.

### CTA upgrade principle for tape drives

This is the most time consuming part as a CTA Service can contain few hundreds of tape drives connected to a bit less tape servers.

In order to upgrade `cta-taped` processes on one tape server:

1. put down all the tape drives on this server using an explicit `reason` to notify other CTA administrators

    !!! example

        for example to put down all drives not down yet on *tapeserver001* with reason set to *upgrading*:

        ```shell
        cta-admin --json dr ls | jq -r '.[] | select(.host == "tapeserver001") | select(.driveStatus != "DOWN")| .driveName' | xargs -itoto cta-admin dr down toto --reason "upgrading"
        ```

2. from the cta operator frontend regularly check drives that are DOWN with a `ctaVersion` different than your target CTA version

    - upgrade CTA software on these using your own operations configuration manager (ansible, puppet, chef,...)

3. put back up tape drives with reason *upgrading* that are running the target CTA version

## Upgrade the main `cta-frontend`

If running a single `cta-frontend` process for the full CTA Service restarting it will mean that connected disk instance won't be able to contact CTA backend during the service restart tine and some requests will fail.

!!! info
    High Availability deployment of `cta-frontend-grpc` will allow to run multiple main `cta-frontend` processes and rollout a CTA upgrade with no user disruption.
