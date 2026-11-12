# Upgrading from CTA v5 to CTA v6

!!! info "Documentation incomplete"

    This guide is a skeleton for the CTA v5-to-v6 migration.
    The service account group requirements are recorded below; migration procedures and upgrade compatibility still need validation before this guide can be used for a production upgrade.

This page documents changes specific to upgrading from CTA v5 to CTA v6.
See [Upgrading CTA](cta.md) for general upgrade guidance and the [changelog](../../../changelog.md) for release-specific changes.

## Supported upgrade paths

TODO: Specify the supported CTA v5 source releases, CTA v6 target releases, and any required intermediate upgrades.
Document compatibility requirements for operating systems, disk systems, and mixed-version deployments.

## Preparation

TODO: Document the deployment inventory, backups, restore checks, maintenance window, and staging validation required before upgrading.
Identify which services must be stopped and how to handle outstanding archive and retrieve requests.

## Packages, services, and configuration

TODO: Map CTA v5 packages, service names, and configuration files to their CTA v6 replacements.
Document configuration conversion, authentication changes, local systemd overrides, and validation commands.

## Service account groups

Clean installations use `cta` as the account's primary group, without account-wide membership in `tape`.
Only `cta-taped` and `cta-rmcd` receive supplementary `tape` membership through their service or container configuration.
The packages do not currently migrate the primary group of an existing account.

TODO: Implement and validate migration from `cta:tape` to `cta:cta`, preserving the account's UID and unrelated supplementary memberships.
Remove account-wide `tape` membership so services without hardware access do not inherit it.
Include existing accounts in the full CTA v5-to-v6 upgrade compatibility check.
Restart all CTA services after upgrading so running processes acquire the new group settings.
Systemd manages ownership of the directories declared by `RuntimeDirectory=` and `LogsDirectory=` when the services start.

Review locally managed unit overrides and configuration permissions before restarting.
For runtime and log directories managed outside systemd, update CTA-owned files and directories from `cta:tape` to `cta:cta` while the affected services are stopped.
Keep tape devices and tape-access tools in the `tape` group.
For container deployments, follow the identity and volume guidance in [Docker Images](../../deploy-and-configure/deployment/installation/docker-images.md#configure-and-run-services).

## Catalogue and scheduler

TODO: Specify the required catalogue and scheduler schema versions, migration order, and treatment of existing scheduler state.
Link the validated steps to [Catalogue Schema Upgrades](catalogue-schema/index.md) and [Scheduler Schema Upgrades](scheduler-schema.md).

## Container deployments

TODO: Document image version selection, configuration and secret changes, volume migration, and rollout order.
For service identities and volume ownership, see [Docker Images](../../deploy-and-configure/deployment/installation/docker-images.md#configure-and-run-services).

## Upgrade sequence

TODO: Provide the validated order for upgrading frontends, maintenance daemons, media changer daemons, and tape daemons.
State which components can coexist across versions and where an outage is required.

## Verification

TODO: Provide commands and expected results for service health, effective user and group IDs, device access, logging, authentication, archive, and retrieve checks.
Define the checks that must pass before resuming normal operations.

## Recovery and rollback

TODO: Document recovery steps for failures at each migration stage, including package, configuration, catalogue, and scheduler changes.
State when rollback is supported, which backups are required, and when restoring state is necessary.
