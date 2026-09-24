# Upgrading CTA

Upgrading CTA is relatively easy as CTA is the tape backend and separated from direct interactions with archival service users **except when upgrading the workflow `cta-frontend`**.

RPM upgrades preserve whether each packaged service is enabled and restart it only when it is already running. See [RPM Packages and Services](../deployment/rpm-packages-and-services.md#package-lifecycle) for the complete package lifecycle contract.

## Test upgrade path

!!! danger
    Upgrading CTA can break EOS <-> CTA coupling: always test your upgrade path on a test EOS-CTA instance before attempting production upgrade.

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
