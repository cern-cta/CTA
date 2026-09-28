!!! info "Documentation incomplete"

    This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.

# Initialisation and Verification

Bring up a CTA deployment after installing and configuring its services.

## Initialise databases

TODO: Document creating and verifying the catalogue and the selected scheduler backend.

## Register administrative and storage resources

TODO: Document the initial administrator, disk instance, virtual organisation, libraries, drives, pools, and policies.

## Start and verify services

TODO: Document service startup, health checks, and the order of dependency checks.

## Verify the disk-system integration

TODO: Document an archive/retrieve verification through the selected disk system, keeping system-specific commands in its integration guide.

## Production-catalogue protection

TODO: Include setting and checking `IS_PRODUCTION` in the commissioning checklist. See [`cta-catalogue-schema-set-production`](../../tools/cta-catalogue-schema-set-production.md) and the protection described by [`cta-catalogue-schema-drop`](../../tools/cta-catalogue-schema-drop.md).

## Acceptance checks and handover

TODO: Document the expected results for archive, retrieve, and copy verification, failure investigation, monitoring readiness, and the resources created during commissioning. Keep the EOS-specific setup in [EOS Configuration](../integrations/eos/configuration.md).
