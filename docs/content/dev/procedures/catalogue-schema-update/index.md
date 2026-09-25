!!! info "WIP"
    This page is still work in progress.

## Making changes to the catalogue schema
This section explains how to create a new version of the CTA Catalogue schema, which involves the following steps:

- [Creating a new version of the schema](./create-new-version.md)
- Modifying the schema, following the [naming conventions](./naming-conventions.md)
- Creating and testing the upgrade/migration scripts
- Pushing these modifications to the CTA-catalogue-schema repository

Usually a partial or failed upgrade can be rolled back, though this is not possible in all cases.

## Summary of the steps

!!! warning
    Whenever you make any change to the CTA schema, you must create and test the corresponding Liquibase upgrade script.
    Schema changes should only be merged to master when the tests pass.

Follow these steps to do a schema update:

- Install `cta-catalogue-schema-verify`
- Identify the changes that have been made to the schema: are they [backward-compatible](./backward-compatible-upgrades.md) or [backward-incompatible](./backward-incompatible-upgrades.md)?
- Create the [Liquibase migration/upgrade changelog file](./create-liquibase-changelog-file.md). These are basically SQL scripts.
- Test the Liquibase upgrade scripts and the full upgrade of CTA on a CTA test environment
- When the tests pass, you can push your changes to CTA-catalogue-schema main

