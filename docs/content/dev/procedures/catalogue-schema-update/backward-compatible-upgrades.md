# Backward-compatible migration/upgrade

## Definition

A backward-compatible modification is a modification that allows the future Catalogue schema to be used by the current CTA version and the new version of CTA.

Example: CTA version 1.0 currently runs on Catalogue version 1.0. Updating the schema from 1.0 to 1.1 with **only** backward-compatible modifications will not avoid the version 1.0 of CTA to run against the new schema version 1.1.

## Backward-compatible modifications

Here is a list of backward-compatible modifications:

- Add a TABLE or a VIEW
- Add a COLUMN
- Remove a COLUMN that is not used by the current CTA version nor the new version of CTA
- Remove a CONSTRAINT
- Add an INDEX

## Upgrade script creation and testing steps

### 1. Check the Catalogue database-to-migrate schema consistency

Check that the Catalogue database-to-migrate schema is the version you expect and that it does not contain any modifications. This check has to be done with the [**cta-catalogue-schema-verify**](./../../../ops/tools/cta-catalogue-schema-verify.md) tool.

```bash
$ cta-catalogue-schema-verify path_to_catalogue_configfile.conf
Schema version : 1.0
Checking indexes...
  SUCCESS
Checking tables, columns and constraints...
  SUCCESS
Status of the checking : SUCCESS
```

### 2. Create the Liquibase Changelog file

Follow the [instructions to create the liquibase changelog file](./create-liquibase-changelog-file.md).

### 3. Verify the correctness of the Liquibase Changelog

After the changelog has been generated, it is tested by following the steps described in the [Catalogue Updater repository](https://gitlab.cern.ch/cta/eoscta-operations/containers/cta-catalogue-updater).

