# Backward-incompatible migration/upgrade

!!! warning
    This page will just give an idea about how backward-incompatible migrations could be done. Write the liquibase changelog files and run a complete upgrade in a full CTA test environment.

## Definition

Backward-incompatible migrations are modifications that are made to the schema in a way that it can not be used by the current version of CTA.

Example: CTA current version is 1.6 and its Catalogue schema version is 1.9. The CTA version 1.6 can not run with the Catalogue schema version 2.0 because the latter contains modifications that are not compatible with CTA version 1.6.

## Backward-incompatible modifications

Here is the list of Backward-incompatible modifications:

- Rename a COLUMN, a TABLE or a VIEW
- Change the data type of a COLUMN
- Remove a COLUMN, a TABLE or a VIEW that is still used by the current CTA version

## Transition versions

A transition version bridges a change that cannot safely be made in a single software or schema upgrade. For example, renaming a column may require first adding the replacement alongside the old column, migrating and keeping their data consistent, and deploying CTA code that uses the replacement. Only after all components have moved away from the old column can a later schema release remove it.

Choose and test the software/schema combinations and deployment order for each stage. Where a CTA release must run against both old and new schema majors, its code must support both and `supportedCatalogueVersions` must declare that support. Changing this list alone does not provide compatibility. An intermediate schema may also be needed; the following examples illustrate the approach rather than a universal upgrade sequence.

### Rename a COLUMN, a TABLE or a VIEW

!!! warning
    This is just to give an idea on how to do it, every migration is different and should be thought of.

    REMEMBER that doing backward-incompatible migration implies to increase the SCHEMA_VERSION_MAJOR number of the CTA Catalogue version (more informations [here](new-version.md))

!!! info
    The COLUMN renaming will be taken as an example.

1. Create a "transition" version of the Catalogue and CTA

    1. Increase the CTA_CATALOGUE_SCHEMA_VERSION_MINOR number in catalogue/cta-catalogue-schema/CTACatalogueSchemaVersion.cmake

    2. Add the COLUMN with the new name and same data type as the current one in the schema .sql files

    3. Compile CTA (with a "transition" version number) with the modifications so that the new COLUMN name is used and test it.

    4. [Create the liquibase changelog file](migration-scripts.md) and execute it against the database : Add the COLUMN with the new name and same data type as the current one and copy the data from the current COLUMN to the new one

          --> Use a trigger to synchronize between the two columns

    5. Update all CTA components with the transition version compiled at step 1.3

          --> It will use the new COLUMN name

2. Create the "final" new version of the Catalogue and CTA

    1. Change the CTA_CATALOGUE_SCHEMA_VERSION_MAJOR and MINOR in catalogue/cta-catalogue-schema/CTACatalogueSchemaVersion.cmake

    2. Remove the old COLUMN from the schema .sql files

    3. Recompile CTA (with the new "final" version)

    4. Test that the new CTA "final" version works without the old COLUMN

    5. [Create the liquibase changelog file](migration-scripts.md) and execute it against the database. The changelog file sql statements should delete the old column and update the SCHEMA_VERSION_MAJOR and MINOR number in the CTA_CATALOGUE table

    6. Update all CTA component with the final version of the schema.

### Change the data type of a COLUMN

!!! warning
    This is just to give an idea on how to do it, every upgrade is different and should be thought of.

    REMEMBER that doing backward-incompatible upgrade implies to increase the SCHEMA_VERSION_MAJOR number of the CTA Catalogue version (more informations [here](new-version.md))

Same as [previous section](incompatible-changes.md#rename-a-column-a-table-or-a-view) but a conversion of the values should be done in between.

### Remove a COLUMN, a TABLE or a VIEW

!!! warning
    This is just to give an idea on how to do it, every upgrade is different and should be thought of.

    REMEMBER that doing backward-incompatible upgrade implies to increase the SCHEMA_VERSION_MAJOR number of the CTA Catalogue version (more informations [here](new-version.md))

#### Steps to follow

Do almost the same as [previous section](incompatible-changes.md#rename-a-column-a-table-or-a-view), no synchronization or addition of a COLUMN, TABLE or VIEW is needed.

1. Create a "transition" version of CTA and the Catalogue Schema where the COLUMN, TABLE or VIEW is deleted.

2. Update CTA so that it does not use the COLUMN, TABLE or VIEW anymore

3. Do the "transition" upgrade of the database

4. Create the "final" version of CTA and the Catalogue Schema

5. Update the Catalogue schema

6. Update CTA so that it runs against the "final" version of the Catalogue Schema

Validate each stage using [Testing catalogue migrations](testing.md), including the required intermediate software and schema versions.
