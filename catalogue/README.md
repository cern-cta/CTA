# Create a new version of the Catalogue Schema

## Introduction

The Catalogue schema is the description of the tables that represent the CTA Catalogue.

The Catalogue schema is associated to a number that has the following format :
```
SCHEMA_VERSION_MAJOR.SCHEMA_VERSION_MINOR
```
*Example: 1.0, 1.1, 2.0*

**WARNING**
    The changing of the *SCHEMA_VERSION_MAJOR* number has to be modified **ONLY** if the changes made to the schema are [not backward-compatible](https://eoscta.docs.cern.ch/catalogue/upgrade/backward_incompatible_upgrades/) with the currently running version of CTA.

**DANGER**
    While starting, CTA will check the *SCHEMA_VERSION_MAJOR* schema version it is supposed to run against. If the *SCHEMA_VERSION_MAJOR* is not the correct one, CTA will not start.

## Modify the Catalogue schema

**WARNING**
    Modifying the Catalogue schema means that a new version of the schema has to be created.

**DANGER**
    Do not modify any .sql files without changing the version of the schema first (follow the following step 1) ! If you do, the current schema will be overwritten by the modifications you made. (Rollbackable by removing your changes and by building CTA again). If any doubts, do a git diff and check that the current schema directory located in the catalogue folder does not contain any changes.

In order to modify the Catalogue schema, please follow the following steps.

1. Modify the **CTA_CATALOGUE_SCHEMA_VERSION_MAJOR** and the **CTA_CATALOGUE_SCHEMA_VERSION_MINOR** variables that are located in the directory *cmake/CTAVersions.cmake*.
2. Run the build of CTA

    --> It will create a new folder that will be named according to the schema versions variables modified at step 1. (Example: if the new schema version is 1.1, the directory 1.1 will be created in the catalogue directory).

3. Modify the schema by editing the .sql files located in the catalogue directory :
    - *databasetype*_catalogue_schema_header.sql
    - common_catalogue_schema.sql
    - *databasetype*_catalogue_schema_trailer.sql

    Where *databasetype* is either **oracle** or **postgres** or **sqlite** or **mysql**.

4. Run the build of CTA
    --> It will modify the files located in the folder created at step 2.

5. Try the new schema you created in an empty database. Your schema should work with Oracle, PostgreSQL, MySQL and SQLite.

**TIP**
        You can use the **cta-catalogue-schema-create** tool to create the new schema

6. If everything works, let's define a [upgrade strategy](https://eoscta.docs.cern.ch/catalogue/upgrade/).