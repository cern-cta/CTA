# Creating a new version of the Catalogue Schema


## Introduction
The Catalogue schema is the description of the tables that represent the CTA Catalogue.

The Catalogue schema version number has the following format:

```
SCHEMA_VERSION_MAJOR.SCHEMA_VERSION_MINOR
```

*Example: 1.0, 1.1, 2.0*

!!! warning
    `SCHEMA_VERSION_MAJOR` should be modified only if the changes made to the schema are [not backward-compatible](incompatible-changes.md#definition) with the currently running version of CTA.

!!! danger
    While starting, CTA checks the `SCHEMA_VERSION_MAJOR` it is supposed to run against. If `SCHEMA_VERSION_MAJOR` is not correct, CTA will not start.

## Modifying the Catalogue schema

In order to make any changes to the CTA Catalogue schema, first a new schema version has to be created.

!!! danger
    Do not modify any `.sql` files without changing the version of the schema first (step 1 below)! If you do not update
    the version, the current schema will be overwritten by any modifications you make. (Roll back by removing your changes
    and rebuilding CTA). If in doubt, do a `git diff` to check that the current schema directory (in the catalogue folder)
    does not contain any changes.

To modify the Catalogue schema, follow these steps:

1. Modify the compile-time constants `CTA_CATALOGUE_SCHEMA_VERSION_MAJOR` and `CTA_CATALOGUE_SCHEMA_VERSION_MINOR`, found in `catalogue/cta-catalogue-schema/CTACatalogueSchemaVersion.cmake`.
2. Run the build of cta-catalogue-schema
```
## In the catalogue/cta-catalogue-schema directory
mkdir build && cd build
cmake ..
make
```

    --> This will create a new folder that will be named according to the schema version variables modified in step 1.

    --> Example: if the new schema version is 1.1, the directory 1.1 will be created in the catalogue directory.

3. Modify the schema by editing the `.sql` files located in the catalogue directory:
    - *databasetype*`_catalogue_schema_header.sql`
    - `common_catalogue_schema.sql`
    - *databasetype*`_catalogue_schema_trailer.sql`

    Where *databasetype* is `oracle`, `postgres` or `sqlite`.

4. Run the build of cta-catalogue-schema

    --> This will modify the files located in the folder created at step 2.

5. Try the new schema you created in an empty database. Your schema should work with Oracle, PostgreSQL and SQLite.

!!! tip
    You can use `cta-catalogue-schema-create` to create the new schema
