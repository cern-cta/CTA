# Manual CTA Catalogue schema upgrade

!!! warning
    This is **not** the recommended way to upgrade the CTA Catalogue schema, given its dependencies and complexity.
    Please consider the [automated procedure](automated.md), which provides a one-click upgrade solution for the CTA Catalogue schema upgrade.

## Prerequisites

#### 1. Choose a machine to run the upgrade

!!! warning
    It should not be a production server!

The upgrade should be run from a machine running CERN CentOS 7. It is recommended to run the upgrade from the container provided by the CTA team.

#### 2. Check the required tools are installed

* [cta-catalogue-schema-verify](../../tools/cta-catalogue-schema-verify.md)
* [Liquibase](https://docs.liquibase.com/commands/home.html)

#### 3. Check the `cta-catalogue.conf` file contains the connection string for the database to be upgraded

This configuration file will be used by the `cta-catalogue-schema-verify` command.

Example:

```bash
$ cat /etc/cta/cta-catalogue.conf
oracle:cta_production/PASSWORD@cta
```

#### 4. Upgrade the CTA software for the next catalogue release version

Use a CTA release that supports both the source and destination catalogue schemas for the migration. Check the [Release Notes](../../../release-notes.md) and the supported catalogue versions for the selected release before proceeding.

#### 5. Obtain the migration script containing the changes to be applied to the database

The migration scripts are located in the cta-catalogue-schema project repository, in the folder:
- [`cta-catalogue-schema/migrations/liquibase/`](https://gitlab.cern.ch:8443/cta/cta-catalogue-schema/-/tree/main/migrations/liquibase)

These files are named according to the following convention: `<SOURCE_VERSION>to<DESTINATION_VERSION>.sql`.

Example:
- To upgrade between schema versions `14.0` and `15.0`, use the file `14.0to15.0.sql`.

## Procedure to execute the manual schema upgrade

#### 1. Set all drives down

```bash
> cta-admin dr down '.*' --reason upgrading
```

#### 2. Verify the database schema is the version you wish to upgrade from

```bash
> cta-catalogue-schema-verify /etc/cta/cta-catalogue.conf
Schema version : 4.5
Checking indexes...
  SUCCESS
Checking tables, columns and constraints...
  SUCCESS
Status of the checking : SUCCESS
```

#### 3. Upgrade the database using the Liquibase command

It is recommended to run the liquibase command in the same directory as the [liquibase `.properties` file](https://docs.liquibase.com/concepts/connections/creating-config-properties.html):

```bash
# JAVA_OPTS="-Doracle.net.tns_admin=/etc" liquibase --defaultsFile=./liquibase.properties --changeLogFile=/home/smurray/CTA/catalogue/migrations/liquibase/oracle/14.0to15.0.sql update
```

#### 4. Verify the database schema has been successfully upgraded

```bash
# cta-catalogue-schema-verify /etc/cta/cta-catalogue.conf
Schema version : 4.6
Checking indexes...
  SUCCESS
Checking tables, columns and constraints...
  SUCCESS
Status of the checking : SUCCESS
```
