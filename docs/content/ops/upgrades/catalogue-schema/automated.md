# Automated CTA Catalogue schema upgrade

!!! warning
    If the DB schema upgrade fails, there is no automated rollback to the previous schema.
    Rollback will involve some manual steps and not all schema changes can be rolled back (for example,deleting columns or tables).
    Before upgrading a production DB, it is strongly recommended to back up the DB and to execute the upgrade procedure **against a clone** of the DB before touching the production DB.

The procedure for the DB schema upgrade is described below.
This is the same procedure used at CERN to upgrade the catalogue schema in production and preproduction.

For local development the upgrade should be done manually on the developer's machine.

Before proceeding with the upgrade it is advised to read the *CTA Release Notes* to be aware of any extra work that may be necessary for certain catalogue upgrades.

## Pull the container from the public registry

The `stable` tag contains the most recent version of the container tested to work at CERN, and is the recomended tag.

To start the automated upgrade procedure, pull the latest `stable` image from the [CTA public container registry](https://gitlab.cern.ch/cta/public_registry) in gitlab.

Example:
```sh
# podman pull gitlab-registry.cern.ch/cta/public_registry/container_registry/cta-catalogue-updater:stable
Trying to pull gitlab-registry.cern.ch/cta/public_registry/container_registry/cta-catalogue-updater:stable...
Getting image source signatures
Copying blob 609c31f8513b skipped: already exists
Copying blob bcc349949b20 skipped: already exists
Copying blob 968e711a3331 skipped: already exists
Copying config eed62e9a05 done
Writing manifest to image destination
eed62e9a05255991660aef71ef84d5161a10ecf5fa32d66b3d3221c26b381bba
```

## Execute the upgrade procedure

### Requirements

A few requirements must be met before running the container:

- Mount `cta-catalogue.conf` in the directory `/shared/etc_cta` (see example in the [CTA repository](https://gitlab.cern.ch/cta/CTA/-/blob/main/catalogue/cta-catalogue.conf.example)).
- Mount the *yum* repos to download the CTA RPMs in the directory `/shared/etc_yum.repos.d`.

### Procedure

After meeting the requirements, run the container with the following cmdline arguments:

- `-f|--catalogue-source-version <catalogue from version>`
    - The current version of the catalogue schema.
- `-t|--catalogue-dest-version <catalogue to version>`
    - The version of the catalogue to upgrade to.
- `-c|--command <liquibase command>`
    - The liquibase command to run (usually `update`, but also `rollback-count` or [others](https://docs.liquibase.com/commands/home.html))

Optionally, the following cmdline arguments can also be used:

* `-d|--disable-schema-verify`
    - If specified, skip the schema verification check (steps `2`, `4`, and `6`).
* `-s|--preflight-command <preflight command>`
    - Preflight command to run before upgrade starts (before step **4**).

!!! example

    Example, using podman:

    ```sh
    podman run -it -v /etc/cta:/shared/etc_cta:ro -v /etc/yum-puppet.repos.d:/shared/etc_yum.repos.d:ro gitlab-registry.cern.ch/cta/public_registry/container_registry/cta-catalogue-updater:stable -f 14.0 -t 15.0 -c update -d
    ```

#### Upgrade steps explained

For reference, these are the steps that the container takes to perform the catalogue upgrade:

1. Install the necessary RPMs to connect to the database backend.
2. Install the `cta-catalogueutils` RPM to get access to the `cta-catalogue-schema-verify` command.
3. Clone the CTA repo and checkout the `cta-catalogue-schema` submodule at the tag provided with `-t|--catalogue-dest-version`. Checking out the catalogue destination version tag will ensure the migration skip is present.
4. Run the `cta-catalogue-schema-verify`command to ensure the current catalogue schema is correct.
5. Perform the liquibase command the user specified (normally the `update` command, but can also use the `rollbackCount` command to rollback an update)
6. Run the `cta-catalogue-schema-verify`command to ensure the changes performed by liquibase left the catalogue with a correct schema.
