# v1.1-0

## Summary

### Features 

- cta-admin tapefile ls command: list the tape files located in a specific tape
- New schema verification tool (cta-catalogue-schema-verify)
- New tape statistic updater tool (cta-statistics-update)
- CTA Frontend has configurable maximum file size limit (cta.archivefile.max_size_gb), default 2TB

### Modifications

- Backward-compatible Catalogue schema changes:
  - CTA_CATALOGUE table contains a status that can be 'UPGRADING' or 'PRODUCTION'. If the status is UPGRADING, the columns NEXT_SCHEMA_VERSION_MAJOR and NEXT_SCHEMA_VERSION_MINOR will contain the future version number of the schema.
  - INDEXES and CONSTRAINT renaming to follow the [naming convention](https://eoscta.docs.cern.ch/catalogue/naming_convention/)
  - UNIQUE CONSTRAINT on ARCHIVE_ROUTE (UNIQUE(STORAGE_CLASS_ID, TAPE_POOL_ID))
  - Creation of an INDEX ARCHIVE_FILE_DFI_IDX on ARCHIVE_FILE(DISK_FILE_ID)
  - Added 3 columns to the TAPE table : NB_MASTER_FILES, MASTER_DATA_IN_BYTES, DIRTY
- Minor changes to cta-admin command syntax
- New configuration file for gRPC namespace endpoints so CTA can query EOS namespace
- Archive requests sent to hard-coded fail_on_closew_test storage class will always fail with an error

### Bug fixes

- The scheduler does not return a mount if a tape is disabled (unless if the tape is repacked with the --disabledtape flag set)

### Improvements 

- CASTOR-To-CTA migration improvements
- Better cta-admin parameter checking and column formatting
- cta-admin --json handles user errors from the Frontend by outputting an empty array [] on stdout and error message on stderr. Error code 1 is returned for protocol errors and code 2 is returned for user errors.
- CTA Frontend logs which FST sent the archive request

## Package changes

- eos-4.6.8 which brings the delete on close feature

## Upgrade Instructions from v1.0-3

### 1. Upgrade the Catalogue schema version from version 1.0 to 1.1

Before updating CTA, the Catalogue schema should be upgraded. Here is the link to the documentation about the database schema updating procedure : [https://eoscta.docs.cern.ch/catalogue/upgrade/](https://eoscta.docs.cern.ch/catalogue/upgrade/)

The liquibase changeLog file is already done so you can directly run the [*liquibase update*](https://eoscta.docs.cern.ch/catalogue/upgrade/backward_compatible_upgrades/#3-run-the-liquibase-updatesql-command) command with the changeLogFile located in the directory *CTA/catalogue/migrations/liquibase/oracle/1.0to1.1.sql*.

### 2. Update CTA components

TODO : Instructions about how to update the tapeservers and the frontend.
