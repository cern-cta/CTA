# v3.1-8

## Summary

This release contains the CTA software Recommended Access Order (RAO) implemented for LTO drives to improve retrieve performances

### Features

- CTA software Recommended Access Order (RAO) implemented for LTO drives
- Upgraded EOS to 4.8.20-1

### Bug fixes

- cta/CTA#895 [catalogue] RdbmsCatalogue::deleteLogicalLibrary does not delete empty logical library 
- utils::trimString() now returns an empty string if the string passed in parameter contains only white-space characters

# v3.1-7

## Summary

This release contains a correction of a performance issue introduced in the v3.1-6 version

### Bug fixes

- cta/CTA#893 Corrected slowliness of RdbmsCatalogue::getArchiveFileToRetrieveByArchiveFileId()

# v3.1-6

## Summary

This release contains some minor improvements.

### Features

- cta/CTA#881 cta-fst-gcd logs can be now sent to stdout by command line option for container based deployments
- cta/CTA#885 cta-admin should be able to query by sys.archive.file_id
- Upgraded EOS to 4.8.15-1
- Upgrading xrootd from 4.12.3-1 to 4.12.4-1

### Modifications

- cta/CTA#890 CTA RPMs should only use the xrootd-client-libs package
- buildtree installation scripts are made compatible with Centos 7
- cta/CTA#892 Modified the log level of the triggering of Archive and Retrieve mounts
- cta/CTA#889 It is not possible to retrieve a file that is not active anymore

### Bug fixes

- cta/CTA#877 ObjectStore.RetrieveQueueAlgorithms unit tests fails or succeeds base on version of cmake
- cta/CTA#888 Garbage collector race condition
- cta/CTA#891 Corrected Repack Archive subrequest creation time

# v3.1-5

## Summary

This release is a bug fix release.

### Features

- cta/CTA#863 Prevent SQLite database files from being used as the CTA catalogue database backend
- cta/CTA#870 Adds "cta-admin failedrequest rm" command

### Modifications

- cta/CTA#861 cta-admin comment column is flush left

### Bug fixes

- cta/CTA#862 Unable to delete tabtest tape pool because it is in an archive route
- cta/CTA#860 Correct contents of cta-lib-catalogue RPM and correct dependencies on it
- Reinstates missing "cta-admin failedrequest ls --summary" option
- cta/CTA#865 Empty the RetrieveQueue in the case of cancellation of a retrieve request when the drive is down


# v3.1-4

## Summary

This release improves database performance, improves the `cta-admin`
command-line tool based on operator requests and removes the deprecated
`cta-objectstore-unfollow-agent` command again for the benefit of operators.

### Modification

- cta/CTA#858 Remove dependency between the cta-migration-tools RPM and librados2
- cta/CTA#857 Remove unnecessary LEFT OUTER JOIN clauses from the CTA catalogue
- cta/CTA#852 Fixing sqlite CI use case
- cta/CTA#850 [repack] If the --no-recall flag is passed to the repack request submission the --disabled-flag test should not be done.
- cta/CTA#846 cta-admin tapefile ls: list by fileid
- cta/CTA#840 Remove cta-objectstore-unfollow-agent from cta-objectstore-tools


# v3.1-3

## Summary

### Features

- Upstream eos 4.8.10-1
  - Adds a fix for `eos-ns-inspect` to [correctly list extended attributes on files](https://its.cern.ch/jira/browse/EOS-4319)
- The `--no-recall` flag can be passed to `cta-admin repack add` command:
  - The repack request will NOT trigger any retrieve mount.  Only the files that are in the repack buffer will be considered for archival. This is used to inject recoverred files from a tape with some hard to read fseqs.
- New `cta-admin schedulinginfos ls` command available to list potential mounts detected by the scheduler

### Modification

- Shrinked `cta-admin repack ls` tabular output
- cta-admin help commands listed in alphabetical order
- Catalogue connection pool improvements
- The scheduler will take the tape that has the highest occupancy for archival in order to limit data scattering across all available tapes


# v3.1-2

## Summary

### Modification

- Added database upgrade/changelog script oracle/3.0to3.1.sql


# v3.1-1

## Summary

### Modification

- Corrected bugs on cta-objectstore-create-missing-repack-index tool
- Corrected a bug that caused crash of all tapeservers while scheduling
- Catalogue schema version 3.1 : addition of a new index on the TAPE table
- Catalogue and Unit tests improvements


# v3.0-3

## Summary

### Modification

- The cta-statistics-update tool updates the tape statistics one by one


# v3.0-2

## Summary

### Features

- Upstream eos 4.8.3-1
- Upstream xrootd 4.12.3-1
- Mount policies are now dynamically updated on queued Archive and Retrieve requests
- ShowQueues now display queued retrieves on disabled tapes


# v3.0-1

## Summary

### Features

- Upstream eos 4.8.2-1
- Upstream xrootd 4.12.1-1
- Catalogue schema updated to version 3.0
- Catalogue production is protected against dropping
- Management of media types
- File recycle-bin for file deletion
- cta-admin drive ls display reason in tabular output
- cta-send-event allowing to manually retry to Archive or Retrieve a file
- KRB5 authentication for CLOSEW and PREPARE events
- prevent eos /eos/INSTANCE/proc/conversion worfklows from deleting files from CTA

### Modifications

- Tape comments are optional and can be removed
- Workflow triggered by a file that is in /proc will not trigger anything on CTA side
- CASTOR-TO-CTA migration adapted to new schema
- Repack submission will fail if no mount policy is given

### Bug fixes

- Fixed archive failure requeueing with no mount policies


# v2.0-5

## Summary

### Features

- Upstream eos 4.7.12-1
- Added support for FileID change in EOS that occurs during conversion
  - fileID updated in the catalogue when frontend receives Workflow::EventType::UPDATE_FID event


# v2.0-3

## Summary

### Features

- Upstream eos 4.7.9-1
- Adding reason and comment to cta drive objects
  - Allow to better track tape drive usage in production
  - Allow to track reasons that conducted a drive to be set down


# v2.0-2

## Summary

### Features

- Upstream eos 4.7.8-1
- Upstream xrootd 4.11.3-1
- Upstream ceph nautilus 14.2.8-0
- Fix for xrdfs query prepare `on_tape` logic
- More tests on the tape drive
  - Chech that device path exists
  - No drive name duplication allowed anymore in objectstore

# v2.0-1

## Summary

### Features 

- New schema version 2.0
  - *VIRTUAL_ORGANIZATION* has its own table
  - *DISK_FILE_PATH* is now resolved on eos instance using grpc and not duplicated anymore in tape catalogue
- Upstream eos 4.7.2-1 (CentOS 7 packages)
- Upstream xrootd-4.11.2-1 for CTA

**Upgrade from previous catalogue version is not provided**

# v1.2-0

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
