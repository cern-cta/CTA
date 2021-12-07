# v4.3-4

## Summary

### Features
- Upgraded EOS to 4.8.67 in CI versionlock.list file
  - EOS/EOS-4976 Fix activity field passed from EOS to CTA
- cta/CTA#607 - Add client host and username in cta-frontend logs
- cta/CTA#777 - Minimize mounts for dual copy tape pool recalls
- cta/CTA#928 - Add youngest request age to cta-admin sq
- cta/CTA#1020 - cta-restore-deleted-files command for restoring deleted files
- cta/CTA#1026 - Add activity Mount Policy resolution to CTA
- cta/CTA#1057 - Remove support for MySQL
- cta/CTA#1069 - Open BackendVFS ObjectStore files in R/W mode when obtaining exclusive locks
- cta/CTA#1070 - Update eos to version 4.8.67
- cta/CTA#1074 - Improve error reporting when retrieving archive
- cta/CTA#1077 - Remove activity fair scheduling logic
- cta/CTA#1083 - Upgrade ceph to version 15.2.15
- cta/CTA#1068 - Build cta with gcc 7.x(C++17) and fix deprecated code

### Bug fixes
- cta/CTA#1059 - Clean up warnings reported by static analysis
- cta/CTA#1062 - cta-admin tf rm should store the diskFilePath when deleting the tape file copy
- cta/CTA#1073 - Retry failed reporting for archive jobs
- cta/CTA#1078 - fix STALE message when cta-taped is restarting
- cta/CTA#1081 - refactor database queries for drive ls

# v4.3-3

## Summary

### Features
- cta/CTA#1053 Remove the objectstore presence in another classes
- cta/CTA#501 cta-taped should set the state to DOWN when machine rebooting

### Bug fixes
- cta/CTA#1056 Fix bugs in cta 4.3-2
- cta/CTA#1058 Remove helgrind_annotator from production

# v4.3-2

## Summary

### Features
- cta-admin: All short options with more than one character now require two dashes

### Bug fixes
- cta/CTA#1013 reportType uninitialized
- cta/CTA#1044 Fix segmentation fault due to uninitialized optional value and remove diskSpaceReservations from `cta-admin dr ls`

# v4.3-1

## Summary

### Features
- cta/CTA#976 Add logical part of Drive Status using Catalogue
- cta/CTA#988 Add diskSpaceReservations map in cta-admin --json dr ls output.
- cta/CTA#983
  - Add cta-versionlock helper script to cta-release package
  - Update cta repo file to use the new public repo
- cta/CTA#1036 Better error reporting in cta-admin tools
- cta/CTA#1039 Improve logging of cta admin commands in cta frontend
- cta/CTA#1041 Fix host values in cta-admin commands

### Bug fixes
- cta/CTA#501 cta-taped should set the state to DOWN when machine rebooting
- cta/CTA#955 cta-taped daemon should stop "immediately" and cleanly when systemctl stop/restart is executed
- cta/CTA#991  Drive is not put down if the device file is removed while cta-taped is running and before a data transfer
- cta/CTA#996  Removes PARALLEL from migration scripts
- cta/CTA#1029 Fix segmentatin fault in frontend when list repacks of a tape that has been deleted in the catalogue
- cta/CTA#1031 Fix Warning in updateDriveStatus
- cta/CTA#1032 cta-admin dr ls crashes the frontend if executed during an archive/retrieve

# v4.2-3

## Summary

### Features
- cta/CTA#983
  - Add cta-versionlock helper script to cta-release package
  - Update cta repo file to use the new public repo
- cta/CTA#1036 Better error reporting in cta-admin tools
- cta/CTA#1039 Improve logging of cta admin commands in cta frontend
- cta/CTA#1041 Fix host values in cta-admin commands
- cta/CTA#1026 `cta-admin sq` now shows the mount policies of potential mounts

### Bug fixes
- cta/CTA#1029 Fix segmentatin fault in frontend when list repacks of a tape that has been deleted in the catalogue
- cta/CTA#1032 cta-admin dr ls crashes the frontend if executed during an archive/retrieve
- cta/CTA#996  Removes PARALLEL from migration scripts
- cta/CTA#1035 log configuration options on frontend startup
- cta/CTA#1042 Do not iterate over retrieve queues when holding global scheduler lock
  - Repacks on a disabled tape must now use a mount policy whose name starts with repack
  - There is no longer an empty mount when a disabled/broken tape queue is full of deleted requests
- cta/CTA#1027 Mitigate popNextBatch bad behaviour in archive queues
- Migration tools: fixes filemode of files imported from CASTOR to EOS

# v4.2-2

## Summary

### Features

### Bug fixes
- cta/CTA#1029 Fix segmentatin fault in frontend when list repacks of a tape that has been deleted in the catalogue

# v4.2-1

## Summary

### Features
- Catalogue schema version 4.2
- cta/CTA#1001 Maximum file size is now defined by VO instead of globally.
- cta/CTA#1019 New command `cta-readtp` allows reading files from tape and verifying their checksum

# v4.1-1

## Summary

### Features
- Catalogue schema version 4.1
- cta/CTA#1016 New options for filtering deleted files using `cta-admin rtf ls` command.
- cta/CTA#983 Add cta-release package for public binary rpm distribution.
- cta/CTA#980 Add external encryption script option
- cta/CTA#976 Define a new table in the DB schema to contain the drive status.
- cta/CTA#834 New command "recycletf restore" allows undeleting a copy of a file from tape deleted using tapefile rm
- [frontend] New command "tapefile rm" allows deleting a copy of a file from tape

### Bug fixes

- cta/CTA#1014 Fix last column alignment when more than 1000 items are listed.

# v4.0-5

## Summary

### Features
- [frontend] Add options to "tapepool ls" to filter tapepools on their name, vo and encryption
- cta/CTA#898 cta-send-event now gets the requester id and eos instance as command line arguments
- cta/CTA#1005 "tape ls" now can filter tapes on wether they were imported from CASTOR
- cta/CTA#1006 "repack ls" now shows the tapepool of the tape being repacked

### Bug fixes

# v4.0-5

## Summary

### Features
- [frontend] Add options to "tapepool ls" to filter tapepools on their name, vo and encryption
- cta/CTA#898 cta-send-event now gets the requester id and eos instance as command line arguments
- cta/CTA#1005 "tape ls" now can filter tapes on wether they were imported from CASTOR
- cta/CTA#1006 "repack ls" now shows the tapepool of the tape being repacked

### Bug fixes

- [frontend] Adds missing break after "schedulinginfo ls" command
- cta/CTA#999 Adds a default mount rule
- cta/CTA#1003 The expansion of a repack request now fails if the archive route for archiving the repacked files is missing

# v4.0-4

### Bug fixes

- cta/CTA#1002 Do not requeue report jobs when reportType is NoReportRequired

# v4.0-3

### Features

- Upgraded ceph to version 14.2.20
- Adds cta-verify-file to cta-cli RPM

# v4.0-2

## Summary

### Features

- Upgraded EOS to 4.8.45 in CI versionlock.list file
  - EOS/EOS-4658 EOS workflow engine should not insist on the W_OK mode bit (for prepare ACL)
  - EOS/EOS-4684 Make the "file archived" GC aware of different EOS spaces
- Upgraded eos-xrootd to 4.12.8 in CI versionlock.list file
- cta/CTA#966 Unable to distinguish empty and wrong tape pool
- cta/CTA#926 Improve MigrationReportPacker::ReportSkipped::execute() exception message
- cta/CTA#584 Validate checksum when recalling from tape

### Bug fixes

- cta/CTA#582 Changes cta-admin storageclass add --copynb to --numberofcopies
- cta/CTA#987 Do not import metadata for zero-length files into CTA Catalogue
- cta/CTA#984 Removes diskFileGid == 0 check
- cta/CTA#669 cta-taped now display the correct error message when the drive device does not exist
- cta/CTA#777 Minimize mounts for dual copy tape pool recalls - Temporary fix
- cta/CTA#927 Bad message for archive route pointing non-existing pool
- cta/CTA#930 Batched the queueing and the deleting of the repack subrequests
- cta/CTA#969 `xrdfs query prepare` malformed JSON output
- cta/CTA#972 Updates cta-taped and rmcd man pages
- cta/CTA#967 CI DB cleanup issues
- cta/CTA#982 cta-fst-gcd ignored eos files with fid>4294967295
- cta/operations#352 Unable to delete empty tape pool

# v4.0-1

## Summary

This version contains the last and clean version of the CTA catalogue schema. This CTA version can not run anymore with the
CTA v3.1-14.

### Features

- Catalogue schema version 4.0
- cta/CTA#964 Adds failure log messages to processCLOSEW in CTA Frontend
- When the operator submits a tape to repack, a check is done about the tape state before queueing the repack request to ensure it can be repacked
- Oracle catalogue migration scripts 3.1to3.2.sql: replaced DELETE FROM table_name by TRUNCATE TABLE table_name

# v3.2-1

## Summary

This version is a transition version between CTA v3.1-14 and CTA v4.0. The functionalities of the v4.0 are implemented,
but at the database level, the columns to be deleted have been put as NULLABLE and the NOT NULL columns to be added as NULLABLE.
The xrootd-ssi-protobuf-interface is not up-to-date with the CTA v4.0: deprecated fields have not been removed.

### Features

- Catalogue schema version 3.2
- Upgraded EOS to 4.8.37-1
- cta/CTA#922 The superseded concept has been removed and replaced by a new recycle bin
- cta/CTA#943 A new tape lifecycle logic has been implemented
- cta/CTA#948 The max drives allowed for reading and writing are now set per virtual organization and not per mount policy anymore
- cta/CTA#952 Reclaiming a tape resets the IS_FROM_CASTOR flag to 0
- cta/CTA#951 The query used by RdbmsCatalogueTapeContentsItor has been put back to the state it was in v3.1-13
- cta/CTA#883 Tape verification tool

# v3.1-14

## Summary

### Features

- Upgraded EOS to 4.8.35-1
- cta/CTA#954 The drive is put down if the CleanerSession fails to eject the tape from it after a shutdown
- cta/CTA#945 The timeout for tape load action is now configurable

### Bug fixes

- cta/CTA#957 The state of the successful Archive for repack jobs is now changed before being queued for reporting
- cta/CTA#958 The RepackRequest garbage collection now changes the owner of the RepackRequest

# v3.1-13

## Summary

### Features

- Upgraded EOS to 4.8.34-1
- Upgraded xrootd to 4.12.6-1

### Bug fixes

- cta/CTA#941 Slow `cta-admin sq` even when there is very little activity
- cta/CTA#951 Improve the performance of RdbmsCatalogueTapeContentsItor
- cta/CTA#939 cta-objectstore-dereference-removed-queue removes all kind of manually deleted queues from the RootEntry

# v3.1-12

### Features

- Upgraded EOS to 4.8.30-1

# v3.1-11

### Features

- cta/CTA#932 Add environment file for cta-frontend service: frontend configured to use 10 XRootD polling threads by default
- cta/CTA#292 Allow non interactive usages of cta-admin with sss authentication
- Upgraded EOS to 4.8.29-1
   - cta/operations#155 Fix for conversion issues
   - cta/operations#154 Improve sys.retrieve.req_id to allow to cancel retrieves on a running instance: adding epoch timestamp in ids
   - EOS-4505 Separate archive and retrieve ACLs in EOS: only needs p ACL for prepare

### Bug fixes

- cta/operations#150 high priority Archive job not scheduled when Repack is running: fixed

# v3.1-10

## Summary

This version contains different bug fixes

### Features

Unuseful WARNING logs are now DEBUG logs

### Bug fixes

cta/CTA#837 Repack now fails if the repack buffer VID directory cannot be created during expansion
cta/CTA#920 Archive and Retrieve error report URL correction on the cta-send-event cmdline tool
cta/CTA#923 Corrected the cta-admin showqueues command to display all the retrieve queues of tapes that are on the same tapepool

# v3.1-9

## Summary

This release contains an improvement allowing to fetch the EOS free space via an external script for backpressure

### Features

- Upgraded EOS to 4.8.26-1
- cta/CTA#907 For backpressure, the EOS free space can be fetched by calling an external script

### Bug fixes

- cta/CTA#917 Corrected the bug in the cta-admin showqueues command in the case ArchiveForUser and ArchiveForRepack exist for the same tapepool
- cta/CTA#919 Archive queue oldestjobcreationtime is now updated at each pop from the ArchiveQueue

# v3.1-8

## Summary

This release contains the CTA software Recommended Access Order (RAO) implemented for LTO drives to improve retrieve performances

### Features

- CTA software Recommended Access Order (RAO) implemented for LTO drives
- Upgraded EOS to 4.8.24-1
- Upgraded xrootd to 4.12.5-1
- cta-admin repack ls tabular output improvements
- Repack management execution can be disabled via the cta-taped configuration file
- cta/CTA#907 Maintenance process can be disabled via the cta-taped configuration file

# Modifications

- Catalogue refactoring

### Bug fixes

- cta/CTA#901 cta-admin tapefile ls too slow
- cta/CTA#895 [catalogue] RdbmsCatalogue::deleteLogicalLibrary does not delete empty logical library
- utils::trimString() now returns an empty string if the string passed in parameter contains only white-space characters
- Repack request and sub-requests are now unowned from their Agent when completed

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
