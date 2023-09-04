
# v4.NEXT

### Bug Fixes
- cta/CTA#461 - Allow setting the repack VO if there is none with ongoing repacks
- cta/CTA#469 - Fix command 'cta-admin drive ls' not showing REPACK VO on mounts of type ARCHIVE_FOR_REPACK
- cta/CTA#472 - Fix repack VO exceeding the readmaxdrives limit 
- cta/CTA#477 - Fix uncaught exception Helpers::NoTapeAvailableForRetrieve
- cta/CTA#481 - Fix security hotspots related to file permissions and capabilities
- cta/CTA#482 - Fix bugs reported by SonarCloud in tape server, scheduler and objectstore
- cta/CTA#486 - Fix cppcheck new errors
- cta/CTA#473 - Fix some code smells from repack expansion changes.

### Features
- cta/CTA#487 - Bump "filesWrittenToTape: File size mismatch" error to ALERT

### Continuous Integration
- cta/CTA#352 - Test rollback Catalogue Version
- cta/CTA#446 - Upgrade eos to 4.8.105/5.1.29

# v4.10.3-1

### Bug Fixes
- cta/CTA#471 - Fix device statistics from LTO drives

# v4.10.2-1

### Bug Fixes
- cta/CTA#470 - Scheduler crashing when look for a Virtual Organization with empty Tape Pool Name

# v4.10.1-1

### Bug Fixes
- cta/CTA#114 - Return catalogue schema compatibility to single version only
- cta/CTA#367 - Don't use "Putting the drive down" message in cleaner
- cta/CTA#373 - cta-verify-file may fail if cta.verification.mount_policy is not set
- cta/CTA#389 - Minor fix in Tape REST API system tests
- cta/CTA#400 - Fix missing 'sleep' in system tests
- cta/CTA#404 - REPACKING_DISABLED queues not always selected for queueing
- cta/CTA#408 - CTA build fails after fresh cloning of repository
- cta/CTA#409 - Liquibade-update test not compatible with new runners
- cta/CTA#411 - Don't update reason for disabled tape in cleaner
- cta/CTA#423 - Remove duplicated RDBMS test files
- cta/CTA#427 - Update getInstanceAndFid to allow multiple files to be returned from the Catalogue query
- cta/CTA#432 - Do not throw uncaught exceptions in a destructor
- cta/CTA#433 - Removes code which will never be executed
- cta/CTA#434 - CTA frontend accepts files larger than a specified size
- cta/CTA#435 - Fixes exception handling issues reported by SonarCloud
- cta/CTA#436 - Removes XrootC2FSReadFile
- cta/CTA#438 - Fixes code quality issues with CTA mediachanger
- cta/CTA#441 - Use snprintf instead of sprintf
- cta/CTA#443 - Harden string functions in C code

### Building and Packaging
- cta/CTA#385 - Remove OpenSSL dependency
- cta/CTA#428 - Compile EOS using Docker
- cta/CTA#437 - Use version to build EOS using Docker
- cta/CTA#454 - Update XRootD versions on CTA

### Continuous Integration
- cta/CTA#453 - Change from testing to production the image of cta-catalogue-updater

### Features
- cta/CTA#31 - Allow VO override for repack
- cta/CTA#267 - Add new purchase order field to the tape table
- cta/CTA#276 - Add physical library table and a new `cta-admin` cmd for interacting with the table
- cta/CTA#314 - Remove the option of using `cta-admin tapefile ls` with the `-l` option
- cta/CTA#349 - Add option to change the state reason even if the state is not changing
- cta/CTA#386 - Remove buildtree for kubernetes CI
- cta/CTA#392 - Remove gRPC configuration for tool and test in cta-change-storage-class-in-catalogue
- cta/CTA#403 - Add option `physicallibrary` to `cta-admin logicallibrary`
- cta/CTA#407 - Allow repack expanding limit to be configurable
- cta/CTA#418 - Add the physical library to the json output for tape ls and drive ls
- cta/CTA#431 - Improvements to the `cta-admin physicallibrary` command

# v4.10.0-2

## Summary
This release is a repackaging of `v4.10.0-1``, which removes any usage of the CTA catalogue version 13.0.
CTA catalogue migration is intended to be made directly from version 12.0 to 14.0.

### Catalogue Schema
- cta/CTA#442 - Update CTA catalogue schema to version 14.0

# v4.10.0-1

### Features
- cta/CTA#395 - Create gitlab job for code format check

### Catalogue Schema
- cta/CTA#397 - Rework catalogue release procedure and deployment path
- cta/CTA#442 - Update CTA catalogue schema to version 14.0

# v4.9.0-1

## Summary
This release is the same as `v4.8.10-2`, for catalogue v13 release.

# v4.8.10-2

### Bug Fixes
- cta/CTA#402 - Schema verify fails for postgres in catalogue v13

# v4.8.10-1

### Bug Fixes
- cta/CTA#114 - Do not fail CTA taped when using older catalogue schema versions

# v4.8.9-1

### Catalogue Schema
- cta/CTA#387 - Update CTA catalogue schema to version 13.0

# v4.8.8-2

### Bug Fixes
- cta/CTA#379 - Fix tagging release CI script for xrootd5

### Continuous Integration
- cta/CTA#378 - Add http tape REST API compliance tests

# v4.8.8-1

### Features
- cta/CTA#78 - Remove gRPC and ability to change the storage class in EOS from the cta-change-storage-class C++ script
- cta/CTA#139 - Modify the Tape Server code to pass the encryption key ID directly to the python script `cta-get-encryption-key`
- cta/CTA#341 - Normalize usage of encryptionKeyName
- cta/CTA#342 - Compile CTA using devtoolset-11
- cta/CTA#356 - Add support for changing to different storage classes during one execution for cta-change-storage-class-in-catalogue
- cta/CTA#366 - Remove the use of the protobuf field `STORAGE_CLASS_NAME` and replace it with the existing `STORAGE_CLASS`

### Bug Fixes
- cta/CTA#147 - Development of the postgres SchedulerDatabase
- cta/CTA#259 - cta-rmcd should not exit if /dev/sg0 is missing
- cta/CTA#340 - Fix catalogue recompiling
- cta/CTA#345 - Remove some objectstore dependencies outside scheduler
- cta/CTA#357 - Fix failing OsmFileReader due to uint underflow
- cta/CTA#371 - Protect EnstoreFileReader from uint underflow

### Continuous Integration
- cta/CTA#216 - Enable HTTP support for EOS4 on CTA CI
- cta/CTA#215 - Enable EOSTOKEN support on CTA CI
- cta/CTA#262 - Manage CI repositories with `cta-release` code
- cta/CTA#297 - Add gfal2 tests to CI
- cta/CTA#297 - Refactor client pod tests and switched test tracking status to sqlite db
- cta/CTA#332 - Add systemtests for cta-admin commands

# v4.8.7-1

### Features
- cta/CTA#93  - Refactor Frontend code to allow code sharing between SSI and gRPC implementations
- cta/CTA#213 - Improve error messages for `cta-eos-namespace-inject`
- cta/CTA#213 - Skip valid paths that exists with valid metadata
- cta/CTA#213 - Skip files with paths that have valid metadata
- cta/CTA#245 - Implements cta-admin functions in cta-frontend-grpc
- cta/CTA#294 - Improve error messages for 'Exec' in the gRPC client
- cta/CTA#302 - Make RMC maxRqstAttempts variable configurable
- cta/CTA#308 - Remove catalogue autogenerated files
- cta/CTA#318 - Limit the number of frontend-grpc request processing threads
- cta/CTA#321 - Add arguments to cta-change-storage-class to validate that the correct files are being changed

### Bug Fixes
- cta/CTA#240 - Use correct fSeq after disk read error
- cta/CTA#305 - AllCatalogueSchema file not created when updating schema
- cta/CTA#309 - Ignore 'NoSuchObject' exceptions thrown by non-existing objects during retrieve job requeuing
- cta/CTA#310 - Trigger cleanup session if taped child process did not exit with success code
- cta/CTA#313 - Average bandwidth not being calculated correctly inside 'fetchMountInfo'
- cta/CTA#320 - CTA No Oracle is failing
- cta/CTA#328 - Make root user able to read encrypted files with cta-readtp
- cta/CTA#329 - Correctly set previous session state to enable cleaner session
- cta/CTA#330 - Fix no tape block movement regression

# v4.8.6-1

### Bug Fixes
- cta/CTA#322 - Queues with cleanup heartbeat above zero are not being picked for cleanup
- cta/CTA#325 - Fix logging for "no tape block movement" message

# v4.8.5-1

## Summary
### Features
- cta/CTA#166 - Refactor Catalogue and RdbmsCatalogue classes
- cta/CTA#211 - Fix issues with cta-readtp encryption
- cta/CTA#213 - Add tool for injecting file into eos
- cta/CTA#213 - Improve test for eos injection tool
- cta/CTA#218 - Do not retry during repack requests
- cta/CTA#219 - Update Ceph repo public key in dev env and buildtree
- cta/CTA#222 - Review what gets logged in CTA
- cta/CTA#224 - Improve error message for cta-verify-file whn VID does not exist
- cta/CTA#230 - Modify CTA code to enforce VID uppercase
- cta/CTA#239 - Add improvments to the cta-change-storage-class tool
- cta/CTA#241 - Add missing forward declarations to standalone cli tools
- cta/CTA#248 - Clean up output from cta-readtp
- cta/CTA#250 - Trim SQL query length in catalogue DB failure reason
- cta/CTA#252 - Update cta-change-storage-class to accept json file as input
- cta/CTA#254 - Create git submodule for CTA catalogue schema
- cta/CTA#284 - Add kerberos authentication for standalone cli tool tests
- cta/CTA#295 - Update standalone cli tool tests, remove sudo and use more generated tmp folders
- cta/CTA#301 - Switch from new to unique pointer in CtaReadTp to fix memory leak
### Bug Fixes
- cta/CTA#174 - Improving error description in cta-tape-label in case of wrong volume label format
- cta/CTA#181 - cta-statistics-update can fail for catalogues in postgres
- cta/CTA#189 - Avoid postgres logging frequent warnings about no transaction in progress
- cta/CTA#234 - Replace stoi with toUint64 in standalone cli tool
- cta/CTA#237 - Fix misplaced ERROR/WARNING messages triggered by the queue cleanup runner
- cta/CTA#238 - Compilation fails when using cta::common::Configuration::getConfEntInt(...)
- cta/CTA#242 - cta-frontend-grpc - problem with loading pem_root_certs
- cta/CTA#273 - Fix tape state change command idempotency when resetting REPACKING/BROKEN/PENDING
- cta/CTA#280 - Add a timeout to tape server global lock on the object store
- cta/CTA#288 - Do not allow tape server to transition from REPACKING_DISABLED to DISABLED
- cta/CTA#289 - Avoid a DB query and improve filtering time in sortAndGetTapesForMountInfo
- cta/CTA#290 - Remove temporary counters used to track single-vid-GetTapesByVid calls
- cta/CTA#292 - Problem with cppcheck
### Continuous Integration
- cta/CTA#253 - Allow Failure for cta_valgrind tests
- cta/CTA#255 - Updating CI to run with old and latest kubernetes versions
- cta/CTA#286 - Upgrade eos to 4.8.98/5.1.9 fixing operation critical eosreport see EOS-5367
- cta/CTA#298 - Remove docs CI stage, moved to submodule

# v4.8.4-1

## Summary
This release reduces the number of DB queries issued to the CTA catalogue.
### Bug Fixes
- cta/CTA#275 - Avoid DB queries via getTapesByVid in OStoreDB::fetchMountInfo
- cta/CTA#274 - Remove unnecessary catalogue DB queries from QueueCleanupRunner

# v4.8.3-1

## Summary
This release is the same as '4.8.2-1', except for the addition of some DB query counters which will be used for profiling purposes. \
The counters are expected to be removed in a future release, when the analysis is no longer necessary.
### Bug Fixes
- cta/CTA#155 - Tape server querying DB at very high rate (log query-count patch)

# v4.8.2-1

## Summary
### Features
- cta/CTA#155 - Tape server querying DB at very high rate

# v4.8.1-1

## Summary
### Bug fixes
- cta/CTA#243 - Modify new queue cleanup protobuf fields from 'required' to 'optional'

# v4.8.0-1

## Summary
This CTA release contains significant changes related to repacking, including the addition of new final and temporary states.
It may make be incompatible with pre-existing operational tools that relied on the old repacking behaviour.
### Features
- cta/CTA#83 - Setup new tape state REPACKING
- cta/CTA#226 - Setup new tape state EXPORTED
- cta/CTA#77 - Add maintenance runner for cleaning-up (retrieve) queue requests and managing new internal states
- cta/CTA#211 - Add functionality for reading encrypted tapes with cta-readtp
- cta/CTA#214 - Update manual page for cta-admin to include info about user defined config files.
### Bug fixes
- cta/CTA#93 - Refactor Frontend code to allow code sharing between SSI and gRPC implementations
- cta/CTA#221 - Change option in cta-send-event from instance to eos.instance
- cta/CTA#223 - Remove vid check to improve run time of cta-verify-file, fix possible _S_construct null not valid error
- cta/CTA#13 - Fix `cta-catalogue-schema-verify` checking of NOT NULL constraints in Postgres

# v4.7.14-1

## Summary
### Features
- cta/CTA#133 - Change severity from ERROR to INFO for "reported a batch of retrieve jobs" log message in Scheduler.cpp
- cta/CTA#201 - Improve error message when oracle configured without oracle support
- cta/CTA#203 - Refactor cta-restore-deletes-files by using the connection configuration class in standalone_cli_tools/common
### Bug fixes
- cta/CTA#209 - handle if $HOME is not defined when choosing config file for cta-admin

# v4.7.13-3

## Summary
### Building and Packaging
- cta/CTA#15 - Repackaging CTA for easy installation of public RPMs

# v4.7.13-2

## Summary
### Building and Packaging
- cta/CTA#207 - Fix tag pipeline

# v4.7.13-1

## Summary
### Features
- cta/CTA#16 - Add option for a user config file
- cta/CTA#23 - Change Owner Identifier String and System Code of Creating System values in tape labels
- cta/CTA#41 - Delete verification_status of tape when tape is reclaimed
- cta/CTA#78 - Tool to update the storage class
- cta/CTA#80 - Revise tape thread complete success/failure logic
- cta/CTA#94 - Remove tape session error codes
- cta/CTA#106 - Investigate cta-taped Watchdog timer configuration
- cta/CTA#153 - Allow verification status to be cleared with cta-admin
- cta/CTA#156 - Change handeling of multilple fxids as input to the cta-restore-files tool
- cta/CTA#166 - Removing Catalogue headers from hpp files and small refactoring of Catalogue classes.
- cta/CTA#173 - Update release notes and small changes to refactoring of operation tools cmd line parsing - Compatible with operations 0.4-95 or later
- cta/CTA#180 - Allow to submit multiple files for verification
- cta/CTA#198 - Add vid existence check and update usage message for cta-verify-file
- cta/CTA#199 - Add static library ctaCmdlineToolsCommon
### Continuous Integration
- cta/CTA#118 - Add unit tests for OSM label
- cta/CTA#191 - Block merge until cta_valgrind success
### Bug fixes
- cta/CTA#48 - Catch tape server exception and log an error instead
- cta/CTA#80 - Fix tape thread complete success/failure message parameter
- cta/CTA#123 - Change some tape server errors into warnings
- cta/CTA#150 - Fix job fetching logic in RecallTaskInjector
- cta/CTA#161 - Fix bug when using temporary tables with PostgreSQL
- cta/CTA#175 - Consistency issue in volume names format
- cta/CTA#182 - Fix cta_valgrind error
- cta/CTA#197 - Include order in XrdSsiCtaRequestMessage.cpp

# v4.7.12-2

## Summary
### Building and Packaging
- cta/CTA#15 - Repackaging CTA for easy installation of public RPMs

# v4.7.12-1

## Summary
### Features
- cta/CTA#144 - frontend-grpc: use provided archiveReportURL AS-IS
- cta/CTA#146 - Refactoring of operation tools cmd line parsing
### Continuous Integration
- cta/CTA#157 - Upgrade eos5 to 5.1.1-1
- cta/CTA#158 - Install 'libisa-l_crypto' in 'doublebuildtree-stage2b-scripts/Dockerfile'
- cta/CTA#15 - Configuring CI for public binary RPM distribution
### Bug fixes
- cta/CTA#122 - Problem with handling of the non-native formats by the CleanerSession
- cta/CTA#160 - Improve DB access to get all the Tape Drive States
- cta/CTA#164 - Cppcheck job is not failing when there are errors
- cta/CTA#165 - Fix oracle dbunittests
- cta/CTA#169 - Fix misconfigured rules in .gitlab-ci.yml files
- cta/CTA#171 - CI runner randomly crashing in cta-tape-label
### Building and Packaging
- cta/CTA#107 - Check latest version of gtest suite

# v4.7.11-1

## Summary
- Note: When using Spectra Logic libraries, this release requires Spectra Logic firmware version >= BlueScale12.8.08.01
### Features
- cta/CTA#89 - Create stubs for Enstore tape label format
- cta/CTA#126 - Remove cta-admin schedulinginfo subcommand
### Continuous Integration
- cta/CTA#7  - Use same versionlock.list file for xrootd4 and 5
- cta/CTA#18 - CI - Testing of DB schema upgrade script
- cta/CTA#49 - Clean up orchestration test scripts
- cta/CTA#101 - CI should cancel running jobs for a branch after a force pushi
- cta/CTA#130 - Do not fail liquibase-update test when CTA is updated without oracle support
- cta/CTA#112 - Add postgres 10.0 to 11.0 liquibase Schema migration script
- cta/CTA#132 - Support postgres migrations
### Building and Packaging
- cta/CTA#92 - Refactor CTA code so that it can be build without Oracle dependencies
- cta/CTA#63 - CTA Frontend protobuf changes to support dCache
### Bug fixes
- cta/CTA#30 - Check size of comments before commit them to the Catalogue
- cta/CTA#142 - Save logs in liquibase-update test
- cta/CTA#127 - DataTransferSession keeps busy while waiting for a mount to be required
- cta/CTA#130 - Liquibase-update test fails when CTA is update without oracle support
- cta/CTA#127 - DataTransferSession keeps busy while waiting for a mount to be required
- cta/CTA#125 - Problem with reading OSM tapes using position by block id

# v4.7.10-1

## Summary
- This is a schema upgrade release for catalogue version 12.0.
- For details on how to do the upgrade check: https://eoscta.docs.cern.ch/catalogue/upgrading_the_schema/
### Catalogue Schema
- cta/CTA#135 - Added EXPORTED and EXPORTED_PENDING states to catalogue
- cta/CTA#131 - Fix indexes on columns in CTA Catalogue which use LOWER()
- cta/CTA#140 - Added REPACKING_DISABLED state to catalogue
- cta/CTA#137 - Added a new column ENCRYPTION_KEY_NAME in the table TAPE_POOL

# v4.7.9-2

## Summary

This release fixes some packaging issues from v4.7.9-1: when upgrading from v4.7.8-1 to v4.7.9-1 some configuration
files were renamed to *configurationfile.rpmsave* and thefore some services and cta-cli lost their configuration.

See all details and fix in cta/CTA#119

### Bug fixes
- cta/CTA#119 [new repo] - Packaging issue with CTA 4.7.8-1/4.7.9-1

# v4.7.9-1

## Summary

***Important NOTE:***
- This is the first release using the new repo: https://gitlab.cern.ch/cta/CTA
- Some issues were already closed, so they can only be found on the old repo: https://gitlab.cern.ch/cta/CTA-old

To make the distinction clear, `[old repo]` and `[new repo]` will be used here to distinguish between
issues on both repositories.

## Changes to options and default values for cta-taped

This release makes some minor changes to options and default values for cta-taped. All options are
detailed in the cta-taped man page and the provided example files. The changes are outlined below:

Options which have been renamed:

- "general FileCatalogConfigFile" was renamed to "taped CatalogueConfigFile"
to be consistent with other options.
- "DisableRepackManagement" and "DisableMaintenanceProcess" were
renamed to "UseRepackManagement" and "UseMaintenanceProcess" with default changed from "no" to "yes"
(default semantics are not changed).
- "FetchEosFreeSpaceScript" renamed to "externalFreeDiskSpaceScript".

Options for which default values have changed:

- LogMask, MountCriteria, BufferCount, TapeLoadTimeout,
UseRAO, RAOLTOAlgorithm, RAOLTOAlgorithmOptions.

The following manual pages have been updated:

  - cta-admin(1cta)
  - cta-catalogue-admin-user-create(1cta)
  - cta-catalogue-schema-create(1cta)
  - cta-catalogue-schema-drop(1cta)
  - cta-catalogue-schema-set-production(1cta)
  - cta-catalogue-schema-verify(1cta)
  - cta-database-poll(1cta)
  - cta-fst-gcd(1cta)
  - cta-readtp(1cta)
  - cta-restore-deleted-files(1cta)
  - cta-rmcd(1cta)
  - cta-smc(1cta)
  - cta-taped(1cta)
  - cta-tape-label(1cta)

### Features
- cta/CTA-old#979  [old repo] - Document configuration options of daemons and command-line tools
- cta/CTA-old#1252 [old repo] - Adds Liquibase changelog files for PostgreSQL
- cta/CTA#3        [new repo] - Implement listing of DISABLED libraries
- cta/CTA#64       [new repo] - Adds support for the OSM Tape Label format to the CTA
- cta/CTA-old#1278 [old repo] - Add support for reading multiple tape formats by the ReadtpCmd command
- cta/CTA-old#1278 [old repo] - Support multiple tape formats in ReadtpCmd command
### Bug fixes
- cta/CTA-old#947  [old repo] - cta-taped should log the FST being used for a data transfer
- cta/CTA-old#1093 [old repo] - The `cta-taped` manpage showed outdated config options
- cta/CTA-old#1106 [old repo] - "cta-admin schedulinginfo ls" returns records larger than SSI buffer size limit
- cta/CTA-old#1269 [old repo] - cta-restore-deleted-files injects wrong diskFileId in CTA Catalogue
- cta/CTA-old#1276 [old repo] - Some configuration files should be renamed from `.conf` to `.conf.example`
- cta/CTA#28       [new repo] - Do NOT allow capacity change of a cartridge if there a still files registered on that tape
- cta/CTA#29       [new repo] - cta-taped should log the FST being used for a data transfer
- cta/CTA#108      [new repo] - Fix broken CTA containerised installation
- cta/CTA#109      [new repo] - Move logic to minimize mounts for multi-copy tape pool recalls out of scheduler logic

### Continuous Integration
- cta/CTA#88       [new repo] - xrootd 5 pipelines failing in CI
- cta/CTA-old#1187 [old repo] - Run EOS5 CI every night
- cta/CTA-old#1280 [old repo] - Refactor .gitlab-ci.yml using templates
### Building and Packaging
- cta/CTA-old#1229 [old repo] - Introduce build of a new taped, using new type of SchedulerDatabase
- cta/CTA#111      [new repo] - Remove daemon/LabelSession
- cta/CTA-old#1143 [old repo] - HTTP REST API transition to CTA
- cta/CTA-old#1224 [old repo] - Remove CASTOR to CTA migration tools RPM

# v4.7.8-1

## Summary
- This is a catalogue schema upgrade release
- For details on how to do the upgrade check: https://eoscta.docs.cern.ch/catalogue/upgrading_the_schema/
### Catalogue Schema
- cta/CTA#1213 - New internal states REPACKING_PENDING & BROKEN_PENDING
- cta/CTA#1226 - Remove deprecated catalogue columns

# v4.7.7-1

## Summary
### Features
- cta/CTA#1239 - Add support to CTA for multiple tape label formats
- cta/CTA#1257 - Refactor CTA/tapeserver/castor/tape/tapeserver/file/File.cpp
- cta/CTA#1263 - Abstract ReadSession and FileReader
- cta/CTA#1265 - Create base of the dCache OSM label format
### Bug fixes
- cta/CTA#1267 - Fix scheduling bug introduced in 4.7.6
### Continuous Integration
- cta/CTA#1266 - Fix eos5 currently failing CI schedules
- cta/CTA#1262 - Fix unnecessary log dump in unit-tests
### Other
- cta/CTA#1251 - Remove dcache leftovers on the grpc-based frontend

# v4.7.6-1

## Summary
### Features
- cta/CTA#1238 - All drive down reasons set by cta-taped should start with [cta-taped]
- cta/CTA#1254 - Remove tape label option from cta-admin
- cta/CTA#1253 - add disk instance to cta-admin vo ls output and fix handling of case sensitive names in cta-admin vo add
- cta/CTA#1159 - All drive statuses must be set only in data transfer session and read/write threads
### Bug fixes
- cta/CTA#1255 - Resolve "VID is missing in DriveHandler when session is killed"
- cta/CTA#1256 - Catch and fix EOS regressions in tape specific xrootd API introduced in eos 4.8.79-1 with eos 4.8.87-1
- cta/CTA#1247 - Fix improper initialization of the variable m_lastFseq of type uint64_t with -1 value in the constructor of castor::tape::tapeserver::daemon::TapeWriteSingleThread
### Building and Packaging
- cta/CTA#1224 - Remove CASTOR to CTA migration tools RPM

# v4.7.5-1

## Summary

This release introduced an additional gRPC based frontend for storage backends. This is still a work in progress and is not ready for use.

The command `cta-verify-file` now requires the options `eos.instance`, `eos.request.user` and `eos.request.group` to be configured in `/etc/cta/cta-cli.conf`.

### Features
- cta/CTA#1222 - Add minimal gRPC based frOntend for integration with dCache
- cta/CTA#1241 - make cta-verify-file get instance, request user and group otions from ctacli config file

### Bug fixes
- cta/CTA#1225 - Fix bug causing tapeserver to sometimes pop the entire archive queue at the end of the mount

### Building and Packaging
- cta/CTA#1224 - Removes CASTOR to CTA migration tools RPM and references to CASTOR repo

# v4.7.4-1

## Summary
### Features
- cta/CTA#1205 - Fail pipeline if cppcheck detects errors
- cta/CTA#1206 - Change NULL for nullptr
- cta/CTA#1217 - Schema verification should just issue an warning if there are extra indexes in the db that are not in the catalogue
- cta/CTA#1220 - Improve queued retrieve logging message
- cta/CTA#1152 - Reduce eos free space query load
- cta/CTA#1231 - Add --loadtimeout option to cta-tape-label and increase default value
- cta/CTA#977 - Add --drive option to cta-tape-label
- cta/CTA#1159 - Move `Starting` drive status from OStoreDB to DataTransferSession

### Bug fixes
- cta/CTA#1120 - Fix negative disk space reservation content
- cta/CTA#1235 - cta-verify-file should return 1 on error

# v4.7.3-1

## Summary
### Features
- cta/CTA#1161 - Tape server refactoring, "Decide where m_reportPacker.setTapeDone() should be called"
- cta/CTA#1195 - `cta-catalogue-schema-drop` should drop the CTA_CATALOGUE table last
### Bug fixes
- cta/CTA#950 - Eliminate race condition preventing the drive to go down on failure
- cta/CTA#1160 - Fix DrainingToDisk stale status in case if there is DiskWriteThreadPool thread left
- cta/operations#708 - Fix "Should run cleaner but VID is missing. Putting the drive down"
- cta/CTA#1197 - Return code from `cta-catalogue-schema-verify` should indicate if the schema is in UPGRADING state

# v4.7.2-1

## Summary
- Deprecated: Replaced by release v4.7.3-1

# v4.7.1-1

## Summary
### Features
- cta/CTA#1179 - Use std::optional instead of cta::optional
- cta/CTA#1190 - Use std::make_unique instead of cta::make_unique
- cta/CTA#1198 - Use hardcoded mount policy for verification requests
- Add verification flag to queued retrieve request log message
- propagate labelFormat from TAPE catalog to VolumeInfo
- cta/CTA#1200 - Remove range class

# v4.7.0-1

## Summary

### Upgrade Instructions

This CTA release requires a non-backwards compatible database schema upgrade to CTA catalogue schema v10.0.
Please consult the [database upgrade documentation](https://eoscta.docs.cern.ch/catalogue/upgrade/).

### Features
- cta/CTA#1163 - cta-admin now prefixes the drivename with a '!' if the respective logical library is disabled
- cta/CTA#1168 - Add configuration option for scheduler stack size to /etc/cta/cta-frontend-xrootd.conf
- cta/CTA#1151 - Update cta-admin ds add for catalogue schema version 10

### Bug fixes
- cta/CTA#1156 - failed to instantiate the RAO algorithm

### Building and Packaging
- cta/CTA#823  - Ensure unit tests CANNOT be executed against a production database
- cta/CTA#1082 - Review software license text in CTA
- cta/CTA#1153 - Change compiler FLAGS in cmake files
- cta/CTA#1155 - Remove GCC suppresions in DriveGeneric and file/Structures
- cta/CTA#1173 - Remove GCC '-Wno-unused-function' suppresion
- cta/CTA#1166 - Add SCL-RH Repo to CTA CI boot scripts
- cta/CTA#1185 - Make expectedLabelStandard default argument (for Enstore reads)

### Catalogue Schema
- cta/CTA#1043 - Add verification column to tape table
- cta/CTA#1158 - Check all foreign key references have a full index on both sides of the constraint
- cta/CTA#1171 - Add LABEL_FORMAT column to TAPE table
- cta/CTA#1172 - Add disabled reason column to logical library table
- cta/CTA#1177 - Fix CTA catalogue schema verify index checker for PostgreSQL
- cta/CTA#1147 - Add foreign key constraint between the disk instance name of a vo and the disk instance table
- cta/CTA#1151 - Drop ACTIVITY_WEIGHTS and TAPE_DRIVE tables

# v4.6.1-1

## Summary

### Upgrade Instructions

This CTA release requires a database schema upgrade to CTA catalogue schema v4.6.
Please consult the [database upgrade documentation](https://tapeoperations.docs.cern.ch/ctaops/upgrade_production_database).

### Features
- cta/CTA#1137 - Stop deletion of failed retrieve/archive requests
- cta/CTA#1150 - Add option to pass desired catalogue version into cta-catalogue-schema-create
- cta/CTA#1147 - Add Disk Instance Column to VO table
- cta/CTA#1119 - Remove support of manual mode for loading tapes
- cta/CTA#1123 - Add mount id to disk space reservations, prevent tape servers from releasing disk space from a previous mount

### Bug fixes
- cta/CTA#1138 - sortAndGetTapesForMountInfo only queries tapes in the current logical library
- cta/CTA#1117 - Update masterDataInBytes when writing files to tape
- cta/CTA#1125 - cta-admin dr ls should show '-' instead of "NO_MOUNT" for Mount Type

### Continuous Integration
- cta/CTA#1131 - Compile CTA using devtoolset-8 in CI
- cta/CTA#1126 - Create tests for TapeDrivesCatalogueState

# v4.6.0-1

## Summary

Updates EOS version in CI to 4.8.75.

### Upgrade Instructions

This CTA release requires a database schema upgrade to CTA catalogue schema v4.5.
Please consult the [database upgrade documentation](https://tapeoperations.docs.cern.ch/ctaops/upgrade_production_database).

### Features
- cta/CTA#999  - Add a default mount rule for recalls
- cta/CTA#1109 - Add --dirtybit option to cta-admin ta ch and show dirty bit value in cta-admin --json ta ls
- cta/CTA#1107 - add mountpolicyname to request schema in objectstore
- cta/CTA#1111 - Add disk instance and disk instance tables to catalogue and respective cta-admin diskinstance/diskinstancespace add/ls/ch/rm commands
- cta/CTA#1114 - Log SSI events in ctafrontend
- cta/CTA#1108 - Make cta-admin --json sq show mount policy with highest priority and mount policy with lowest request age for each queue.

### Bug fixes
- cta/CTA#1102 - Make requeued jobs retain their original creation time
- cta/CTA#1091 - Move TAPE_DRIVE table to DRIVE_STATE and refactor code to update drive states
- cta/CTA#1110 - Moves disk space reservations to the DRIVE_STATE table, uses atomic updates
- Don't throw an exception if DiskSpaceReservationRequest brings reservation < 0, just reset and log the error

### Continuous Integration
- cta/CTA#734  - Adds test for FST delete-on-close behaviour
- Adds system tests for 'prepare' and 'query prepare' (idempotent prepare)

# v4.5.1-2

## Summary

No code changes. Updates EOS version in CI to 4.8.74.

# v4.5.1-1

## Summary

### Bug fixes
- cta/CTA#1101 - Fix disk space reservation logic adding all existing disk space reservations for all disk systems
- cta/CTA#1023 - Retrieve puts the queue to sleep if the eos disk instance is not reachable

# v4.5.0-1

## Summary

### Features
- Improve cta-versionlock script
- cta/CTA#1091 - New Drive State table in CTA Catalogue
- cta/CTA#1054 - Fix filing of disk buffer when recalling from tapeservers with RAO
- cta/CTA#1076 - Retrieve fails if disk system configuration is removed
- cta/CTA#1087 - Add new tapeserver config option UseEncryption
- cta/CTA#1096 - Better handling of bad checksums in archive/retrieve sessions

### Bug fixes
- cta/CTA#1092 - Fix overflow error with drive state latestBandwith causing cta frontend crash
- cta/CTA#501 - Fix disappearing reason when TapeDrive is reading or writing

# v4.4.1-1

### Bug fixes
- cta/CTA#1092 Fix overflow with drive bandwith causing frontend to crash

# v4.4.0-1

### Upgrade Instructions

This CTA release requires a database schema upgrade to CTA catalogue schema v4.3. Please consult the [database upgrade documentation](https://tapeoperations.docs.cern.ch/ctaops/upgrade_production_database).

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

- Shrinks `cta-admin repack ls` tabular output
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

### Upgrade Instructions from v1.0-3

#### 1. Upgrade the Catalogue schema version from version 1.0 to 1.1

Before updating CTA, the Catalogue schema should be upgraded. Here is the link to the documentation about the database schema updating procedure : [https://eoscta.docs.cern.ch/catalogue/upgrade/](https://eoscta.docs.cern.ch/catalogue/upgrade/)

The liquibase changeLog file is already done so you can directly run the [*liquibase update*](https://eoscta.docs.cern.ch/catalogue/upgrade/backward_compatible_upgrades/#3-run-the-liquibase-updatesql-command) command with the changeLogFile located in the directory *CTA/catalogue/migrations/liquibase/oracle/1.0to1.1.sql*.

#### 2. Update CTA components

TODO : Instructions about how to update the tapeservers and the frontend.
