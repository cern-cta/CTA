---
date: 2024-07-18
section: 1cta
title: CTA-TAPED
header: The CERN Tape Archive (CTA)
---
<!---
@project      The CERN Tape Archive (CTA)
@copyright    Copyright © 2020-2024 CERN
@license      This program is free software, distributed under the terms of the GNU General Public
              Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
              redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
              option) any later version.

              This program is distributed in the hope that it will be useful, but WITHOUT ANY
              WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
              PARTICULAR PURPOSE. See the GNU General Public License for more details.

              In applying this licence, CERN does not waive the privileges and immunities
              granted to it by virtue of its status as an Intergovernmental Organization or
              submit itself to any jurisdiction.
--->

# NAME

cta-taped --- CTA Tape Server daemon

# SYNOPSIS

**cta-taped** \[\--config *config_file*] \[\--foreground \[\--stdout]] \[\--log-to-file *log_file*] \[\--log-format *format*]\
**cta-taped** \--help

# DESCRIPTION

**cta-taped** is the daemon responsible for controlling one or more tape drives.

When **cta-taped** is executed, it immediately forks, with the parent terminating and the child
running in the background. The **\--foreground** option can be used to keep the parent process
running in the foreground.

## Tape Library Support

CTA supports SCSI-compatible tape libraries. The **cta-taped** daemon requires the tape library
daemon (**cta-rmcd**) to be installed and running on the same server as itself.

# OPTIONS

-c, \--config *config_file*

:   Read **cta-taped** configuration from *config_file* instead of the default,
    */etc/cta/cta-taped.conf*.

-f, \--foreground

:   Do not terminate the **cta-taped** parent process, keep it in the foreground.

-h, \--help

:   Display command options and exit.

-l, \--log-to-file *log_file*

:   Log to a file instead of using syslog.

-o, \--log-format *format*

:   Output format for log messages. \[default\|json\]

-s, \--stdout

:   Log to standard output instead of using syslog. Requires \--foreground.

# CONFIGURATION

The **cta-taped** daemon reads its configuration parameters from the CTA
configuration file (by default, */etc/cta/cta-taped.conf*). Each option
is listed here with its *default* value.

## Tape Server Configuration Options

**taped DaemonUserName *cta***

:   The user of the **cta-taped** daemon process.

**taped DaemonGroupName *tape***

:   The group of the **cta-taped** daemon process.

**taped LogMask *INFO***

:   Logs with a level lower than this value will be masked. Possible
    values are EMERG, ALERT, CRIT, ERR, WARNING, NOTICE (USERERR), INFO,
    DEBUG. USERERR log level is equivalent to NOTICE, because by
    convention, CTA uses log level NOTICE for user errors.

**taped LogFormat *default***

:   The default format for log lines is plain-text key-value pairs. If
    this option is set to *json*, log lines will be output in JSON format.

**taped CatalogueConfigFile */etc/cta/cta-catalogue.conf***

:   Path to the CTA Catalogue configuration file. See **FILES**, below.

**ObjectStore BackendPath** (no default)

:   URL of the objectstore (CTA Scheduler Database). Usually this will
    be the URL of a Ceph RADOS objectstore. For testing or small
    installations, a file-based objectstore can be used instead. See
    **cta-objectstore-initialize**.

## Drive Configuration Options

**taped DriveName** (no default)

:   The name of the drive. Will be included for every line in the logs.

**taped DriveLogicalLibrary** (no default)

:   CTA\'s logical library the tape drive will be linked to.

**taped DriveDevice** (no default)

:   Path to the character special device used to access the drive.

**taped DriveControlPath** (no default)

:   The SCSI media changer address of the drive. This is \"smc\" + the
    drive ordinal number of the device, which can be obtained with
    **cta-smc -q D**.

## General Configuration Options

This options will be included in every log line of the tape daemon to
enhance log identification when swapping drives between different backends.

**general InstanceName** (no default)

:   Unique string to identify CTA\'s catalogue instance the tape daemon is serving.

**general SchedulerBackendName** (no default)

:   The unique string to identify the backend scheduler resources. It
    can be structured as: \[ceph\|postgres\|vfs]\[User\|Repack].

## Memory management options

**taped BufferCount *5000***

:   Number of memory buffers per drive in the data transfer cache.

**taped BufferSizeBytes *5000000***

:   Size of a memory buffer in the data transfer cache, in bytes.

## Batched metadata access and tape write flush options

**taped ArchiveFetchBytesFiles *80000000000*,*4000***

:   Maximum batch size for processing archive requests, specified as a
    tuple (number of bytes, number of files). When **cta-taped** fetches
    a batch of archive requests, the batch cannot exceed the number of
    bytes and number of files specified by this parameter. Defaults to
    80 GB and 4000 files.

**taped ArchiveFlushBytesFiles *32000000000*,*200***

:   Flush to tape criteria, specified as a tuple (number of bytes,
    number of files). During archiving operations, this parameter
    defines the maximum number of bytes and number of files that will be
    written to tape before a flush to tape (synchronised tape mark).
    Defaults to 32 GB and 200 files.

**taped RetrieveFetchBytesFiles *80000000000*,*4000***

:   Maximum batch size for processing retrieve requests, specified as a
    tuple (number of bytes, number of files). When cta-taped fetches a
    batch of retrieve requests, the batch cannot exceed the number of
    bytes and number of files specified by this parameter. Defaults to
    80 GB and 4000 files.

## Scheduling options

**taped MountCriteria *500000000000*,*10000***

:   Criteria to mount a tape, specified as a tuple (number of bytes,
    number of files). An archival or retrieval queue must contain at
    least this number of bytes or this number of files before a tape
    mount will be triggered. This does not apply when the timeout
    specified in the applicable mount rule is exceeded. Defaults to 500
    GB and 10000 files.

## Disk file access options

**taped NbDiskThreads *10***

:   The number of disk I/O threads. This determines the maximum number
    of parallel file transfers.

## Tape encryption support

**taped UseEncryption *yes***

:   Enable tape hardware encryption. Encryption will be enabled only for
    tapes where a valid encryption key has been configured for the tape
    or tape pool.

**taped externalEncryptionKeyScript** (no default)

:   Path to the external script used to obtain encryption keys.

## Disk space management options

**taped externalFreeDiskSpaceScript** (no default)

:   Path to the external script used to determine free disk space in the
    retrieve buffer.

## Recommended Access Order (RAO) options

**taped UseRAO *yes***

:   Enable Recommended Access Order (RAO) if available. This setting is
    used to enable both hardware and software RAO for all drives that
    support it. Hardware RAO in IBM Enterprise and LTO drives generation
    9 or later needs no further configuration. The additional RAO
    options below are for software RAO on LTO-8 drives.

**taped RAOLTOAlgorithm *sltf***

:   On LTO-8 tape drives, specify which software RAO algorithm to use.
    Valid options are **linear**, **random**, **sltf**. **linear** means
    retrieve files ordered by logical file ID. **random** means retrieve
    files in a random order. **sltf** is the Shortest Locate Time First
    algorithm, which traverses the tape by always picking the nearest
    (lowest cost) neighbour to the last file selected. The cost function
    is specified in **RAOLTOAlgorithmOptions**, below.

    Linear and random ordering are useful only to establish a baseline
    for RAO tests. This option should be set to **sltf** in production
    environments.

> Note: the **sltf** option requires that the following parameters have
> been specified in the CTA Catalogue for the LTO-8 media type:
> *nbwraps*, *minlpos*, *maxlpos*. See **cta-admin mediatype**.

**taped RAOLTOAlgorithmOptions *cost_heuristic_name:cta***

:   Options for the software RAO algorithm specified by
    **RAOLTOAlgorithm**, above.

## Maintenance process configuration options

**taped UseRepackManagement *yes***

:   Enable RepackRequestManager for repack-related operations.

**taped UseMaintenanceProcess *yes***

:   Enable MaintenanceProcess for repack-related operations, garbage
    collection and disk reporting.

## Timeout options

**taped TapeLoadTimeout *300***

:   Maximum time to wait for a tape to load, in seconds.

**taped WatchdogCheckMaxSecs *120***

:   Maximum time allowed to determine a drive is ready, in seconds.

**taped WatchdogScheduleMaxSecs *300***

:   Maximum time allowed to schedule a single mount, in seconds.

**taped WatchdogMountMaxSecs *600***

:   Maximum time allowed to mount a tape, in seconds.

**taped WatchdogUnmountMaxSecs *600***

:   Maximum time allowed to unmount a tape, in seconds.

**taped WatchdogDrainMaxSecs *1800***

:   Maximum time allowed to drain a file to disk during retrieve, in seconds.

**taped WatchdogShutdownMaxSecs *900***

:   Maximum time allowed to shutdown of a tape session, in seconds.

**taped WatchdogNoBlockMoveMaxSecs *1800***

:   Maximum time allowed after mounting without any tape blocks being
    read/written, in seconds. If this timeout is exceeded, the session
    will be terminated.

**taped WatchdogIdleSessionTimer *10***

:   Maximum time to wait after scheduling came up idle, in seconds.

# ENVIRONMENT

XrdSecPROTOCOL

:   The XRootD security protocol to use for client/server authentication.

XrdSecSSSKT

:   Path to the XRootD Simple Shared Secrets (SSS) keytab to use for
    client/server authentication.

# FILES

*/etc/cta/cta-taped.conf*

:   The CTA Tape Server configuration file, containing the options
    described above under **CONFIGURATION**. See */etc/cta/cta-taped.conf.example*.

*/etc/cta/cta-catalogue.conf*

:   Usual location for the CTA Catalogue configuration file. See **taped CatalogueConfigFile**
    option under **CONFIGURATION**, and */etc/cta/cta-catalogue.conf.example*.

*/var/log/cta/cta-taped.log*

:   Usual location for the tape server log file.

# SEE ALSO

**cta-rmcd**(1cta)

CERN Tape Archive documentation [https://eoscta.docs.cern.ch/](https://eoscta.docs.cern.ch/)

# COPYRIGHT

Copyright © 2024 CERN. License GPLv3+: GNU GPL version 3 or later [http://gnu.org/licenses/gpl.html](http://gnu.org/licenses/gpl.html).
This is free software: you are free to change and redistribute it. There is NO WARRANTY, to the extent permitted by law.
In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its status as an
Intergovernmental Organization or submit itself to any jurisdiction.
