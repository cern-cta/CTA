---
date: 2024-08-15
section: 1cta
title: CTA-ADMIN
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

cta-admin --- Administrative command-line interface to CTA Frontend

# SYNOPSIS

**cta-admin** \[\--json] \[\--config *path*] *command* \[*subcommand*] \[*options*]

**cta-admin** \--help

# DESCRIPTION

**cta-admin** sends the specified command to the CTA Frontend (see **CONFIGURATION** below).

Invoking **cta-admin** with no command line parameters shows a list of valid commands. To show the
subcommands and options for a specific command, type:

> **cta-admin** *command* help

## Commands

Commands have a long version and an abbreviated version (shown in brackets).

activitymountrule (amr)

:   Add, change, remove or list the activity mount rules. This is provided as an alternative to
    requester mount rules and group mount rules. Activity mount rules allow the scheduling priority
    to be set based on metadata sent by the client rather than the authenticated identity of the
    requestor.

admin (ad)

:   Add, change, remove or list the administrators of the system. In order to use **cta-admin**,
    users must be included in the list of administrators, in addition to being authenticated.

archiveroute (ar)

:   Add, change, remove or list the archive routes. Archive routes are the policies linking namespace
    entries to tape pools.

diskinstance (di)

:   Add, change, remove or list the disk instances. A disk instance is a separate namespace. A CTA
    installation has one or more disk instances. Multiple disk instances can be configured if it is
    desired to have a separate namespace for each Virtual Organization (VO).

    **\-\-name** specifies the disk instance name, which is the unique identifier of the disk
    instance and cannot be changed.

diskinstancespace (dis)

:   Add, change, remove or list the disk instance spaces. A disk instance can contain zero or more
    disk instance spaces. A disk instance space is a partition of the disk.

    A typical use case for disk instance spaces is to configure separate spaces for archival and
    retrieval operations on each instance.

    **\-\-name** specifies the disk instance space name. The disk instance name (**\-\-diskinstance**)
    and disk instance space name form a pair which is the unique identifier for the disk instance space.

    **\-\-diskinstance** specifies the name of the disk instance to query, e.g. **eosctapublic**

    **\-\-freespacequeryurl** specifies the URL to query the free disk space on this disk instance
    space. It should be specified in the following format:

        <query protocol>:<name of disk space>

    Example:

        eosSpace:retrieve

    **\-\-refreshinterval** specifies how long (in seconds) the cached value of the free space query
    will be used before performing a new query.

disksystem (ds)

:   Add, change, remove or list the disk systems. The disk system defines the disk buffer to be used
    for CTA archive and retrieval operations for each VO. It corresponds to a specific directory tree
    on a disk instance space.

    **\-\-disksystem** specifies the unique identifier of the disk system.

    **\-\-diskinstance** and **\-\-diskinstancespace** form a pair which specifies the disk instance
    and partition where this disk system is physically located.

    **\-\-fileregexp** specifies the regular expression to match filenames (from *destinationURL*) to disk systems.

    Example:

        destinationURL root://eos_instance//eos/cta/myfile?eos.lfn=fxid:7&eos.space=spinners

    will match the regular expression:

        ^root://eos_instance//eos/cta(.*)eos.space=spinners

    The two options below are provided to configure backpressure for retrieve operations. Backpressure
    is a mechanism to postpone retrieval of files from tape to the disk buffer when there is insufficient
    disk space. It can be configured separately for each disk system.
    
    Before a retrieve mount, the destination URL of each file is pattern-matched to identify the disk
    system. The corresponding disk instance space is queried to determine if there is sufficient free
    space to perform the mount. If there is insufficient space, the tape server sleeps for the specified
    interval.

    **\-\-targetedfreespace** specifies how much free space should be available before processing a
    batch of retrieve requests. It should be calculated based on the free space update latency (based
    on *diskinstancespace* parameters) and the expected bandwidth for transfers to the external Storage
    Element.

    **\-\-sleeptime** specifies how long (in seconds) to sleep when the disk system has insufficient
    space, before retrying the retrieve mount.

drive (dr)

:   Bring tape drives up or down, list tape drives or remove tape drives from the CTA system.

    This is a synchronous command to set and read back the state of one or more tape drives. The
    *drive_name* option accepts a regular expression. If the *drive_name* option is set to **first**,
    the **up**, **down**, **ls** and **ch** commands will scan the local configuration directory
    *\/etc\/cta* and use the drive from the first tape server configuration file found. This does not
    guarantee that the same drive will be used every time.

    **up** puts a drive into active state, able to perform an archive or retrieve mount.

    **down** puts a drive into inactive state, unable to mount. Drives will complete any running session
    and dismount the tape before changing state.

    **\-\-reason** specifies the reason (free text) why the drive is being put down. The reason will
    be displayed in the output of **ls**.

    **ls** lists the drives matching *drive_name*, or all drives by default. An exclamation mark (**!**)
    is displayed in front of the drive name for drives in disabled libraries.

    **rm** deletes the drive definition. Drives must be in the **down** state before deleting.
    (Override with **\-\-force**).

failedrequest (fr)

:   List and remove requests which failed and for which all retry attempts failed.

groupmountrule (gmr)

:   Add, change, remove or list the group mount rules.

logicallibrary (ll)

:   Add, change, remove or list the logical libraries. Logical libraries are logical groupings of
    tapes and drives based on physical location and tape drive capabilities.

    A tape can be accessed by a drive if it is in the same physical library and if the drive is
    capable of reading or writing the tape. In this case, that tape and that drive should normally
    be in the same logical library.

mediatype (mt)

:   Add, change, remove or list the tape cartridge media types. This command is used to specify the
    nominal capacity of each media type, which is used to estimate the total capacity of tape pools.

    Optionally, specify the parameters for software Recommended Access Order (LTO-8 or older tape
    technology only). See **cta-taped(1cta)** for details.

mountpolicy (mp)

:   Add, change, remove or list the mount policies.

physicallibrary (pl)

:   Add, change, remove or list the physical tape libraries.

recycletf (rtf)

:   List tape files in the recycle log.

    Tape files in the recycle log can be listed by VID, EOS disk file ID, EOS disk instance,
    ArchiveFileId or copy number. Disk file IDs should be provided in hexadecimal format (fxid).

repack (re)

:   Add or remove a request to repack one or more tapes, list repack requests in progress, display
    repack errors.

    **add** adds one or more tapes to the active repack requests:

    **\-\-mountpolicy** specifies the mount policy that will be applied to the repack subrequests
    (the retrieve and archive requests).

    **\-\-vid** specifies a single tape to repack.

    **\-\-vidfile** specifies the filename of a text file containing a list of tapes to repack.

    **\-\-bufferurl** optionally specifies the buffer to use in place of the default repack buffer
    URL (specified in the CTA Frontend configuration). It should follow this format:

        root://eos_instance//path/to/repack/buffer

    **\-\-maxfilestoselect** optionally limits the the number of files to be repacked to the specified
    value, overriding the default value (specified in the CTA Frontend configuration). Set the value
    to zero to force all files to be selected.

    **\-\-no-recall** inhibits the retrieve mount. Only files that are already located in the disk
    buffer will be considered for archival.

    By default, CTA will migrate files onto a new tape (or multiple tapes) AND add new (or missing)
    copies of the file. The expected number of copies is defined by the storage class of the file.

    **\-\-justmove** means that the files located on the tape to repack will be migrated onto new
    tape(s), without creating any additional copies.

    **\-\-justaddcopies** means that new (or missing) copies of the files located on the tape to
    repack will be created on the new tape(s), but the source tape file will not be migrated.

    **rm** removes a tape from the list of tapes to repack.

    **ls** lists repack requests in progress. Rows marked with the **\*** flag indicate that not all
    files were selected for repack. The number of files to repack at once can be restricted to avoid
    overloading the Scheduler Database. To repack the remaining files, repeat the operation.

    **err** displays any repack errors.

requestermountrule (rmr)

:   Add, change, remove or list the requester mount rules.

showqueues (sq)

:   Show the status of all active queues. The bottom section shows requests already being serviced by
    tape servers, which is why the values of some fields are at zero.

storageclass (sc)

:   Add, change, remove or list the storage classes. The storage class of a file specifies its
    expected number of tape copies, and the corresponding tape pool that each copy should be archived
    to.

    In EOS, the storage class is specified as an extended attribute of the directory, which is
    inherited as an extended attribute of the file at creation time.

tape (ta)

:   Add, change, remove, reclaim, list or label tapes. This command is used to manage the physical
    tape cartridges in each library.

tapefile (tf)

:   List files on a specified tape or delete a tape file.

    **ls** lists tape files. Which files to list can be specified by VID, the (disk instance,
    disk file ID) pair, or the archive file ID.

    **\-\-vid** specifies the volume identifier (VID) of a tape.

    **\-\-fxid** specifies the disk file ID. EOS disk file IDs should be provided in hexadecimal
    format.

    **\-\-instance** specifies on which disk instance the disk file ID is located.

    **\-\-fxidfile** specifies the filename of a text file containing a list of disk file IDs
    in hexadecimal formal. The format of this file is the same as the output of
    **eos find --fid <path>**.

    **\-\-id** specifies an archive file ID.

    **rm** deletes a single tape file. Note that some disk files have more than one associated tape file.

tapepool (tp)

:   Add, change, remove or list tape pools. Tape pools are logical sets of tapes which are used to
    manage the tape lifecycle:

        label → supply pool → user pool → erase → label

    **add** creates a new tapepool:

    **\-\-partialtapesnumber** specifies the number of tapes which should be made available for
    writing in this tape pool. If the number of tapes eligible for writing falls below this number
    (for example, because a tape becomes full), a new tape will be added from a supply pool.

    **\-\-supply** specifies a comma-separated list of tape pools to use as supply pools, when
    adding new (empty) tapes to this tape pool.

    **ls** shows statistics such as the total number of tapes in the pool and number of free tapes.

version (v)

:   Display the version of **cta-admin**, the CTA Frontend, the protocol buffer used for client/server
    communication, and the CTA Catalogue schema.

virtualorganization (vo)

:   Add, change, remove or list the Virtual Organisations (VOs). A VO corresponds to an entity whose
    data transfers and storage should be managed independently of the others, for example an experimental
    collaboration.

    **\-\-virtualorganisation** specifies the name of the virtual organisation. It must be unique.

    **\-\-readmaxdrives** specifies the maximum number of drives the virtual organisation is allowed
    to use for reading>

    **\-\-writemaxdrives** specifies the maximum number of drives the virtual organisation is allowed
    to use for writing.

    **\-\-maxfilesize** specifies the maximum file size for this virtual organisation. Default is 0,
    which means no limit.

    **\-\-isrepackvo** if set to **true**, normal data archival and retrieval is inhibited for this VO;
    it can only be used for repack operations.

# OPTIONS

**\-\-json**

:   Return the output of **cta-admin** as a JSON object rather than columnar text. This option is
    intended for use by scripts to ease automated processing of results.

**\-\-config**

:   Override the default configuration file */etc/cta/cta-cli.conf*.

# CONFIGURATION

**cta-admin** reads its configuration parameters from */etc/cta/cta-cli.conf*.
Each option is listed with its *default* value.

cta.endpoint (no default)

:   Specifies the CTA Frontend hostname (FQDN) and TCP port.

cta.resource.options *none*

:   Currently the only supported option is *Reusable*. For an explanation of XRootD SSI reusable
    resources, see:
    [http://xrootd.org/doc/dev49/ssi_reference-V2.htm#_Toc506323429](http://xrootd.org/doc/dev49/ssi_reference-V2.htm#_Toc506323429).

cta.log *none*

:   Sets the client log level (see **XrdSsiPbLogLevel** below).

cta.log.hiRes *false*

:   Specify whether log timestamps should have second (*false*) or microsecond (*true*) resolution.

# EXIT STATUS

**cta-admin** returns 0 on success.

If there is an error, a message will be printed on *stderr*. XRootD errors, protocol buffer
errors and CTA Frontend errors return exit code 1. User errors (e.g. invalid tape pool or VID) return
with exit code 2.

In the case of user errors, when the **\-\-json** option is specified, **cta-admin** will return an empty JSON array
to *stdout* in addition to the error message printed on *stderr*. Scripts using **cta-admin** should interpret
the error code to determine whether valid parameters were used.

# ENVIRONMENT

XrdSecPROTOCOL

:   Sets the XRootD security protocol to use for client/server connections.

XrdSsiPbLogLevel *none*

:   Set the desired log level. Logging is sent to stderr.

    Available log levels are: *none* *error* *warning* *info* *debug*

    There are two additional debugging flags to expose details of the communication between client and server:

    *protobuf* shows the contents of the Google Protocol buffers used for communication in JSON format\
    *protoraw* shows the serialized Google Protocol buffer, i.e. the binary format sent on the wire

    Log level *all* is a synonym for "*debug* *protobuf* *protoraw*".

XRDDEBUG

:   If the XRootD environment variable XRDDEBUG=1, the log level is set to *all* (see above).

XrdSecDEBUG

:   If the XRootD environment variable XrdSecDEBUG=1, authentication messages are logged. This is
    useful for debugging problems with Kerberos or SSS authentication.

XRDSSIDEBUG

:   If the XRootD environment variable XRDSSIDEBUG=1, debug messages will be displayed for each
    low-level SSI event.

XRD\_REQUESTTIMEOUT *1800*

:   Specifies the timeout (in seconds) for the entire request to be processed: connection to load balancer +
    redirection + connection to data server + request/response round-trip. Normally this should be less than
    one second, but for a heavily-loaded system can take more than one minute.

    The same timeout is also applied to the response for list commands. List commands can return arbitrarily
    long output, but by using the XRootD SSI stream mechanism, the timeout is applied to each packet of the
    response rather than the total time taken to process the response.

XRD\_STREAMTIMEOUT

:   Note that the XRootD stream timeout is explicitly disabled by the XRootD server for SSI requests,
    so this environment variable is **not** used.

XRD\_CONNECTIONRETRY *1*

:   By default, if the connection to the CTA Frontend fails for any reason, **cta-admin** returns
    immediately with **[FATAL] Connection error**. XRD\_CONNECTIONRETRY and XRD\_CONNECTIONWINDOW
    can be used to configure retry behaviour. Note that Connection Retry is a misnomer as it sets
    the total number of attempts, not the number of retries.

# FILES

*/etc/cta/cta-cli.conf*

:   See **CONFIGURATION** above, and */etc/cta/cta-cli.conf.example*.

*/etc/cta/eos.grpc.keytab*

:   gRPC keys for each instance, used by **cta-admin tapefile ls** to display disk metadata associated with
    files on tape. The format is one line per disk instance, as follows:

        # disk instance  endpoint (host:port)         gRPC token
        eosctaphysics    eosctaphysics.cern.ch:50051  bf8d9c49-2eda-40bd-82aa-630a556caf31

# BUGS

When using the **\--json** option, 64-bit integer fields are returned as strings. This is a feature of the
Google Protocol Buffers library, to cope with the fact that JavaScript parsers do not have the ability
to handle integers with more than 32-bit precision.

# SEE ALSO

CERN Tape Archive documentation [https://eoscta.docs.cern.ch/](https://eoscta.docs.cern.ch/)

# COPYRIGHT

Copyright © 2024 CERN. License GPLv3+: GNU GPL version 3 or later [http://gnu.org/licenses/gpl.html](http://gnu.org/licenses/gpl.html).
This is free software: you are free to change and redistribute it. There is NO WARRANTY, to the extent permitted by law.
In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its status as an
Intergovernmental Organization or submit itself to any jurisdiction.
