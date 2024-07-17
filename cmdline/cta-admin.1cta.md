% CTA-ADMIN(1cta) | The CERN Tape Archive (CTA)
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

**cta-admin** \[\--json] \[\--config] \<path> *command* \[*subcommand*] \[*options*]

# DESCRIPTION

**cta-admin** sends the specified command to the CTA Frontend (see **CONFIGURATION** below).

Invoking **cta-admin** with no command line parameters shows a list of valid commands. To show the
subcommands and options for a specific command, type:

|        **cta-admin** *command* help

## Commands

Commands have a long version and an abbreviated version (shown in brackets).

activitymountrule (amr)

: Add, change, remove or list the activity mount rules. This is provided as an alternative to requester mount rules and
  group mount rules, where the scheduling priority is based on metadata sent by the client rather than the authenticated
  identity of the requestor.

admin (ad)

: Add, change, remove or list the administrators of the system. In order to use **cta-admin**, users must
  exist in the administrator list and must authenticate themselves with a valid Kerberos KRB5 credential.

archiveroute (ar)

: Add, change, remove or list the archive routes, which are the policies linking namespace entries to
  tape pools.

diskinstance (di)

: Add, change, remove or list the disk instances. A CTA installation has one or more disk instances. A
  disk instance is a separate namespace. Multiple disk instances should be configured if it is desired
  to have a separate namespace for each Virtual Organization (VO).

diskinstancespace (dis)

: Add, change, remove or list the disk instance spaces. A disk instance can contain zero or more disk
  instance spaces. A disk instance space is a partition of the disk. For example, it can be desirable
  to have separate spaces for archival and retrieval operations on each instance.

disksystem (ds)

: Add, change, remove or list the disk systems. The disk system defines the disk buffer to be used for
  CTA archive and retrieval operations for each VO. It corresponds to a specific directory tree on a
  disk instance space (specified using a regular expression). Backpressure can be configured separately
  for each disk system (how much free space should be available before processing a batch of retrieve
  requests; how long to sleep when the disk is full).

drive (dr)

:   Bring tape drives up or down, list tape drives or remove tape drives from the CTA system.

    **cta-admin drive ls** displays an exclamation mark (**!**) in front of the drive name, for drives
    in DISABLED logical libraries.

failedrequest (fr)

: List and remove requests which failed and for which all retry attempts failed.

groupmountrule (gmr)

: Add, change, remove or list the group mount rules.

logicallibrary (ll)

: Add, change, remove or list the logical libraries, which are logical groupings of tapes and drives based
  on physical location and tape drive capabilities. A tape can be accessed by a drive if it is in the same
  physical library and if the drive is capable of reading or writing the tape. In this case, that tape and
  that drive should normally also be in the same logical library.

mediatype (mt)

: Add, change, remove or list the tape cartridge media types. This command is used to specify the nominal
  capacity of each media type, which is used to estimate the total capacity of tape pools. Optionally,
  specify the parameters for software Recommended Access Order (LTO-8 or older tape technology). See
  **cta-taped(1cta)** for details.

mountpolicy (mp)

: Add, change, remove or list the mount policies.

recycletf (rtf)

: List tape files in the recycle log.

repack (re)

: Add or remove a request to repack one or more tapes. This command can also list repack requests in
  progress and display any errors.

requestermountrule (rmr)

: Add, change, remove or list the requester mount rules.

showqueues (sq)

: Show the status of all active queues.

storageclass (sc)

: Add, change, remove or list the storage classes. Storage classes are associated with directories, to
  specify the number of tape copies for each file, and the corresponding tape pool that each copy
  should be archived to. In EOS, the storage class is added as an extended attribute of the
  directory, which is inherited by the file at creation time.

tape (ta)

: Add, change, remove, reclaim, list or label tapes. This command is used to manage the physical tape
  cartridges in each library.

tapefile (tf)

: List files on a specified tape. **cta-admin tapefile ls -l** allows listing the disk metadata
  as well as tape metadata. Use of this option requires that gRPC is correctly configured on the disk
  system. See **FILES**, below.

tapepool (tp)

: Add, change, remove or list tape pools. Tape pools are logical sets of tapes which are used to manage
  the tape lifecycle: label -> supply -> user pool -> erase -> label. **cta-admin tapepool ls** shows
  statistics such as the total number of tapes in the pool and number of free tapes.

version (v)

: Display the version of **cta-admin**, the CTA Frontend, the protocol buffer used for client/server
  communication, and the CTA Catalogue schema.

virtualorganization (vo)

: Add, change, remove or list the Virtual Organizations (VOs). Each VO corresponds to an entity whose data
  transfers should be managed independently of the others, for example an experimental collaboration.

# OPTIONS

\--json

: Some commands, such as **cta-admin tapefile ls**, can return an arbitrarily long list of results,
  which are normally returned in plain text format, with one record per line. If the \--json option is
  supplied, the results are returned as an array of records in JSON format. This option is intended for
  use by scripts to ease automated processing of results.

\--config

: This option is intended for users who want to specify the location of the config file as an argument in the cli.
  If the option is not provided, the script will look for the config file *~/.cta/cta-cli.conf*. The default config file
  */etc/cta/cta-cli.conf* is used if the user does not provide one.

# CONFIGURATION

The **cta-admin** configuration is specified in */etc/cta/cta-cli.conf*. The following configuration
options are available:

cta.endpoint (mandatory)

: Specifies the CTA Frontend hostname (FQDN) and TCP port.

cta.resource.options (default: *none*)

: Currently the only supported option is *Reusable*. For an explanation of XRootD SSI reusable resources, see:
  [http://xrootd.org/doc/dev49/ssi_reference-V2.htm#_Toc506323429](http://xrootd.org/doc/dev49/ssi_reference-V2.htm#_Toc506323429)

cta.log (default: *none*)

: Sets the client log level (see **XrdSsiPbLogLevel** below).

cta.log.hiRes (default: *false*)

: Specify whether log timestamps should have second (*false*) or microsecond (*true*) resolution.

# EXIT STATUS

**cta-admin** returns 0 on success.

If there is an error, a message will be printed on *stderr*. XRootD errors, protocol buffer
errors and CTA Frontend errors return exit code 1. User errors (e.g. invalid tape pool or VID) return
with exit code 2.

In the case of user errors, when the \--json option is specified, **cta-admin** will return an empty JSON array
to *stdout* in addition to the error message printed on *stderr*. Scripts using **cta-admin** should interpret
the error code to determine whether valid parameters were used.

# ENVIRONMENT

XrdSecPROTOCOL

: Sets the XRootD security protocol to use for client/server connections. Note that the CTA Frontend enforces
  the use of the *krb5* protocol. Admin commands sent using a different security protocol will be rejected.

XrdSsiPbLogLevel (default: *none*)

:    Set the desired log level. Logging is sent to stderr.

     Available log levels are: *none* *error* *warning* *info* *debug*

     There are two additional debugging flags to expose details of the communication between client and server:

         *protobuf* shows the contents of the Google Protocol buffers used for communication in JSON format
         *protoraw* shows the serialized Google Protocol buffer, i.e. the binary format sent on the wire

     Log level *all* is a synonym for "*debug* *protobuf* *protoraw*".

XRDDEBUG

: If the XRootD environment variable XRDDEBUG=1, the log level is set to *all* (see above).

XrdSecDEBUG

: If the XRootD environment variable XrdSecDEBUG=1, authentication messages are logged. This is useful for
  debugging problems with Kerberos or SSS authentication.

XRDSSIDEBUG

: If the XRootD environment variable XRDSSIDEBUG=1, debug messages will be displayed for each low-level SSI
  event.

XRD\_REQUESTTIMEOUT (default: *1800* seconds)

:    Sets a limit on the time for the entire request to be processed: connection to load balancer +
     redirection + connection to data server + request/response round-trip. Normally this should be less than
     one second, but for a heavily-loaded system can take more than one minute.

     The same timeout is also applied to the response for list commands. List commands can return arbitrarily
     long output, but by using the XRootD SSI stream mechanism, the timeout is applied to each packet of the
     response rather than the total time taken to process the response.

XRD\_STREAMTIMEOUT

: Note that the XRootD stream timeout is explicitly disabled by the XRootD server for SSI requests, so this
  environment variable is **not** used.

XRD\_CONNECTIONRETRY (default: *1*)

: By default, if the connection to the CTA Frontend fails for any reason, **cta-admin** returns immediately with
  **[FATAL] Connection error**. XRD\_CONNECTIONRETRY and XRD\_CONNECTIONWINDOW can be used to configure
  retry behaviour. Note that Connection Retry is a misnomer as it sets the total number of attempts, not the
  number of retries.

# FILES

***/etc/cta/cta-cli.conf***

: See **CONFIGURATION** above, and */etc/cta/cta-cli.conf.example*.

***/etc/cta/eos.grpc.keytab***

:    gRPC keys for each instance, used by **cta-admin tapefile ls** to display disk metadata associated with
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
