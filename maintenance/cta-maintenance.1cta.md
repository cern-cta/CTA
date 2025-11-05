---
date: 2025-08-12
section: 1cta
title: CTA-MAINTENANCE
header: The CERN Tape Archive (CTA)
---
<!---
@project      The CERN Tape Archive (CTA)
@copyright    Copyright © 2020-2025 CERN
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

cta-maintenance --- CTA Maintenance daemon

# SYNOPSIS

**cta-maintenance** \[\--config *config_file*] \[\--stdout]] \[\--log-to-file *log_file*] \[\--log-format *format*]\
**cta-maintenance** \--help

# DESCRIPTION

**cta-maintenance** is the daemon responsible for performing housekeeping tasks related to the disk buffer and scheduler. It is composed of different routines that periodically execute.

**Disk Report Routine**

**Queue Cleanup Routine**

**Garbage Collector Routine**

**Repack Request Routine**

# OPTIONS

-c, \--config *config_file*

:   Read **cta-maintenance** configuration from *config_file* instead of the default,
    */etc/cta/cta-maintenance.conf*.

-h, \--help

:   Display command options and exit.

-l, \--log-to-file *log_file*

:   Log to a file instead of stdout.

-o, \--log-format *format*

:   Output format for log messages. \[default\|json\]

-s, \--stdout

:   Log to standard output. Logging to stdout is the default, but this option is kept for compatibility reasons

# CONFIGURATION

The **cta-maintenance** daemon reads its configuration parameters from the CTA configuration file (by
default, */etc/cta/cta-maintenance.conf*). Each option is listed with its *default* value.

## Tape Server Configuration Options
maintenance LogMask *INFO*

:   Logs with a level lower than this value will be masked. Possible
    values are EMERG, ALERT, CRIT, ERR, WARNING, NOTICE (USERERR), INFO,
    DEBUG. USERERR log level is equivalent to NOTICE, because by
    convention, CTA uses log level NOTICE for user errors.

maintenance LogFormat *json*

:   The default format for log lines is jsos. If
    this option is set to *unstructured*, log lines will be output in key=value format.

maintenance CatalogueConfigFile */etc/cta/cta-catalogue.conf*

:   Path to the CTA Catalogue configuration file. See **FILES**, below.

ObjectStore BackendPath (no default)

:   URL of the objectstore (CTA Scheduler Database). Usually this will
    be the URL of a Ceph RADOS objectstore. For testing or small
    installations, a file-based objectstore can be used instead. See
    **cta-objectstore-initialize**.

## General Configuration Options

This options will be included in every log line of the tape daemon to
enhance log identification when swapping drives between different backends.

InstanceName (no default)

:   Unique string to identify CTA\'s catalogue instance the tape daemon is serving.

SchedulerBackendName (no default)

:   The unique string to identify the backend scheduler resources. It
    can be structured as: \[ceph\|postgres\|vfs]\[User\|Repack].

# FILES

*/etc/cta/cta-maintenance.conf*

:   The CTA Maintenance configuration file, containing the options
    described above under **CONFIGURATION**. See */etc/cta/cta-maintenance.conf.example*.

*/etc/cta/cta-catalogue.conf*

:   Usual location for the CTA Catalogue configuration file. See **CatalogueConfigFile**
    option under **CONFIGURATION**, and */etc/cta/cta-catalogue.conf.example*.

*/var/log/cta/cta-maintenance.log*

:   Usual location for the maintenance log file.

# SEE ALSO

CERN Tape Archive documentation [https://cta.docs.cern.ch/](https://eoscta.docs.cern.ch/)

# COPYRIGHT

Copyright © 2025 CERN. License GPLv3+: GNU GPL version 3 or later [http://gnu.org/licenses/gpl.html](http://gnu.org/licenses/gpl.html).
This is free software: you are free to change and redistribute it. There is NO WARRANTY, to the extent permitted by law.
In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its status as an
Intergovernmental Organization or submit itself to any jurisdiction.

