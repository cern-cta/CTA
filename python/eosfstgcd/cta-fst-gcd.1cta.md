---
date: 2024-07-18
section: 1cta
title: CTA-FST-GCD
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

cta-fst-gcd --- Garbage Collector Daemon for EOS FSTs with CTA tape back-end enabled

# SYNOPSIS

**cta-fst-gcd** \[\--help] \[\--config *config_file*] \[\--stdout]

# DESCRIPTION

**cta-fst-gcd** is a daemon that runs on an EOS disk server (FST), to garbage
collect (evict) disk replicas which have been safely archived to tape.

The **cta-fst-gcd** daemon scans every single EOS disk file on the FST.
A file is garbage collected if:

-   The amount of free space on the corresponding file system is
    considered too low.

-   The file is considered old enough to be garbage collected.

The **cta-fst-gcd** daemon garbage collects an EOS disk file by
extracting the hexadecimal EOS file identifier (*fxid*) from the local
disk filename and then running:

> **eos stagerm fxid:***fxid*.

# OPTIONS

-h, \--help

:   Display command options and exit.

-c, \--config *config_file*

:   Set the path of the configuration file. Defaults to
    */etc/cta/cta-fst-gcd.conf*.

-s, \--stdout

:   Sets log output to stdout. This disables use of a log file.

# CONFIGURATION

The **cta-fst-gcd** daemon reads its parameters from its configuration
file, by default */etc/cta/cta-fst-gcd.conf*.

**log_file =** */var/log/eos/fst/cta-fst-gcd.log*

:   Path of the garbage collector log file.

**mgm_host =** *HOSTNAME.2NDLEVEL.TOPLEVEL*

:   Fully qualified host name of EOS MGM.

**eos_spaces =** *EOS_SPACE_1* *EOS_SPACE_2*

:   Space-separated list of names of the EOS spaces to be garbage
    collected.

**eos_space_to_min_free_bytes =** *EOS_SPACE_1:10000000000* *EOS_SPACE_2:10000000000*

:   Minimum number of free bytes each filesystem should have.

**gc_age_secs =** *7200*

:   Minimum age of a file before it can be considered for garbage
    collection.

**absolute_max_age_secs =** *604800*

:   Age at which a file will be considered for garbage collection,
    regardless of the amount of free space.

**query_period_secs =** *310*

:   Delay in seconds between free space queries to the local file
    systems.

**main_loop_period_secs =** *300*

:   Period in seconds of the main loop of the **cta-fst-gcd** daemon.

**xrdsecssskt =** */etc/eos.keytab*

:   Path to Simple Shared Secrets keytab to authenticate with EOS MGM.

# EXIT STATUS

**cta-fst-gcd** returns 0 on success.

# FILES

*/etc/cta/cta-fst-gcd.conf*

:   Default location for the configuration file of the **cta-fst-gcd**
    daemon. This can be overriden using the \--config option. See
    **CONFIGURATION** above, and */etc/cta/cta-fst-gcd.conf.example*.

*/var/log/eos/fst/cta-fst-gcd.log*

:   The default log file of the **cta-fst-gcd** daemon. This can be
    changed in **cta-fst-gcd.conf** or disabled with the \--stdout
    option.

# SEE ALSO

CERN Tape Archive documentation [https://eoscta.docs.cern.ch/](https://eoscta.docs.cern.ch/)

# COPYRIGHT

Copyright © 2024 CERN. License GPLv3+: GNU GPL version 3 or later [http://gnu.org/licenses/gpl.html](http://gnu.org/licenses/gpl.html).
This is free software: you are free to change and redistribute it. There is NO WARRANTY, to the extent permitted by law.
In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its status as an
Intergovernmental Organization or submit itself to any jurisdiction.
