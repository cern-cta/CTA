---
date: 2024-07-18
section: 1cta
title: CTA-READTP
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

cta-readtp --- Low-level utility to read files from a tape

# SYNOPSIS

**cta-readtp** *vid* *sequence* \[\--destination\_files *file_url*] \[\--drive *drive_name*] \[\--help]

# DESCRIPTION

**cta-readtp** is a command-line tool for reading files from tape and
validating their checksums.

The tape to be read is specified by the *vid* argument. The tape files
to be read are specified as a *sequence* of tape file sequence numbers.
The syntax used to specify the sequence is as follows:

    f1-f2         Files f1 to f2 inclusive.
    f1-           Files f1 to the last file on the tape.
    f1-f2,f4,f6-  A series of non-consecutive ranges of files.

# OPTIONS

-f, \--destination\_files *file_url*

:   Path to a file containing a list of destination URLs that the files
    will be written to (one URL per line). If not specified, all data
    read will be written to */dev/null*. If there are less destination
    files than read files, the remaining files read will be written to
    */dev/null*.

-u, \--drive *drive_name*

:   The unit name of the drive used. Defaults to the first taped
    configuration file matching *cta-taped-unitName.conf* under the
    directory */etc/cta/*.

-h, \--help

:   Display command options and exit.

# EXIT STATUS

**cta-readtp** returns 0 on success.

# EXAMPLE

cta-readtp V01007 10002,10004-10006,10008-

# SEE ALSO

CERN Tape Archive documentation [https://eoscta.docs.cern.ch/](https://eoscta.docs.cern.ch/)

# COPYRIGHT

Copyright © 2024 CERN. License GPLv3+: GNU GPL version 3 or later [http://gnu.org/licenses/gpl.html](http://gnu.org/licenses/gpl.html).
This is free software: you are free to change and redistribute it. There is NO WARRANTY, to the extent permitted by law.
In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its status as an
Intergovernmental Organization or submit itself to any jurisdiction.
