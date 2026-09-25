---
date: 2025-05-28
section: 1cta
title: CTA-OBJECTSTORE-DUMP_OBJECT
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

cta-objectstore-dump-object --- Fetch and print an object from the object store into the terminal

# SYNOPSIS

**cta-objectstore-dump-object** \[\--help] \[\--json] \[\--bodydump] \[objectstoreURL] *objectname*

# DESCRIPTION

**cta-objectstore-dump-object** is a command-line tool that will fetch an object from the object store and
print it in the terminal.

*objectname* is the reference to the object to be printed.

# OPTIONS

\--help

:   Display command options and exit.

\--json

:   Print the output in fully-compliant JSON format.

\--bodydump

:   Print only the contents of the object body (ignore header contents).

# EXIT STATUS

**cta-objectstore-dump-object** returns 0 on success.

# EXAMPLE

cta-objectstore-dump-object --json root

# SEE ALSO

CERN Tape Archive documentation [https://eoscta.docs.cern.ch/](https://eoscta.docs.cern.ch/)

# COPYRIGHT

Copyright © 2024 CERN. License GPLv3+: GNU GPL version 3 or later [http://gnu.org/licenses/gpl.html](http://gnu.org/licenses/gpl.html).
This is free software: you are free to change and redistribute it. There is NO WARRANTY, to the extent permitted by law.
In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its status as an
Intergovernmental Organization or submit itself to any jurisdiction.
