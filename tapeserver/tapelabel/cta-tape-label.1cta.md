---
date: 2024-07-18
section: 1cta
title: CTA-TAPE-LABEL
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

cta-tape-label --- Pre-label a CTA tape

# SYNOPSIS

**cta-tape-label** \[\--oldlabel *old_vid*] \[\--loadtimeout *timeout*] \[\--drive *drive_name*] \[\--force] \[\--help] \[\--debug] \--vid *vid*

# DESCRIPTION

**cta-tape-label** writes the CTA label file **VOL1** at the beginning
of a tape, to prepare the tape to store data files. The label contains
the Volume ID specified by *vid*.

If the tape to be labelled already contains files, running **cta-tape-label**
is a destructive operation, which writes the label file to the beginning
of tape and renders all data beyond the label file inaccessible.

For non-blank tapes, **cta-tape-label** checks that the *vid* supplied
matches the existing label of the tape in the drive. This behaviour can
be changed with the \--oldlabel and \--force options (see **OPTIONS**, below).

# OPTIONS

-o, \--oldlabel *old_vid*

:   The Volume ID (VID) of the current label on the tape, if it is not
    the same as *vid* and the tape is not blank.

-t, \--loadtimeout *timeout*

:   The timeout to load the tape in the drive slot, in seconds. Defaults
    to 7200 (two hours), to allow time for media initialisation.

-u, \--drive *drive_name*

:   The unit name of the drive used. Defaults to the first taped
    configuration file matching *cta-taped-unitName.conf* under the
    directory */etc/cta/*.

-f, \--force

:   **Warning: this option destroys any data on the tape without first
    performing the label check.**

    Force labelling for non-blank tapes. This is intended for testing or
    use in rare situations by expert operators. This option should not be
    used when calling **cta-tape-label** from a script.

-h, \--help

:   Display command options and exit.

-d, \--debug

:   Verbose log messages.

# EXIT STATUS

**cta-tape-label** returns 0 on success.

# EXAMPLES

cta-tape-label \--vid I54321 \--oldvid T12345 \--debug

cta-tape-label \--vid L54321 \--force

# SEE ALSO

CERN Tape Archive documentation [https://eoscta.docs.cern.ch/](https://eoscta.docs.cern.ch/)

# COPYRIGHT

Copyright © 2024 CERN. License GPLv3+: GNU GPL version 3 or later [http://gnu.org/licenses/gpl.html](http://gnu.org/licenses/gpl.html).
This is free software: you are free to change and redistribute it. There is NO WARRANTY, to the extent permitted by law.
In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its status as an
Intergovernmental Organization or submit itself to any jurisdiction.
