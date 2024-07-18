---
date: 2024-07-18
section: 1cta
title: CTA-RESTORE-DELETED-FILES
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

cta-restore-deleted-files --- Restore deleted files from the CTA Recycle Bin

# SYNOPSIS

**cta-restore-deleted-files** \[\--id *archive_file_id*] \[\--instance *disk_instance*] \[\--fxid *eos_fxid*]
\[\--fxidfile *filename*] \[\--vid *vid*] \[\--copynb *copy_number*] \[\--help] \[\--debug]

# DESCRIPTION

**cta-restore-deleted-files** restores files deleted from CTA that match
some criteria. Recovered files may have been deleted by a user (with
**eos rm**) or by an operator (with **cta-admin tapefile rm**).

There are three scenarios for disk file recovery:

1.  The file has been deleted in the EOS namespace and in the CTA
    catalogue. This happens in the case of a normal file removal by the
    user (**eos rm**). The strategy is to reinject the file metadata
    into EOS and restore the file entry in the CTA catalogue (with a new
    disk file id).

2.  The file has been deleted in the CTA catalogue, but the metadata is
    unchanged in EOS. This can happen during disk draining. The file is
    kept in the EOS namespace but the entry is removed from the CTA
    Catalogue. The strategy is to restore the CTA Catalogue entry.

3.  The file has been deleted in the CTA catalogue and the EOS
    diskFileId changed. This can happen during the conversion of a file
    from a space to another. The strategy is to recover the CTA
    Catalogue entry and update the disk file id to the one that
    corresponds to the metadata in EOS.

# OPTIONS

-I, \--id *archive_file_id*

:   Archive file id of the files to restore.

-i, \--instance *disk_instance*

:   Disk instance of the files to restore.

-f, \--fxid *eos_fxid*

:   Disk file id of the files to restore, expressed as a hexadecimal
    string.

-F, \--fxidfile *filename*

:   Path to file containing a list of disk file ids to restore.

-v, \--vid *vid*

:   Volume identifier (VID) of the files to restore.

-c, \--copynb *copy_number*

:   Copy number of the files to restore.

-h, \--help

:   Display command options and exit.

-d, \--debug

:   Enable debug log messages.

# EXIT STATUS

**cta-restore-deleted-files** returns 0 on success.

# EXAMPLE

cta-restore-deleted-files \--vid V01007

# SEE ALSO

CERN Tape Archive documentation [https://eoscta.docs.cern.ch/](https://eoscta.docs.cern.ch/)

# COPYRIGHT

Copyright © 2024 CERN. License GPLv3+: GNU GPL version 3 or later [http://gnu.org/licenses/gpl.html](http://gnu.org/licenses/gpl.html).
This is free software: you are free to change and redistribute it. There is NO WARRANTY, to the extent permitted by law.
In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its status as an
Intergovernmental Organization or submit itself to any jurisdiction.
