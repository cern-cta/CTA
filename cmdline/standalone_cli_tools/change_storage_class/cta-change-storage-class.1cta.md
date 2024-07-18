---
date: 2024-07-18
section: 1cta
title: CTA-CHANGE-STORAGE-CLASS
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

cta-change-storage-class --- Change which storageclass files with a given archive id use

# SYNOPSIS

**cta-change-storage-class** \--id/-I *archiveFileID* \| \--filename/-F *filename* \--storage.class.name/-n *storageClassName*

# DESCRIPTION

**cta-change-storage-class** changes the storage class for the files
specified by the user. The tool will first change the storage class on
the EOS side, and then change the storage class for the files that were
successfully changed in EOS in the catalogue

# OPTIONS

-I, \--id *archiveFileId*

:   Archive file id of the files to change.

-F, \--filename *filename*

:   Path to a file with a archive file ids. The text file should have one id on each line.

-n, \--storage.class.name *storageClassName*

:   Path to a file with a archive file ids. The text file should have one id on each line.

-t, \--frequency *osRequestFrequency*

:   After the eosRequestFrequency amount of requests to EOS the tool will sleep for 1 second.

# EXIT STATUS

**cta-change-storage-class** returns 0 on success.

# EXAMPLE

cta-change-storage-class \--filename \~/idfile.txt \--storage.class.name newStorageClassName

# SEE ALSO

CERN Tape Archive documentation [https://eoscta.docs.cern.ch/](https://eoscta.docs.cern.ch/)

# COPYRIGHT

Copyright © 2024 CERN. License GPLv3+: GNU GPL version 3 or later [http://gnu.org/licenses/gpl.html](http://gnu.org/licenses/gpl.html).
This is free software: you are free to change and redistribute it. There is NO WARRANTY, to the extent permitted by law.
In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its status as an
Intergovernmental Organization or submit itself to any jurisdiction.
