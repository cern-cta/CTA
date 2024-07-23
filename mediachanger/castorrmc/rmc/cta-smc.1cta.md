---
date: 2024-07-18
section: 1cta
title: CTA-SMC
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

cta-smc --- CTA SCSI Media Changer client

# SYNOPSIS

**cta-smc** -d -D *drive_ordinal* \[ -V *vid* ]\
**cta-smc** -e -V *vid*\
**cta-smc** -i \[ -V *vid* ]\
**cta-smc** -m -D *drive_ordinal* -V *vid*\
**cta-smc** -q D \[ -D *drive_ordinal* ] \[ \--json ]\
**cta-smc** -q L \[ \--json ]\
**cta-smc** -q P \[ \--json ]\
**cta-smc** -q S \[ -N nb_elements ] \[ -S starting_slot ] \[ \--json ]\
**cta-smc** -q V \[ -N nb_elements ] \[ -V *vid* ] \[ \--json ]

# DESCRIPTION

**cta-smc** is the SCSI Media Changer client. It controls the mounting,
dismounting and exporting of tapes in a robotic tape library and can be
used to query the state of the library.

# OPTIONS

-d

:   Triggers a dismount operation. The drive must be already unloaded.
    The drive must be specified, but the Volume ID (VID) of the
    cartridge is optional. If -V *vid* is specified, *vid* must match
    the VID on the cartridge to be dismounted.

-D *drive_ordinal*

:   Specify which drive to control or query. *drive_ordinal* is the SCSI
    bus number of the drive, starting from 0.

-e

:   Moves a tape from a storage slot to the export slot (which can be
    virtual). The VID must be specified.

-i

:   Imports either a specific VID (indicated by -V) or all tapes in
    state \"import\" in the import/export slots.

-m

:   Triggers a mount operation. The drive must be free. The drive and
    the VID must be specified.

-N *nb_elements*

:   Specifies the maximum number of entries to be reported.

-q *query_type*

:   Queries the library and produces a report about the state of the
    library. *query_type* may be one of the following:

    D

    :   Produce a list of drives with their status and the VID of the
        mounted tape (if any). The status of all drives is reported,
        unless a specific drive is selected with the -D option.

    L

    :   Prints the result of the SCSI **INQUIRY** command
        (Vendor/Product/Revision). It also gives the starting address
        and the number of slots, drives, ports and transports (robotic
        arms) in the library.

    P

    :   Prints the status of import/export slots.

    S

    :   Prints the status of slots. By default all slots are reported,
        but the starting address may be specified with the -S option,
        and the number of elements to be reported may be specified with
        the -N option.

    V

    :   Prints the status of volumes (tapes). A single VID or a pattern
        may also be specified with the -V option.

-S *starting_slot*

:   Specifies the starting slot address for the query operation.

-V *vid*

:   A full VID or a pattern may be specified using shell wildcard
    characters \'?\' and \'\*\'.

\--json

:   Return query responses in JSON format (for use in scripts).

# EXIT STATUS

0 OK.\
1 Parameter error or unrecoverable error (just log it).\
2 Should release drive and retry in 600 seconds.\
3 Should retry in 60 seconds.\
4 Should first do a demount force.\
5 Should configure the drive down.\
6 Should send a message to the operator and exit.\
7 Ops msg (nowait) + release drive + slow retry.\
8 Should send a message to the operator and wait.\
9 Should unload the tape and retry demount.\
16 Robot busy.

# EXAMPLE

To mount the tape with VID JK2005 on drive 1:

> cta-smc -m -D 1 -V JK2005

To dismount the tape present on drive 1 after checking the VID:

> cta-smc -d -D 1 -V JK2005

To query the main charateristics of the library:

> cta-smc -q L
>
>     Vendor/Product/Revision = <IBM     03584L32        1802>
>     Transport Count = 1, Start = 1000
>     Slot Count = 99, Start = 0
>     Port Count = 1, Start = 1010
>     Device Count = 2, Start = 1030

To query the status of all the drives:

> cta-smc -q D
>
>     Drive Ordinal   Element Addr.   Status          Vid
>              0          1030        free
>              1          1031        unloaded        JK2005

To get the list of a few slots in the library:

> cta-smc -q S -S 20 -N 10
>
>     Element Addr.   Vid
>           20        JK2021
>           21        JK2022
>           22        JK2023
>           23        JK2024
>           24        JK2025
>           25        JK2026
>           26        JK2027
>           27        JK2028
>           28        JK2029
>           29        JK2030

To get the status of tapes for which the VID starts with JK200

> cta-smc -q V -V \'JK200\*\'
>
>     Vid     Element Addr.   Element Type
>     JK2001         0        slot
>     JK2002         1        slot
>     JK2003         2        slot
>     JK2004         3        slot
>     JK2006         5        slot
>     JK2007         6        slot
>     JK2008         7        slot
>     JK2009         8        slot
>     JK2005      1031        drive

# SEE ALSO

CERN Tape Archive documentation [https://eoscta.docs.cern.ch/](https://eoscta.docs.cern.ch/)

# COPYRIGHT

Copyright © 2024 CERN. License GPLv3+: GNU GPL version 3 or later [http://gnu.org/licenses/gpl.html](http://gnu.org/licenses/gpl.html).
This is free software: you are free to change and redistribute it. There is NO WARRANTY, to the extent permitted by law.
In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its status as an
Intergovernmental Organization or submit itself to any jurisdiction.
