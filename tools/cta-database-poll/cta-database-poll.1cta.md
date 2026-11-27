---
date: 2024-07-18
section: 1
title: CTA-DATABASE-POLL
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

cta-database-poll --- Poll a CTA database or wait for readiness

# SYNOPSIS

**cta-database-poll** *databaseConnectionFile* *numberOfSecondsToKeepPolling* \[\--until-ready] \[\--help]

# DESCRIPTION

**cta-database-poll** checks database connectivity using CTA's database drivers and connection files.
It supports the catalogue database and the PostgreSQL scheduler database.
Each attempt connects and queries the database's table names; no CTA schema is required.

By default, the tool polls with a one-second pause between attempts for the specified number of iterations, logging database errors and continuing.
This mode is intended for testing reconnection behaviour.

*databaseConnectionFile* is the path to the catalogue or PostgreSQL scheduler connection file.

*numberOfSecondsToKeepPolling* specifies the polling duration in seconds, excluding time spent connecting and querying.
Blocked database calls can extend the overall duration in either mode.

# OPTIONS

\--until-ready

:   Exit successfully after the first successful query, retrying failed attempts once per second.
    Exit with an error if polling finishes without a successful query.

-h, \--help

:   Display command options and exit.

# EXIT STATUS

In the default polling mode, the tool completes the polling loop and returns 1 if any database attempt failed, even if later attempts succeeded; otherwise it returns 0.
With \--until-ready, the tool returns 0 only after a successful query and 1 if polling finishes without success.
Invalid arguments, invalid connection files and unavailable database drivers return a non-zero status in either mode.

# EXAMPLE

cta-database-poll /etc/cta/cta-catalogue.conf 5

Wait for the PostgreSQL scheduler database using up to 120 polling attempts:

cta-database-poll --until-ready /etc/cta/cta-scheduler.conf 120

# SEE ALSO

CERN Tape Archive documentation [https://cta.docs.cern.ch/](https://cta.docs.cern.ch/)

# COPYRIGHT

Copyright © 2024 CERN. License GPLv3+: GNU GPL version 3 or later [http://gnu.org/licenses/gpl.html](http://gnu.org/licenses/gpl.html).
This is free software: you are free to change and redistribute it. There is NO WARRANTY, to the extent permitted by law.
In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its status as an
Intergovernmental Organization or submit itself to any jurisdiction.
