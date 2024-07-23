---
date: 2024-07-18
section: 1cta
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

cta-database-poll --- Poll the CTA Catalogue database once per second

# SYNOPSIS

**cta-database-poll** *databaseConnectionFile* *numberOfSecondsToKeepPolling* \[\--help]

# DESCRIPTION

**cta-database-poll** is a tool for testing and monitoring of the
database connection to the CTA Catalogue. The advantage over other
lower-level tools is that it abstracts away the details of the specific
database technology, using the CTA Catalogue configuration file to
determine where to connect.

**cta-database-poll** pings the configured database once a second for
the number of seconds specified.

*databaseConnectionFile* is the path to the configuration file
containing the connection details of the CTA Catalogue database.

*numberOfSecondsToKeepPolling* is the total number of seconds that
**cta-database-poll** should run before exiting.

# OPTIONS

-h, \--help

:   Display command options and exit.

# EXIT STATUS

**cta-database-poll** returns 0 on success.

# EXAMPLE

cta-database-poll /etc/cta/cta-catalogue.conf 5

# SEE ALSO

CERN Tape Archive documentation [https://eoscta.docs.cern.ch/](https://eoscta.docs.cern.ch/)

# COPYRIGHT

Copyright © 2024 CERN. License GPLv3+: GNU GPL version 3 or later [http://gnu.org/licenses/gpl.html](http://gnu.org/licenses/gpl.html).
This is free software: you are free to change and redistribute it. There is NO WARRANTY, to the extent permitted by law.
In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its status as an
Intergovernmental Organization or submit itself to any jurisdiction.
