---
date: 2024-07-18
section: 1cta
title: CTA-CATALOGUE-SCHEMA-VERIFY
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

cta-catalogue-schema-verify --- Verify the CTA Catalogue schema

# SYNOPSIS

**cta-catalogue-schema-verify** *databaseConnectionFile* \[\--help]

# DESCRIPTION

**cta-catalogue-schema-verify** verifies that the schema of the CTA
Catalogue deployed in the database conforms to the schema definition
defined by the CTA software.

*databaseConnectionFile* is the path to the configuration file
containing the connection details of the CTA Catalogue database.

The tool checks the following items:

-   The schema matches the expected version
-   Compare table names, column names and types
-   Compare constraint names (except `NOT NULL` constraints in PostgreSQL)
-   Compare index names
-   Display warnings if any constraints do not have an index for the keys on both sides
-   Display warnings if any tables have been set as `PARALLEL` (Oracle)
-   Display warnings if Oracle types, synonyms, stored procedures or
    error checking tables are detected in the catalogue database

# OPTIONS

-h, \--help

:   Display command options and exit.

# EXIT STATUS

**cta-catalogue-schema-verify** returns 0 on success and non-zero if any
errors are detected.

# EXAMPLE

cta-catalogue-schema-verify /etc/cta/cta-catalogue.conf

# SEE ALSO

CERN Tape Archive documentation [https://eoscta.docs.cern.ch/](https://eoscta.docs.cern.ch/)

# COPYRIGHT

Copyright © 2024 CERN. License GPLv3+: GNU GPL version 3 or later [http://gnu.org/licenses/gpl.html](http://gnu.org/licenses/gpl.html).
This is free software: you are free to change and redistribute it. There is NO WARRANTY, to the extent permitted by law.
In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its status as an
Intergovernmental Organization or submit itself to any jurisdiction.
