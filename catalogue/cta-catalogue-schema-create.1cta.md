---
date: 2024-07-18
section: 1cta
title: CTA-CATALOGUE-SCHEMA-CREATE
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

cta-catalogue-schema-create --- Create the CTA Catalogue schema

# SYNOPSIS

**cta-catalogue-schema-create** *databaseConnectionFile* \[\--help] \[\--version *schema_version*]

# DESCRIPTION

**cta-catalogue-schema-create** is a command-line tool that creates the
CTA catalogue database schema. It will abort if the **CTA_CATALOGUE**
table is already present in the database.

*databaseConnectionFile* is the path to the configuration file
containing the connection details of the CTA Catalogue database.

# OPTIONS

-h, \--help

:   Display command options and exit.

-v, \--version *schema_version*

:   Version of the CTA Catalogue schema to be created. By default,
    **cta-catalogue-schema-create** creates the latest version of the
    schema. This option allows the creation of an earlier version. This
    is useful for testing the upgrade from an older schema version to a
    newer version.

# EXIT STATUS

**cta-catalogue-schema-create** returns 0 on success.

# EXAMPLE

cta-catalogue-schema-create /etc/cta/cta-catalogue.conf

# SEE ALSO

CERN Tape Archive documentation [https://eoscta.docs.cern.ch/](https://eoscta.docs.cern.ch/)

# COPYRIGHT

Copyright © 2024 CERN. License GPLv3+: GNU GPL version 3 or later [http://gnu.org/licenses/gpl.html](http://gnu.org/licenses/gpl.html).
This is free software: you are free to change and redistribute it. There is NO WARRANTY, to the extent permitted by law.
In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its status as an
Intergovernmental Organization or submit itself to any jurisdiction.
