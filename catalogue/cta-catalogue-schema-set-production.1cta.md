---
date: 2024-07-18
section: 1cta
title: CTA-CATALOGUE-SCHEMA-SET-PRODUCTION
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

cta-catalogue-schema-set-production --- Mark the CTA Catalogue as \"in production\", to prevent accidentally dropping the schema

# SYNOPSIS

**cta-catalogue-schema-set-production** *databaseConnectionFile* \[\--help]

# DESCRIPTION

**cta-catalogue-schema-set-production** sets the **IS_PRODUCTION** flag
on the **CTA_CATALOGUE** table in the CTA Catalogue database. This
prevents **cta-catalogue-schema-drop** from dropping the schema,
protecting the schema from accidental deletion due to misconfiguation or
human error.

By design, there is no tool to unset the **IS_PRODUCTION** flag. It can
only be unset by running an SQL UPDATE statement directly on the
**CTA_CATALOGUE** table.

*databaseConnectionFile* is the path to the configuration file
containing the connection details of the CTA Catalogue database.

# OPTIONS

-h, \--help

:   Display command options and exit.

# EXIT STATUS

**cta-catalogue-schema-set-production** returns 0 on success.

# EXAMPLE

cta-catalogue-set-production /etc/cta/cta-catalogue.conf

# SEE ALSO

**cta-catalogue-schema-drop**(1cta)

CERN Tape Archive documentation [https://eoscta.docs.cern.ch/](https://eoscta.docs.cern.ch/)

# COPYRIGHT

Copyright © 2024 CERN. License GPLv3+: GNU GPL version 3 or later [http://gnu.org/licenses/gpl.html](http://gnu.org/licenses/gpl.html).
This is free software: you are free to change and redistribute it. There is NO WARRANTY, to the extent permitted by law.
In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its status as an
Intergovernmental Organization or submit itself to any jurisdiction.
