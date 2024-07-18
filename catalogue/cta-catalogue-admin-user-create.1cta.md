---
date: 2024-07-18
section: 1cta
title: CTA-CATALOGUE-ADMIN-USER-CREATE
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

cta-catalogue-admin-user-create --- Create a CTA admin user

# SYNOPSIS

**cta-catalogue-admin-user-create** *databaseConnectionFile* \--username *username* \--comment *comment* \[\--help]

# DESCRIPTION

**cta-catalogue-admin-user-create** creates an admin user in the CTA
Catalogue database. It connects directly to the database (unlike
**cta-admin**, which connects to the CTA Frontend).
**cta-catalogue-admin-user-create** can therefore be used to bootstrap
the creation of admin users on a new installation of CTA.

*databaseConnectionFile* is the path to the configuration file
containing the connection details of the CTA Catalogue database.

# OPTIONS

-u, \--username *username*

:   The name of the admin user to be created.

-m, \--comment *comment*

:   Comment describing the creation of the admin user.

-h, \--help

:   Display command options and exit.

# EXIT STATUS

**cta-catalogue-admin-user-create** returns 0 on success.

# EXAMPLE

cta-catalogue-admin-user-create /etc/cta/cta-catalogue.conf \--username ctaadmin \--comment \"The CTA admin account\"

# SEE ALSO

**cta-admin**(1cta)

CERN Tape Archive documentation [https://eoscta.docs.cern.ch/](https://eoscta.docs.cern.ch/)

# COPYRIGHT

Copyright © 2024 CERN. License GPLv3+: GNU GPL version 3 or later [http://gnu.org/licenses/gpl.html](http://gnu.org/licenses/gpl.html).
This is free software: you are free to change and redistribute it. There is NO WARRANTY, to the extent permitted by law.
In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its status as an
Intergovernmental Organization or submit itself to any jurisdiction.
