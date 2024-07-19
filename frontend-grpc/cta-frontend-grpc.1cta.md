---
date: 2024-07-18
section: 1cta
title: CTA-FRONTEND-GRPC
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

cta-frontend-grpc --- gRPC-based Frontend for CTA

# SYNOPSIS

**cta-frontend-grpc** \[\--port *port*] \[\--log-header\|\--no-log-header] \[\--tls] \[\--threads *N*]
\[\--version] \[\--help]

# DESCRIPTION

**cta-frontend-grpc** is the daemon process providing a gRPC endpoint for integration with storage systems.

# OPTIONS

-p, \--port *port*

:   TCP port used to accept connections from the client.

-c, \--threads *N*

:   Maximum number of threads to process gRPC requests. This defaults to 8 times the number of CPUs.

-n, \--log-header

:   Include timestamp and host name into the log messages.

-s, \--no-log-header

:   Don\'t include timestamp and host name into the log messages, default behaviour.

-t, \--tls

:   Enable TLS for communication. The paths to the TLS certificate, key and root-ca chan files in PEM
    format should be specified by *gRPC TlsCert*, *gRPC TlsKey* and *gRPC TlsChain* in the *cta.conf*
    configuration file.

-v, \--version

:   Print the version number and exit.

-h, \--help

:   Display command options and exit.

# FILES

*/etc/cta/cta.conf*

:   The CTA configuration file.

*/etc/cta/cta-catalogue.conf*

:   The CTA catalog configuration file. See */etc/cta/cta-catalogue.conf.example*.

# SEE ALSO

CERN Tape Archive documentation [https://eoscta.docs.cern.ch/](https://eoscta.docs.cern.ch/)

# COPYRIGHT

Copyright © 2024 CERN. License GPLv3+: GNU GPL version 3 or later [http://gnu.org/licenses/gpl.html](http://gnu.org/licenses/gpl.html).
This is free software: you are free to change and redistribute it. There is NO WARRANTY, to the extent permitted by law.
In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its status as an
Intergovernmental Organization or submit itself to any jurisdiction.
