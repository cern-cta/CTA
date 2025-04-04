#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2024 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.

. /opt/run/bin/init_pod.sh
echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "${BASH_SOURCE[0]}")] Started"

# Install missing RPMs
dnf install -y cta-frontend cta-catalogueutils cta-debuginfo cta-debugsource
if [ "$SCHEDULER_BACKEND" == "ceph" ]; then
  dnf config-manager --enable ceph
  dnf install -y ceph-common
fi

echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "${BASH_SOURCE[0]}")] Ready"

tail -F /var/log/cta/cta-frontend-grpc.log &
runuser --shell='/bin/bash' --session-command='/usr/bin/cta-frontend-grpc >> /var/log/cta/cta-frontend-grpc.log' cta

echo "ctafrontend died"
echo "analysing core file if any"
/scripts/ctafrontend-backtrace.sh
sleep infinity # Keep the container alive for debugging purposes
