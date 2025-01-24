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
yum -y install cta-frontend cta-debuginfo
if [ "$SCHEDULER_BACKEND" == "ceph" ]; then
  yum-config-manager --enable ceph
  yum -y install ceph-common
fi

echo "Using the grpc frontend for ctafrontend pod"

echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "${BASH_SOURCE[0]}")] Ready"
runuser --shell='/bin/bash' --session-command='/usr/bin/cta-frontend-grpc >> /var/log/cta/cta-frontend-grpc.log' cta

echo "ctafrontend died"
echo "analysing core file if any"
/opt/run/bin/ctafrontend-backtrace.sh
sleep infinity # Keep the container alive for debugging purposes
