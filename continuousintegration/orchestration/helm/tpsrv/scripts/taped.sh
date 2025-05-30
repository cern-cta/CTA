#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2022-2024 CERN
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
dnf install -y mt-st lsscsi sg3_utils cta-taped cta-tape-label cta-debuginfo cta-eosdf cta-debugsource

if [ "$SCHEDULER_BACKEND" == "ceph" ]; then
  dnf config-manager --enable ceph
  dnf install -y  ceph-common
fi

# cta-taped is ran with runuser to avoid a bug with Docker that prevents both
# the setresgid(-1, 1474, -1) and setresuid(-1, 14029, -1) system calls from
# working correctly
echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "${BASH_SOURCE[0]}")] Ready"
tail -F "/var/log/cta/cta-taped-${DRIVE_NAME}.log" &
runuser -c "/usr/bin/cta-taped -c /etc/cta/cta-taped-${DRIVE_NAME}.conf --foreground --log-format=json --log-to-file=/var/log/cta/cta-taped-${DRIVE_NAME}.log"

echo "taped died"
sleep infinity # Keep the container alive for debugging purposes
