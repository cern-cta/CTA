#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
#
# SPDX-License-Identifier: GPL-3.0-or-later


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
