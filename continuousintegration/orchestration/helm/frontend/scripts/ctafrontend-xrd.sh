#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "${BASH_SOURCE[0]}")] Started"

# Install missing RPMs
dnf install -y cta-frontend cta-catalogueutils cta-debuginfo cta-debugsource
if [[ "$SCHEDULER_BACKEND" == "ceph" ]]; then
  dnf config-manager --enable ceph
  dnf install -y ceph-common
fi

echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "${BASH_SOURCE[0]}")] Ready"
tail -F /var/log/cta/cta-frontend.log &
runuser --shell='/bin/bash' --session-command='cd ~cta; xrootd -l /var/log/cta-frontend-xrootd.log -k fifo -n cta -c /etc/cta/cta-frontend-xrootd.conf -I v4' cta
