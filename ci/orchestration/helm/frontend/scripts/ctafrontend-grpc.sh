#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "${BASH_SOURCE[0]}")] Started"

# Install missing RPMs
dnf install -y cta-frontend-grpc cta-catalogueutils
if [[ "$SCHEDULER_BACKEND" == "ceph" ]]; then
  dnf config-manager --enable ceph
  dnf install -y ceph-common
fi

echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "${BASH_SOURCE[0]}")] Ready"

#tail -F /var/log/cta/cta-frontend-grpc.log &
runuser --shell='/bin/bash' --session-command='/usr/bin/cta-frontend-grpc >> /var/log/cta/cta-frontend-grpc.log' cta
