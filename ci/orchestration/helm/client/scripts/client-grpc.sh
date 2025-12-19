#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "${BASH_SOURCE[0]}")] Started"

# Install missing RPMs
dnf install -y cta-admin-grpc cta-immutable-file-test xrootd-client eos-client
# Rename cta-admin-grpc to cta-admin, overwriting the xrootd/ssi cta-admin if it exists
# since tests expect the CLI utility to be named cta-admin
ln -sf /usr/bin/cta-admin-grpc /usr/bin/cta-admin

echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "${BASH_SOURCE[0]}")] Ready"
# sleep forever but exit immediately when pod is deleted
exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"
