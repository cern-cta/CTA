#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
#
# SPDX-License-Identifier: GPL-3.0-or-later

# Install missing RPMs
dnf install -y cta-cli cta-immutable-file-test xrootd-client eos-client

echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "${BASH_SOURCE[0]}")] Ready"
# sleep forever but exit immediately when pod is deleted
exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"
