#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

dnf install -y cta-admin-grpc
# Rename to cta-admin, overwriting the xrootd/ssi cta-admin if it exists
# since tests expect the CLI utility to be named cta-admin
ln -sf /usr/bin/cta-admin-grpc /usr/bin/cta-admin

echo "Sleeping..."
# sleep forever but exit immediately when pod is deleted
exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"
