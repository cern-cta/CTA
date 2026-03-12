#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# By default, only Postgres is installed, if we detect Oracle, we install OCCI instead
grep -q oracle /etc/cta/cta-catalogue.conf && dnf install -y cta-lib-catalogue-occi
dnf install -y mt-st lsscsi sg3_utils cta-taped cta-tape-label cta-eosdf

# cta-taped is ran with runuser to avoid a bug with Docker that prevents both
# the setresgid(-1, 1474, -1) and setresuid(-1, 14029, -1) system calls from
# working correctly
tail -F "/var/log/cta/cta-taped.log" &
runuser -c "/usr/bin/cta-taped -c /etc/cta/cta-taped.conf --foreground --log-format=json --log-to-file=/var/log/cta/cta-taped.log"
