#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# By default, only Postgres is installed, if we detect Oracle, we install OCCI instead
grep -q oracle /etc/cta/cta-catalogue.conf && dnf install -y cta-lib-catalogue-occi
dnf install -y cta-maintd

# to get maintd logs to stdout
tail -F /var/log/cta/cta-maintd.log &
runuser --shell='/bin/bash' --session-command="/usr/bin/cta-maintd --log-file=/var/log/cta/cta-maintd.log --config-strict --config /etc/cta/cta-maintd.toml --runtime-dir /run/cta" cta
