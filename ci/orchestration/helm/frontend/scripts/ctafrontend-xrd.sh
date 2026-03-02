#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# By default, only Postgres is installed, if we detect Oracle, we install OCCI instead
grep -q oracle /etc/cta/cta-catalogue.conf && dnf install -y cta-lib-catalogue-occi
dnf install -y cta-frontend cta-catalogueutils

tail -F /var/log/cta/cta-frontend.log &
runuser --shell='/bin/bash' --session-command='cd ~cta; xrootd -l /var/log/cta-frontend-xrootd.log -k fifo -n cta -c /etc/cta/cta-frontend-xrootd.conf -I v4' cta
