#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# By default, only Postgres is installed, if we detect Oracle, we install OCCI instead
grep -q oracle /etc/cta/cta-catalogue.conf && dnf install -y cta-lib-catalogue-occi
dnf install -y cta-frontend-grpc
# The || section can be removed once we have a stable release with the cta-catalogue-utils name
dnf install -y  cta-catalogue-utils || dnf install -y cta-catalogueutils

tail -F /var/log/cta/cta-frontend.log &
runuser --shell='/bin/bash' --session-command='/usr/bin/cta-frontend-grpc >> /var/log/cta/cta-frontend.log' cta
