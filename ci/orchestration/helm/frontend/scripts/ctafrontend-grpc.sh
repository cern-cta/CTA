#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Install missing RPMs
# By default, only Postgres is installed, if we detect Oracle, we install OCCI instead
grep -q oracle /etc/cta/cta-catalogue.conf && dnf install -y cta-lib-catalogue-occi
dnf install -y cta-frontend-grpc cta-catalogueutils

tail -F /var/log/cta/cta-frontend-grpc.log &
runuser --shell='/bin/bash' --session-command='/usr/bin/cta-frontend-grpc >> /var/log/cta/cta-frontend-grpc.log' cta
