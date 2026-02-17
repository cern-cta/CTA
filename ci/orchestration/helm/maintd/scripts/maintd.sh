#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

dnf install -y cta-maintd

# to get maintd logs to stdout
tail -F /var/log/cta/cta-maintd.log &
runuser --shell='/bin/bash' --session-command="/usr/bin/cta-maintd --log-file=/var/log/cta/cta-maintd.log --config-strict --config /etc/cta/cta-maintd.toml --runtime-dir" cta
