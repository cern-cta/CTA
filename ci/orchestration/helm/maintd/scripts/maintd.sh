#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

dnf install -y cta-maintd

# to get maintd logs to stdout
#tail -F /var/log/cta/cta-maintd.log &
runuser --shell='/bin/bash' --session-command="/usr/bin/cta-maintd --foreground --log-to-file=/var/log/cta/cta-maintd.log --log-format json --config /etc/cta/cta-maintd.conf" cta
