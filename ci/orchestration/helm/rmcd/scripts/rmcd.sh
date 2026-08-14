#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# to get rmcd logs to stdout
tail -F /var/log/cta/cta-rmcd.log 2>/dev/null &
/usr/bin/cta-rmcd --log-file=/var/log/cta/cta-rmcd.log --config-strict --config /etc/cta/cta-rmcd.toml --runtime-dir /run/cta
