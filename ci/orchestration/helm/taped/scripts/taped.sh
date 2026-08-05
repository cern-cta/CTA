#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

tail -F "/var/log/cta/cta-taped.log" 2>/dev/null &
cta-taped -c /etc/cta/cta-taped.conf --foreground --log-format=json --log-to-file=/var/log/cta/cta-taped.log
