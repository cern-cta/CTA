#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

tail -F "/var/log/cta/cta-taped.log" 2>/dev/null &
cta-taped --log-file=/var/log/cta/cta-taped.log --config-strict --config /etc/cta/cta-taped.toml --runtime-dir /run/cta
