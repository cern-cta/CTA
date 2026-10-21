#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

tail -F /var/log/cta/cta-admin-api.log &
/usr/bin/cta-admin-api --log-file /var/log/cta/cta-admin-api.log --config /etc/cta/cta-admin-api.toml --runtime-dir /run/cta
