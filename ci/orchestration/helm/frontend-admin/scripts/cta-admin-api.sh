#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

tail -F /var/log/cta/cta-admin-api.log &
/usr/bin/cta-admin-api --config /etc/cta/cta-admin-api.toml >> /var/log/cta/cta-admin-api.log 2>&1
