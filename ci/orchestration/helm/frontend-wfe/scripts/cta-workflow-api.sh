#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

tail -F /var/log/cta/cta-workflow-api.log &
/usr/bin/cta-workflow-api --log-file /var/log/cta/cta-workflow-api.log --config /etc/cta/cta-workflow-api.toml --runtime-dir /run/cta
