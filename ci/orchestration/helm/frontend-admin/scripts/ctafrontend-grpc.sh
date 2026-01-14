#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

tail -F /var/log/cta/cta-frontend.log &
/usr/bin/cta-frontend-grpc >> /var/log/cta/cta-frontend.log
