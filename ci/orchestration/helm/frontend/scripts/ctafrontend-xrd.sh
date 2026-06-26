#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

tail -F /var/log/cta/cta-frontend.log &
cd ~cta; xrootd -l /var/log/cta-frontend-xrootd.log -k fifo -n cta -c /etc/cta/cta-frontend-xrootd.conf -I v4
