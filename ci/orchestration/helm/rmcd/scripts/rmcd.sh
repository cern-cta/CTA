#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# install RPMs
dnf install -y mt-st mtx lsscsi sg3_utils cta-rmcd cta-smc

# to get rmcd logs to stdout
tail -F /var/log/cta/cta-rmcd.log &
runuser --user cta -- /usr/bin/cta-rmcd -f /dev/smc
