#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -e


echo "Installing the cta-scheduler-utils"
dnf install -y cta-scheduler-utils

echo "Dropping the scheduler DB schema"
echo "yes" | cta-scheduler-schema-drop /etc/cta/cta-scheduler.conf

echo "Creating the scheduler DB schema"
cta-scheduler-schema-create /etc/cta/cta-scheduler.conf

echo "Scheduler reset completed"
