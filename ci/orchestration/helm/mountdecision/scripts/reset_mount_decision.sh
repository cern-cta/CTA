#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -e

die() {
  echo "$@" 1>&2
  exit 1
}

echo "Installing the cta-mount-decision-utils"
dnf install -y cta-mount-decision-utils

echo "Dropping the Mount Decision DB schema"
echo "yes" | cta-mount-decision-schema-drop /etc/cta/cta-mount-decision.conf || die "ERROR: Could not drop Mount Decision DB schema."

echo "Creating the Mount Decision DB schema"
cta-mount-decision-schema-create /etc/cta/cta-mount-decision.conf || die "ERROR: Could not create Mount Decision DB schema."

echo "Mount Decision DB reset completed"
