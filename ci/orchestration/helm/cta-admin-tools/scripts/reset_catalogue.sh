#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -euo pipefail

config=/etc/cta/cta-catalogue.conf
echo yes | cta-catalogue-schema-drop "$config"

# Older tools do not accept an explicit 'latest' version.
if [[ "$SCHEMA_VERSION" == latest ]]; then
  cta-catalogue-schema-create "$config"
else
  cta-catalogue-schema-create -v "$SCHEMA_VERSION" "$config"
fi
