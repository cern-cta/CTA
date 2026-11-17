#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Call from ci/orchestration after sourcing log_utils.sh.
update_local_cta_chart_dependencies() {
  # Local dependencies need no remote repositories; skip both index loading and refreshes.

  log_task "Updating local chart dependencies..."
  local chart
  local charts=(
    "common"
    "cta-admin-tools"
    "frontend-wfe"
    "frontend-admin"
    "taped"
    "rmcd"
    "maintd"
    "cta"
  )
  for chart in "${charts[@]}"; do
    helm dependency update helm/"$chart" --skip-refresh --repository-config /dev/null > /dev/null
  done
}
