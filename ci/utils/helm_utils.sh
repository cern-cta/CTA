#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Call from ci/orchestration after sourcing log_utils.sh.
# Keep this file POSIX-sourceable: check_charts.sh uses /bin/sh.

# Local charts, ordered so that each one is packaged before its consumers.
cta_local_charts="common cta-tools frontend-wfe frontend-admin taped rmcd maintd cta"

check_helm_installed() {
  if ! command -v helm >/dev/null 2>&1; then
    die "Helm does not seem to be installed. To install Helm, see: https://helm.sh/docs/intro/install/"
  fi
}

update_local_cta_chart_dependencies() {
  log_task "Updating local chart dependencies..."
  local chart
  for chart in $cta_local_charts; do
    # Local dependencies need no remote repositories; skip both index loading and refreshes.
    helm dependency update "helm/$chart" --skip-refresh --repository-config /dev/null > /dev/null
  done
}
