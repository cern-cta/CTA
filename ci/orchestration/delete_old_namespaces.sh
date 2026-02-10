#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -euo pipefail

IFS=$'\n\t'
echo "Cleaning up old namespaces:"

# Exclude the default and kube-* namespaces
mapfile -t old_namespaces < <(
  kubectl get namespaces -o json |
    jq -r '
      .items[]
      | select(.metadata.name != "default")
      | select(.metadata.name | test("^kube-") | not)
      | .metadata.name
    '
)

if ((${#old_namespaces[@]} == 0)); then
  echo "No old namespaces to clean up."
  exit 0
fi

for ns in "${old_namespaces[@]}"; do
  ./delete_instance.sh --namespace "$ns" --discard-logs
done

echo "Cleanup complete"
