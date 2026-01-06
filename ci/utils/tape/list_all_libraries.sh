#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -euo pipefail

echo "["

first=true

lsscsi -g | grep mediumx | awk '{print $3, $4, $7}' | sort | while read -r vendor model device; do
  serial=$(sg_inq -p 0x80 "$device" 2>/dev/null | awk '/Unit serial number:/ {print $NF}')


  # Skip devices without serials
  [ -z "$serial" ] && continue

  library_id=$(printf "%s-%s-%s" "$vendor" "$model" "$serial" \
    | tr '[:upper:]' '[:lower:]' \
    | tr -c 'a-z0-9-' '-')

  if [ "$first" = true ]; then
    first=false
  else
    echo ","
  fi

  cat <<EOF
  {
    "id": "$library_id",
    "vendor": "$vendor",
    "model": "$model",
    "serial": "$serial",
    "device": "$device"
  }
EOF
done

echo
echo "]"
