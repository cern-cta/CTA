#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2024 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.

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
