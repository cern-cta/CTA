#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2025 CERN
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

: "${NAMESPACE:?NAMESPACE env var must be set}"
INPUT_DIR="/input"

# Regex: DNS label must be <= 63 chars, start/end with alphanum, contain only a-z0-9- (no consecutive dashes at ends)
is_valid_name() {
  [[ "$1" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] && [[ ${#1} -le 63 ]]
}

# Create a secret for every file in the input directory
# The secret name is equivalent to the file name (lowercase and dots replaced by dashes)
for filepath in "$INPUT_DIR"/*; do
  [[ -f "$filepath" ]] || continue

  filename=$(basename "$filepath")
  secret_name="${filename//./-}"      # Replace . with -
  secret_name=$(echo "$secret_name" | tr '[:upper:]' '[:lower:]')  # Lowercase

  if ! is_valid_name "$secret_name"; then
    echo "Warning: Skipping file '$filename'. Invalid secret name: '$secret_name'" >&2
    continue
  fi

  echo "Creating secret: $secret_name from $filename"
  kubectl create secret generic "$secret_name" \
    --from-file="$filename=$filepath" \
    --namespace "$NAMESPACE" \
    --dry-run=client -o yaml | kubectl apply -f -
done
