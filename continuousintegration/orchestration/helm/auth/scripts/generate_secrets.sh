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

k8s_create_secret() {
  local secret_name="$1"
  local filename="$2"
  local filepath="$3"
  local namespace="${NAMESPACE:-default}"

  # Encode file contents base64 (no line wraps)
  local filedata
  filedata=$(base64 -w0 < "$filepath")

  # Construct JSON payload
  read -r -d '' payload <<EOF
{
  "apiVersion": "v1",
  "kind": "Secret",
  "metadata": {
    "name": "${secret_name}",
    "namespace": "${namespace}"
  },
  "type": "Opaque",
  "data": {
    "${filename}": "${filedata}"
  }
}
EOF

  # Set up API access from in-cluster env
  local host="https://${KUBERNETES_SERVICE_HOST}:${KUBERNETES_SERVICE_PORT}"
  local token
  token=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
  local cacert="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

  # POST to create a new secret
  curl -sS --cacert "$cacert" \
       -H "Authorization: Bearer $token" \
       -H "Content-Type: application/json" \
       -X POST \
       -d "$payload" \
       "${host}/api/v1/namespaces/${namespace}/secrets"
}


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
  k8s_create_secret "$secret_name" "$filename" "$filepath"
done
