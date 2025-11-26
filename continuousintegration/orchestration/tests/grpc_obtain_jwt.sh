#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2024 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING"). You can
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

# Test-specific script to obtain JWT token from Keycloak for GRPC testing
# This should be called during test setup, not at deployment time

set -e

KEYCLOAK_URL="${KEYCLOAK_URL:-http://auth-keycloak:8080}"
REALM="${KEYCLOAK_REALM:-master}"
CLIENT_ID="${KEYCLOAK_CLIENT_ID:-grpc-client}"
USERNAME="${KEYCLOAK_USERNAME:-ctaadmin1}"
PASSWORD="${KEYCLOAK_PASSWORD:-ctaadmin1}"
TOKEN_FILE="${JWT_TOKEN_FILE:-/etc/grid-security/jwt-token-grpc}"

die() {
  echo "ERROR: $*" 1>&2
  exit 1
}

echo "$(date '+%Y-%m-%d %H:%M:%S') [grpc_obtain_jwt] Fetching JWT token from Keycloak for testing..."

# Install jq if not already present
if ! command -v jq &> /dev/null; then
  echo "$(date '+%Y-%m-%d %H:%M:%S') [grpc_obtain_jwt] Installing jq for JSON parsing..."
  dnf install -y jq || die "Failed to install jq"
fi

# Obtain an access token from Keycloak
response=$(curl -s -X POST "${KEYCLOAK_URL}/realms/${REALM}/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password" \
  -d "client_id=${CLIENT_ID}" \
  -d "username=${USERNAME}" \
  -d "password=${PASSWORD}") || die "Failed to connect to Keycloak at ${KEYCLOAK_URL}"

# Extract access token using jq
access_token=$(echo "${response}" | jq -r .access_token)

if [ -z "$access_token" ] || [ "$access_token" = "null" ]; then
  echo "$(date '+%Y-%m-%d %H:%M:%S') [grpc_obtain_jwt] Keycloak response:"
  echo "${response}"
  die "Failed to extract access token from Keycloak response"
fi

# Ensure directory exists
mkdir -p "$(dirname "$TOKEN_FILE")" || die "Failed to create directory for token file"

# Write token to file
printf "%s" "$access_token" > "$TOKEN_FILE" || die "Failed to write token to $TOKEN_FILE"

echo "$(date '+%Y-%m-%d %H:%M:%S') [grpc_obtain_jwt] JWT token successfully saved to $TOKEN_FILE"
echo "$(date '+%Y-%m-%d %H:%M:%S') [grpc_obtain_jwt] Token will be used for GRPC authentication in tests"
