#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


set -e

# Note that this is not client specific and just an example/quick test to see if we can get a token from keycloak
response=$(curl -X POST http://auth-keycloak:8080/realms/master/protocol/openid-connect/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password" \
  -d "client_id=admin-cli" \
  -d "username=admin" \
  -d "password=admin" \
  -s)

access_token=$(echo ${response} | jq -r .access_token)

echo "Access token: $access_token"

## Run the python script here
dnf install -y python3-pip
pip install jwcrypto requests pyjwt

python3 /root/test_jwt_validate.py --token $access_token --jwkUri http://auth-keycloak:8080/realms/master/protocol/openid-connect/certs
