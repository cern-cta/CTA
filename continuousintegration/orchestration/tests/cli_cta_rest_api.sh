#!/bin/bash

set -e

response=$(curl -X POST http://auth-keycloak:8080/realms/master/protocol/openid-connect/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password" \
  -d "client_id=admin-cli" \
  -d "username=admin" \
  -d "password=admin" \
  -s)

access_token=$(echo ${response} | jq -r .access_token)


curl -s http://cta-rest-api/v0/health

curl -s -H "Authorization: Bearer ${access_token}" http://cta-rest-api/v0/status | jq
