#!/bin/bash

## needs to be run from the client pod
response=$(curl -X POST http://auth-keycloak:8080/realms/master/protocol/openid-connect/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password" \
  -d "client_id=admin-cli" \
  -d "username=admin" \
  -d "password=admin" \
  -s)

access_token=$(echo ${response} | jq -r .access_token)

## to get the public key of keycloak:
## probably something like this:
pubkey=$(curl -X GET http://auth-keycloak/realms/master | jq -r .public_key)

## it seems it is not straightforward to set a custom expiration time for a keycloak token, default is one minute always

## SEt up port forwarding
kubectl port-forward pod/auth-keycloak 8080:8080 -n dev

# check the config

response=$(curl -X POST http://auth-keycloak:8080/realms/master/protocol/openid-connect/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password" \
  -d "client_id=admin-cli" \
  -d "username=admin" \
  -d "password=admin" \
  -s)

access_token=$(echo ${response} | jq -r .access_token)

client_admin=$(curl -X GET http://auth-keycloak:8080/admin/realms/master/clients/b2bc9e53-323e-4354-8ec1-297e7898f535 \
  -H "Authorization: Bearer $access_token"
)

clients=$(curl -X GET http://auth-keycloak:8080/admin/realms/master/clients \
  -H "Authorization: Bearer $access_token"
) # yes, this endpoint works. I care about admin-cli for now. Other clients are broker, account, etc
# id of admin-cli is b2bc9e53-323e-4354-8ec1-297e7898f535

clients=$(curl -X GET http://auth-keycloak:8080/admin/realms/master/clients-initial-access \
  -H "Authorization: Bearer $access_token"
)

client_scopes=$(curl -X GET http://auth-keycloak:8080/admin/realms/master/client-scopes \
  -H "Authorization: Bearer $access_token"
)

auth_providers=$(curl -X GET http://auth-keycloak:8080/admin/realms/master/authentication/authenticator-providers \
  -H "Authorization: Bearer $access_token"
)

client_auth_providers=$(curl -X GET http://auth-keycloak:8080/admin/realms/master/authentication/client-authenticator-providers \
  -H "Authorization: Bearer $access_token"
)

provider_=$(curl -X GET http://auth-keycloak:8080/admin/realms/master/authentication/config-description/provider_id \
  -H "Authorization: Bearer $access_token"
)

### How to obtain longer-lived tokens
response=$(curl -X POST http://auth-keycloak:8080/realms/master/protocol/openid-connect/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password" \
  -d "client_id=admin-cli" \
  -d "username=admin" \
  -d "password=admin" \
  -s)

access_token=$(echo ${response} | jq -r .access_token)

admin_cli_id="b2bc9e53-323e-4354-8ec1-297e7898f535"

curl -X PUT "http://auth-keycloak:8080/admin/realms/master/clients/" \
  -H "Authorization: Bearer $access_token" \
  -H "Content-Type: application/json" \
  -d '{
    "attributes": {
      "access.token.lifespan": "4800"
    }
}'

## to verify:
curl -X GET "http://auth-keycloak:8080/admin/realms/master/clients/$admin_cli_id" \
  -H "Authorization: Bearer $access_token" \
  -s | jq .attributes


curl -X PUT "http://auth-keycloak:8080/admin/realms/master/clients/$admin_cli_id" \
  -H "Authorization: Bearer $access_token" \
  -H "Content-Type: application/json" \
  -d '{
    "accessTokenLifespan": 3600
}'
