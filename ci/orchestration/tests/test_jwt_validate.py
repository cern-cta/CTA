# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from jwcrypto import jwk
import jwt
import requests
import argparse

# on client pod
# dnf install python3-pip

parser = argparse.ArgumentParser()
parser.add_argument("--jwkUri", help="the URI to query for fetching the JWKS")
parser.add_argument("--token", help="the token to validate")
args = parser.parse_args()

token = args.token

header = jwt.get_unverified_header(token)

# Get the 'kid'
kid = header.get("kid")
print("KID:", kid)

# now for the specific KID, get the public key from the JWKS
# Fetch the JWKS from Keycloak
jwks_url = args.jwkUri
response = requests.get(jwks_url)
jwks = response.json()
key_data = next((key for key in jwks["keys"] if key["kid"] == kid), None)

if key_data is None:
    raise ValueError(f"No key found with kid: {kid}")

key = jwk.JWK(**key_data)
pubkey = key.export_to_pem(private_key=False).decode()
print(pubkey)

## Validate the token using the public key..?
jwt.decode(token, pubkey, algorithms=["RS256"])
