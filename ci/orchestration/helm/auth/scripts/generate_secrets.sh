#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -euo pipefail

SECRETS_DIR="/secrets"
: "${NAMESPACE:?NAMESPACE env var must be set}"

# --- Setup --- #

mkdir -p "$SECRETS_DIR"
# Extra flags are to improve speed
pip install --no-cache-dir --no-deps --only-binary :all: pyjwt cryptography cffi

# --- SSS Keytabs --- #

# Generated using:
#   echo y | xrdsssadmin -k ctaeos+ -u daemon -g daemon add /tmp/eos.keytab
# Note that this hardcoded keytab is only for CI/dev purposes
# The reason that this is hardcoded is that xrdsssadmin requires xrootd-server which takes quite long to install
# That would slow down the CI startup significantly...
echo "0 u:daemon g:daemon n:ctaeos+ N:7570028795780923393 c:1762534677 e:0 f:0 k:468153fa4be9a871c7f7e1fa3aefbfeb12d3f0a99ff4a18f9b6ebe3d3abacbc1" > $SECRETS_DIR/eos.keytab
# Same as above, but changing the user and group to the eos instance name
echo "0 u:ctaeos g:ctaeos n:ctaeos+ N:7570028795780923393 c:1762534677 e:0 f:0 k:468153fa4be9a871c7f7e1fa3aefbfeb12d3f0a99ff4a18f9b6ebe3d3abacbc1" > $SECRETS_DIR/cta-frontend.keytab

# --- Certificates --- #

# Generate CA key and cert
openssl genrsa -passout pass:1234 -des3 -out $SECRETS_DIR/ca.key 4096
openssl req -passin pass:1234 -new -x509 -days 365 -key $SECRETS_DIR/ca.key -out $SECRETS_DIR/ca.crt -subj "/C=CH/ST=Geneva/L=Geneva/O=Test/OU=Test/CN=Root CA" -addext "basicConstraints = critical, CA:TRUE" -addext "keyUsage = critical, keyCertSign, cRLSign"

# Generate server key and CSR
openssl genrsa -passout pass:1234 -des3 -out $SECRETS_DIR/server.key 4096
openssl req -passin pass:1234 -new -key $SECRETS_DIR/server.key -out server.csr -subj "/C=CH/ST=Geneva/L=Geneva/O=Test/OU=Server/CN=cta-frontend-grpc"

# Sign the server cert with the CA
openssl x509 -req -passin pass:1234 -days 365 -in server.csr -CA $SECRETS_DIR/ca.crt -CAkey $SECRETS_DIR/ca.key -set_serial 01 -out $SECRETS_DIR/server.crt

# Remove passphrase from the server key
openssl rsa -passin pass:1234 -in $SECRETS_DIR/server.key -out $SECRETS_DIR/server.key

# Generate SciTokens issuer TLS key and cert
openssl genrsa -out $SECRETS_DIR/scitokens-issuer.key 4096
openssl req -new -key $SECRETS_DIR/scitokens-issuer.key -out $SECRETS_DIR/scitokens-issuer.crt -subj "/C=CH/ST=Geneva/L=Geneva/O=Test/OU=Server/CN=scitokens-issuer" -addext "basicConstraints = critical, CA:FALSE" -addext "keyUsage = critical, digitalSignature, keyEncipherment" -addext "extendedKeyUsage = serverAuth" -addext "subjectAltName = DNS:scitokens-issuer" -CA $SECRETS_DIR/ca.crt -CAkey $SECRETS_DIR/ca.key -passin pass:1234 -days 365 -set_serial 02

chmod 0644 $SECRETS_DIR/ca.key
chmod 0644 $SECRETS_DIR/server.key
chmod 0644 $SECRETS_DIR/scitokens-issuer.key

# --- JWT for Frontend Auth --- #

python3 /scripts/generate_jwt.py \
  --output-dir "$SECRETS_DIR" \
  --cert "$SECRETS_DIR/server.crt" \
  --key "$SECRETS_DIR/server.key" \
  --sub ctaeos \
  --sub ctaadmin1

python3 /scripts/generate_jwt.py \
  --output-dir "$SECRETS_DIR" \
  --cert "$SECRETS_DIR/scitokens-issuer.crt" \
  --key "$SECRETS_DIR/scitokens-issuer.key" \
  --issuer "https://scitokens-issuer:8443" \
  --audience "https://wlcg.cern.ch/jwt/v1/any" \
  --wlcg-version "1.0" \
  --jwk-format rsa \
  --jwks-filename scitokens.jwks \
  --jwt-filename scitokens.jwt \
  --scope "storage.create:/" \
  --scope "storage.read:/" \
  --scope "storage.modify:/" \
  --scope "storage.stage:/" \
  --sub poweruser1

echo '{ "issuer": "https://scitokens-issuer:8443", "jwks_uri": "https://scitokens-issuer:8443/jwk" }' > "$SECRETS_DIR/scitokens-well-known"

# --- Generate K8s secrets for all of these --- #

python3 /scripts/create_k8s_secrets.py \
  --namespace "$NAMESPACE" \
  --dir "$SECRETS_DIR"
