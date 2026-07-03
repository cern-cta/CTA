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
openssl genrsa -passout pass:1234 -des3 -out $SECRETS_DIR/ca.key.pem 4096
openssl req -passin pass:1234 -new -x509 -days 365 -key $SECRETS_DIR/ca.key.pem -out $SECRETS_DIR/ca.crt.pem -subj "/C=CH/ST=Geneva/L=Geneva/O=Test/OU=Test/CN=Root CA"

generate_signed_cert() {
  local file_prefix="$1"
  local host="$2"
  local key="$SECRETS_DIR/${file_prefix}.key.pem"
  local csr="$SECRETS_DIR/${file_prefix}.csr.pem"
  local crt="$SECRETS_DIR/${file_prefix}.crt.pem"

  echo "Generating cert/key pair '$file_prefix' for '$host'"

  openssl genrsa -passout pass:1234 -des3 -out $key 4096
  openssl req -passin pass:1234 -new -key $key -out $csr \
    -subj "/C=CH/ST=Geneva/L=Geneva/O=Test/OU=Server/CN=$host" \
    -addext "subjectAltName=DNS:$host"
  openssl x509 -req -passin pass:1234 -days 365 -in $csr -CA $SECRETS_DIR/ca.crt.pem -CAkey $SECRETS_DIR/ca.key.pem \
    -set_serial 01 -copy_extensions copy -out $crt
  openssl rsa -passin pass:1234 -in $key -out $key
  rm -f $csr
}

# Generate server certs for the WFE and admin frontends
generate_signed_cert server-admin cta-frontend-admin
generate_signed_cert server-wfe cta-frontend-wfe

chmod 0644 $SECRETS_DIR/ca.key.pem
chmod 0644 $SECRETS_DIR/server-admin.key.pem
chmod 0644 $SECRETS_DIR/server-wfe.key.pem

# Generate corresponding JWT tokens for these certs, along with a JWKs file
python3 /scripts/generate_jwt.py \
  --output-dir "$SECRETS_DIR" \
  --cert "$SECRETS_DIR/server-admin.crt.pem" \
  --key "$SECRETS_DIR/server-admin.key.pem" \
  --jwks "jwks.json" \
  --sub ctaadmin1
python3 /scripts/generate_jwt.py \
  --output-dir "$SECRETS_DIR" \
  --cert "$SECRETS_DIR/server-wfe.crt.pem" \
  --key "$SECRETS_DIR/server-wfe.key.pem" \
  --jwks "jwks.json" \
  --sub ctaeos

# generate cert for ctaeos (to be used with mTLS)
generate_signed_cert ctaeos-self-signed eos-mgm.biglab
# --- Generate K8s secrets for all of these --- #

python3 /scripts/create_k8s_secrets.py \
  --namespace "$NAMESPACE" \
  --dir "$SECRETS_DIR"
