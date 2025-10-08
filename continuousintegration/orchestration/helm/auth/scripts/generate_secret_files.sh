#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


set -euo pipefail

OUTPUT_DIR="/output"

# --- SSS Keytabs --- #

# Generated using echo y | xrdsssadmin -k ctaeos+ -u daemon -g daemon add /tmp/eos.keytab
# Note that this hardcoded keytab is only for CI/dev purposes
# The reason that this is hardcoded is that xrdsssadmin requires xrootd-server which takes quite long to install
# That would slow down the CI startup significantly...
echo "0 u:daemon g:daemon n:ctaeos+ N:7449334094434926593 c:1734433252 e:0 f:0 k:32869bcee6fb7ccfb835e28b2622286260d93ea5b5ad5aa3d854ba59c8754e6" > $OUTPUT_DIR/eos.keytab
# Same as above, but changing the user and group to the eos instance name
echo "0 u:ctaeos g:ctaeos n:ctaeos+ N:7449334094434926593 c:1734433252 e:0 f:0 k:32869bcee6fb7ccfb835e28b2622286260d93ea5b5ad5aa3d854ba59c8754e6" > $OUTPUT_DIR/cta-frontend.keytab

# --- Certificates --- #

# Generate CA key and cert
openssl genrsa -passout pass:1234 -des3 -out $OUTPUT_DIR/ca.key 4096
openssl req -passin pass:1234 -new -x509 -days 365 -key $OUTPUT_DIR/ca.key -out $OUTPUT_DIR/ca.crt -subj "/C=CH/ST=Geneva/L=Geneva/O=Test/OU=Test/CN=Root CA"

# Generate server key and CSR
openssl genrsa -passout pass:1234 -des3 -out $OUTPUT_DIR/server.key 4096
openssl req -passin pass:1234 -new -key $OUTPUT_DIR/server.key -out server.csr -subj "/C=CH/ST=Geneva/L=Geneva/O=Test/OU=Server/CN=cta-frontend-grpc"

# Sign the server cert with the CA
openssl x509 -req -passin pass:1234 -days 365 -in server.csr -CA $OUTPUT_DIR/ca.crt -CAkey $OUTPUT_DIR/ca.key -set_serial 01 -out $OUTPUT_DIR/server.crt

# Remove passphrase from the server key
openssl rsa -passin pass:1234 -in $OUTPUT_DIR/server.key -out $OUTPUT_DIR/server.key

chmod 0644 /$OUTPUT_DIR/ca.key
chmod 0644 /$OUTPUT_DIR/server.key
