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

OUTPUT_DIR="/output"

# --- SSS Keytabs --- #

# Generated using:
#   echo y | xrdsssadmin -k ctaeos+ -u daemon -g daemon add /tmp/eos.keytab
#   echo y | xrdsssadmin -k ctaeos+ -u ctaeos -g ctaeos add /tmp/cta-frontend.keytab
# Note that this hardcoded keytab is only for CI/dev purposes
# The reason that this is hardcoded is that xrdsssadmin requires xrootd-server which takes quite long to install
# That would slow down the CI startup significantly...
echo "0 u:daemon g:daemon n:ctaeos+ N:7570028795780923393 c:1762534677 e:0 f:0 k:468153fa4be9a871c7f7e1fa3aefbfeb12d3f0a99ff4a18f9b6ebe3d3abacbc1" > $OUTPUT_DIR/eos.keytab
# Same as above, but changing the user and group to the eos instance name
echo "0 u:ctaeos g:ctaeos n:ctaeos+ N:7570030999099146241 c:1762535190 e:0 f:0 k:b97f9aab547f9afefdef4d3cd0894d8080530ecbe0375d9b2e8d7b4a4238fa03" > $OUTPUT_DIR/cta-frontend.keytab

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
