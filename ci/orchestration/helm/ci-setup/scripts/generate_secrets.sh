#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -euo pipefail
set -x

SECRETS_DIR="/secrets"
: "${NAMESPACE:?NAMESPACE env var must be set}"

# --- Setup --- #

mkdir -p "$SECRETS_DIR"
microdnf install -y --disablerepo=* --enablerepo=baseos openssl

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
openssl req -passin pass:1234 -new -x509 -days 365 -key $SECRETS_DIR/ca.key -out $SECRETS_DIR/ca.crt -subj "/C=CH/ST=Geneva/L=Geneva/O=Test/OU=Test/CN=Root CA"

# Generate server key and CSR
openssl genrsa -passout pass:1234 -des3 -out $SECRETS_DIR/server.key 4096
openssl req -passin pass:1234 -new -key $SECRETS_DIR/server.key -out server.csr -subj "/C=CH/ST=Geneva/L=Geneva/O=Test/OU=Server/CN=cta-frontend-grpc"

# Sign the server cert with the CA
openssl x509 -req -passin pass:1234 -days 365 -in server.csr -CA $SECRETS_DIR/ca.crt -CAkey $SECRETS_DIR/ca.key -set_serial 01 -out $SECRETS_DIR/server.crt

# Remove passphrase from the server key
openssl rsa -passin pass:1234 -in $SECRETS_DIR/server.key -out $SECRETS_DIR/server.key

chmod 0644 /$SECRETS_DIR/ca.key
chmod 0644 /$SECRETS_DIR/server.key

# --- Generate K8s secrets for all of these --- #

k8s_create_secret() {
  local secret_name="$1"
  local filename="$2"
  local filepath="$3"

  # Encode file contents base64 (no line wraps)
  local filedata
  filedata=$(base64 -w0 < "$filepath")

  # Construct JSON payload
  payload=$(cat <<EOF
{
  "apiVersion": "v1",
  "kind": "Secret",
  "metadata": {
    "name": "${secret_name}",
    "namespace": "${NAMESPACE}"
  },
  "type": "Opaque",
  "data": {
    "${filename}": "${filedata}"
  }
}
EOF
  )

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
       "${host}/api/v1/namespaces/${NAMESPACE}/secrets"
}



# Regex: DNS label must be <= 63 chars, start/end with alphanum, contain only a-z0-9- (no consecutive dashes at ends)
is_valid_name() {
  [[ "$1" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] && [[ ${#1} -le 63 ]]
}

# Create a secret for every file in the input directory
# The secret name is equivalent to the file name (lowercase and dots replaced by dashes)
for filepath in "$SECRETS_DIR"/*; do
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
