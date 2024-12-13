#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2024 CERN
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

set -e
set -x

# There are a few things that can be improved here:
# - have separate init containers for the instantiation of the kubernetes secrets. Use an empty dir to generate keytabs and then an official image containing kubectl to create the secrets

echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "${BASH_SOURCE[0]}")] Started"


# See: https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/6/html/managing_smart_cards/configuring_a_kerberos_5_server
yum -y install epel-release
yum -y install krb5-libs krb5-server krb5-workstation

echo "Initialising key distribution center... "
KRB5_DB_MASTER_KEY=$(openssl rand -base64 32)
# Create DB
kdb5_util create -s -r $KRB5_REALM -P $KRB5_DB_MASTER_KEY
# Add main principal
kadmin.local addprinc -pw $KRB5_ADMIN_PRINC_PWD $KRB5_ADMIN_PRINC_NAME/admin
# Start kdc
krb5kdc
# Start kadmind to receive requests to add principals
kadmind

# Readiness container should check if the kdc is reachable

# TODO: this should be done in an init container

# We need to access the Kubernetes API to generate secrets in the current namespace
k8s_namespace=$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace)
k8s_sa_token=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
k8s_sa_ca_cert=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt
k8s_api_server="https://${KUBERNETES_SERVICE_HOST}:${KUBERNETES_SERVICE_PORT}"

echo "Creating keytab secrets"
keytabs=$(cat /tmp/keytabs-names.json)
# Loop through each keytab entry and generate a secret for it
echo "$keytabs" | jq -c '.[]' | while read -r keytab; do
  user=$(echo "$keytab" | jq -r '.user')
  filename=$(echo "$keytab" | jq -r '.keytab')
  keytab_path="/root/$(basename "$filename")"

  # Populate KDC and generate keytab file
  echo "Generating $keytab_path for $user... "
  kadmin.local -q "addprinc -randkey $user"
  kadmin.local -q "ktadd -k $keytab_path $user"

  content=$(base64 "$keytab_path")
  secret_json=$(
    jq -n \
      --arg name "$(basename "$user")-keytab" \
      --arg filename "$(basename "$filename")" \
      --arg content "$content" \
      '{
        apiVersion: "v1",
        kind: "Secret",
        metadata: { name: $name },
        type: "Opaque",
        data: { ($filename): $content }
      }'
  )
  curl -s --cacert "${k8s_sa_ca_cert}" \
          -H "Authorization: Bearer ${k8s_sa_token}" \
          -H "Content-Type: application/json" \
          -X POST \
          -d "${secret_json}" \
          "${k8s_api_server}/api/v1/namespaces/${k8s_namespace}/secrets"

  echo "Created Kubernetes secret: ${user}-keytab in namespace $k8s_namespace"
done

touch /KDC_READY
echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "${BASH_SOURCE[0]}")] Ready"
# sleep forever but exit immediately when pod is deleted
exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"
