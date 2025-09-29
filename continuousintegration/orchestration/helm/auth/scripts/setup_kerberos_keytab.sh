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

# Script to create Kerberos service principal and keytab for Keycloak
# This should be run after the KDC is initialized

KDC_NAME="${KDC_NAME:-auth-kdc}"
KDC_NAMESPACE="${KDC_NAMESPACE:-default}"
KEYCLOAK_NAME="${KEYCLOAK_NAME:-auth-keycloak}"
KEYCLOAK_NAMESPACE="${KEYCLOAK_NAMESPACE:-default}"
KRB5_REALM="${KRB5_REALM:-TEST.CTA}"
KRB5_ADMIN_PRINC_NAME="${KRB5_ADMIN_PRINC_NAME:-root}"
KRB5_ADMIN_PRINC_PWD="${KRB5_ADMIN_PRINC_PWD:-defaultcipassword}"

echo "Setting up Kerberos keytab for Keycloak..."

# Wait for KDC to be ready
echo "Waiting for KDC to be ready..."
until kubectl get pod ${KDC_NAME} -n ${KDC_NAMESPACE} --no-headers | grep -q Running; do
  echo "Waiting for KDC pod to be running..."
  sleep 5
done

# Create HTTP service principal for Keycloak
echo "Creating HTTP service principal for Keycloak..."
kubectl exec -n ${KDC_NAMESPACE} ${KDC_NAME} -- bash -c "
  export KRB5_CONFIG=/etc/krb5.conf
  kadmin -r ${KRB5_REALM} -p ${KRB5_ADMIN_PRINC_NAME}/admin -w ${KRB5_ADMIN_PRINC_PWD} \
    -q 'addprinc -randkey HTTP/${KEYCLOAK_NAME}.${KEYCLOAK_NAMESPACE}.svc.cluster.local@${KRB5_REALM}'
"

# Create keytab file
echo "Creating keytab file..."
kubectl exec -n ${KDC_NAMESPACE} ${KDC_NAME} -- bash -c "
  export KRB5_CONFIG=/etc/krb5.conf
  kadmin -r ${KRB5_REALM} -p ${KRB5_ADMIN_PRINC_NAME}/admin -w ${KRB5_ADMIN_PRINC_PWD} \
    -q 'ktadd -k /tmp/keycloak.keytab HTTP/${KEYCLOAK_NAME}.${KEYCLOAK_NAMESPACE}.svc.cluster.local@${KRB5_REALM}'
"

# Copy keytab file from KDC pod
echo "Copying keytab file from KDC..."
kubectl cp ${KDC_NAMESPACE}/${KDC_NAME}:/tmp/keycloak.keytab ./keycloak.keytab

# Create Kubernetes secret with the keytab
echo "Creating Kubernetes secret with keytab..."
kubectl create secret generic keycloak-keytab -n ${KEYCLOAK_NAMESPACE} \
  --from-file=keycloak.keytab=./keycloak.keytab \
  --dry-run=client -o yaml | kubectl apply -f -

# Clean up local keytab file
rm -f ./keycloak.keytab

echo "Kerberos keytab setup completed successfully!"