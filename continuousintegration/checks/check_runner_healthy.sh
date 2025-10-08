#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


errors=0

echo "Checking that kubectl is installed"
if command -v kubectl >/dev/null 2>&1; then
  echo "SUCCESS: kubectl is installed"
else
  echo "ERROR: kubectl does not seem to be installed"
  errors=$((errors + 1))
fi

echo
echo "Checking that kubectl is working"
if command -v kubectl version --client >/dev/null 2>&1; then
  echo "SUCCESS: kubectl seems to be working"
else
  echo "ERROR: kubectl is installed but not working"
  errors=$((errors + 1))
fi

MIN_KUBECTL_VERSION="1.31.0"
echo
echo "Checking that kubectl client and server versions are at least $MIN_KUBECTL_VERSION"
client_version=$(kubectl version --client -o json | jq -r '.clientVersion.gitVersion' | sed 's/^v//')
server_version=$(kubectl version -o json | jq -r '.serverVersion.gitVersion' | sed 's/^v//')
version_ge() {
  [ "$(printf '%s\n' "$1" "$2" | sort -V | head -n1)" = "$2" ]
}
if version_ge "$client_version" "$MIN_KUBECTL_VERSION" && version_ge "$server_version" "$MIN_KUBECTL_VERSION"; then
  echo "SUCCESS: kubectl client and server versions are >= v$MIN_KUBECTL_VERSION"
else
  echo "ERROR: kubectl client or server version is < v$MIN_KUBECTL_VERSION"
  errors=$((errors + 1))
fi

echo
echo "Checking if Helm is installed"
if command -v helm >/dev/null 2>&1; then
  echo "SUCCESS: Helm is installed"
else
  echo "ERROR: Helm does not seem to be installed. To install Helm, see: https://helm.sh/docs/intro/install/"
  errors=$((errors + 1))
fi

ctageneric_secret_name="reg-ctageneric"
echo
echo "Checking if Kubernetes $ctageneric_secret_name is present for pulling of the private CTA images"
if kubectl get secret $ctageneric_secret_name >/dev/null 2>&1; then
  echo "SUCCESS: Secret $ctageneric_secret_name exists"
else
  echo "ERROR: Secret $ctageneric_secret_name is not present. Without this, pulling CTA images from the private ctageneric registry is not possible."
  if kubectl get secret ctaregsecret >/dev/null 2>&1; then
    echo "WARNING: an alternative valid secret \"ctaregsecret\" was found. Note that this secret name is deprecated. Please update the runner configuration."
  else
    errors=$((errors + 1))
  fi
fi

cta_operations_secret_name="reg-eoscta-operations"
echo
echo "Checking if Kubernetes $cta_operations_secret_name is present for pulling of the private CTA operations images"
if kubectl get secret $cta_operations_secret_name >/dev/null 2>&1; then
  echo "SUCCESS: Secret $cta_operations_secret_name exists"
else
  echo "WARNING: Secret $cta_operations_secret_name is not present. This secret is not necessary for normal workflows, but you will not be able to pull private operation images."
fi

echo
echo "Checking if a local path provisioner is available"
if kubectl get pods -n local-path-storage -l app=local-path-provisioner 2>/dev/null | grep -q Running; then
  echo "SUCCESS: Local path provisioning is enabled. Using VFS scheduler is okay."
else
  echo "WARNING: Local path provisioning is not available. Using the VFS scheduler will not be possible"
fi

echo
echo "Checking if mhvtl.target is enabled"
if systemctl is-enabled --quiet mhvtl.target; then
  echo "SUCCESS: mhvtl.target is enabled"
else
  echo "ERROR: mhvtl.target is not enabled. Make sure mhvtl is installed and running"
  errors=$((errors + 1))
fi

echo
if [ "${errors}" -gt 0 ]; then
  echo "FAILURE: not all conditions were satisfied. The runner is not configured correctly"
  exit 1
fi
echo "SUCCESS: Runner configured correctly"
