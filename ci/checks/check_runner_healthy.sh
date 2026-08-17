#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

source "$(dirname "${BASH_SOURCE[0]}")/../utils/log_utils.sh"

errors=0

log_task "Checking that kubectl is installed"
if command -v kubectl >/dev/null 2>&1; then
  log_success "SUCCESS: kubectl is installed"
else
  log_error "ERROR: kubectl does not seem to be installed"
  errors=$((errors + 1))
fi

echo
log_task "Checking that kubectl is working"
if command -v kubectl version --client >/dev/null 2>&1; then
  log_success "SUCCESS: kubectl seems to be working"
else
  log_error "ERROR: kubectl is installed but not working"
  errors=$((errors + 1))
fi

MIN_KUBECTL_VERSION="1.31.0"
echo
log_task "Checking that kubectl client and server versions are at least $MIN_KUBECTL_VERSION"
client_version=$(kubectl version --client -o json | jq -r '.clientVersion.gitVersion' | sed 's/^v//')
server_version=$(kubectl version -o json | jq -r '.serverVersion.gitVersion' | sed 's/^v//')
version_ge() {
  local candidate_version="$1"
  local minimum_version="$2"
  [[ "$(printf '%s\n' "$candidate_version" "$minimum_version" | sort -V | head -n1)" = "$minimum_version" ]]
}
if version_ge "$client_version" "$MIN_KUBECTL_VERSION" && version_ge "$server_version" "$MIN_KUBECTL_VERSION"; then
  log_success "SUCCESS: kubectl client and server versions are >= v$MIN_KUBECTL_VERSION"
else
  log_error "ERROR: kubectl client or server version is < v$MIN_KUBECTL_VERSION"
  errors=$((errors + 1))
fi

echo
log_task "Checking if Helm is installed"
if command -v helm >/dev/null 2>&1; then
  log_success "SUCCESS: Helm is installed"
else
  log_error "ERROR: Helm does not seem to be installed. To install Helm, see: https://helm.sh/docs/intro/install/"
  errors=$((errors + 1))
fi

ctageneric_secret_name="reg-ctageneric"
echo
log_task "Checking if Kubernetes $ctageneric_secret_name is present for pulling of the private CTA images"
if kubectl get secret $ctageneric_secret_name >/dev/null 2>&1; then
  log_success "SUCCESS: Secret $ctageneric_secret_name exists"
else
  log_error "ERROR: Secret $ctageneric_secret_name is not present. Without this, pulling CTA images from the private ctageneric registry is not possible."
  errors=$((errors + 1))
fi

cta_operations_secret_name="reg-eoscta-operations"
echo
log_task "Checking if Kubernetes $cta_operations_secret_name is present for pulling of the private CTA operations images"
if kubectl get secret $cta_operations_secret_name >/dev/null 2>&1; then
  log_success "SUCCESS: Secret $cta_operations_secret_name exists"
else
  log_warn "WARNING: Secret $cta_operations_secret_name is not present. This secret is not necessary for normal workflows, but you will not be able to pull private operation images."
fi

echo
log_task "Checking if a local path provisioner is available"
if kubectl get pods -n local-path-storage -l app=local-path-provisioner 2>/dev/null | grep -q Running; then
  log_success "SUCCESS: Local path provisioning is enabled. Using VFS scheduler is okay."
else
  log_warn "WARNING: Local path provisioning is not available. Using the VFS scheduler will not be possible"
fi

echo
log_task "Checking if mhvtl.target is enabled"
if systemctl is-enabled --quiet mhvtl.target; then
  log_success "SUCCESS: mhvtl.target is enabled"
else
  log_error "ERROR: mhvtl.target is not enabled. Make sure mhvtl is installed and running"
  errors=$((errors + 1))
fi

if uv --version >/dev/null 2>&1; then
  log_success "SUCCESS: uv is available"
else
  log_error "ERROR: uv is not available or not working"
  exit 1
fi

echo
if [[ "${errors}" -gt 0 ]]; then
  log_error "FAILURE: not all conditions were satisfied. The runner is not configured correctly"
  exit 1
fi
log_success "SUCCESS: Runner configured correctly"
