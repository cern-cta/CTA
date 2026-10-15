#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -eo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../utils/log_utils.sh"

die() {
  log_error "$@"
  exit 1
}

log_run() {
  log_task "Running: $*"
  "$@"
}

usage() {
  echo
  echo "Deploys EOS to a given namespace. If a deployment already exists, it will try and perform an upgrade."
  echo
  echo "Usage: $0 -n <namespace> [options]"
  echo
  echo "options:"
  echo "  -h, --help:                         Shows help output."
  echo "  -n, --namespace <namespace>:        Specify the Kubernetes namespace."
  echo "  -r, --eos-image-repository <repo>:  The EOS Docker image name."
  echo "  -i, --eos-image-tag <tag>:          The EOS Docker image tag."
  echo "      --eos-config <file>:            Values file to use for the EOS chart."
  echo "      --setup-enabled <true|false>:   Whether to run the setup scripts or not."
  echo
  exit 1
}

check_helm_installed() {
  # First thing we do is check whether helm is installed
  if ! command -v helm >/dev/null 2>&1; then
    die "Helm does not seem to be installed. To install Helm, see: https://helm.sh/docs/intro/install/"
  fi
}

deploy() {
  setup_enabled=true
  eos_config="presets/eos/auth-jwt.yaml"

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      -h | --help) usage ;;
      -n|--namespace)
        namespace="$2"
        shift ;;
      -r|--eos-image-repository)
        eos_image_repository="$2"
        shift ;;
      -i|--eos-image-tag)
        eos_image_tag="$2"
        shift ;;
      --eos-config)
        eos_config="$2"
        shift ;;
      --setup-enabled)
        setup_enabled="$2"
        shift ;;
      *)
        die_usage "Unsupported argument: $1"
        ;;
    esac
    shift
  done

  # Argument checks
  if [[ -z "${namespace}" ]]; then
    die_usage "Missing mandatory argument: -n | --namespace"
  fi
  if ! kubectl get namespace "$namespace" >/dev/null 2>&1; then
    die "Namespace $namespace does not exist. Upgrade failed"
  fi


  helm_flags=(--values "$eos_config")
  if [[ -n "$eos_image_repository" ]]; then
    helm_flags+=(--set-string "server.global.repository=${eos_image_repository}")
  fi
  if [[ -n "$eos_image_tag" ]]; then
    helm_flags+=(--set-string "server.global.tag=${eos_image_tag}")
  fi

  # The pinned OCI dependency does not need HTTP repository index refreshes.
  log_run helm dependency build helm/eos --skip-refresh
  log_run helm upgrade --install eos helm/eos \
                      --namespace "$namespace" \
                      --wait --timeout 5m \
                      "${helm_flags[@]}"

  if [[ "$setup_enabled" == "true" ]]; then
    EOS_MGM_POD=eos-mgm-0
    kubectl cp --namespace "${namespace}" ./setup/configure_eoscta_tape.sh ${EOS_MGM_POD}:/tmp -c eos-mgm
    kubectl exec --namespace "${namespace}" ${EOS_MGM_POD} -c eos-mgm -- /bin/bash -c "chmod +x /tmp/configure_eoscta_tape.sh && /tmp/configure_eoscta_tape.sh"
  fi
}

check_helm_installed
deploy "$@"
log_success "Deployed EOS in namespace ${namespace}."
