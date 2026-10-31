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
  echo "      --eos-deploy-timeout <min>:     Helm --timeout for the EOS chart in minutes (default: 5)."
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
  eos_config="presets/eos/auth-jwt.yaml"
  eos_deploy_timeout=5

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
      --eos-deploy-timeout)
        eos_deploy_timeout="$2"
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

  # Dependency update can take a few seconds to do a quick check to see if we can skip it
  if ! helm dependency list helm/eos | grep -q '[[:space:]]ok[[:space:]]*$'; then
    log_run helm dependency update helm/eos --skip-refresh
  fi
  log_run helm upgrade --install eos helm/eos \
                      --namespace "$namespace" \
                      --wait --timeout "${eos_deploy_timeout}m" \
                      "${helm_flags[@]}"
}

check_helm_installed
deploy "$@"
log_success "Deployed EOS in namespace ${namespace}."
