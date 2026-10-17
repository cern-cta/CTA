#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -eo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../utils/log_utils.sh"

log_run() {
  log_task "Running: $*"
  "$@"
}

usage() {
  echo
  echo "Deploys dCache to a given namespace. If a deployment already exists, it will try and perform an upgrade."
  echo
  echo "Usage: $0 -n <namespace> [options]"
  echo
  echo "options:"
  echo "  -h, --help:                             Shows help output."
  echo "  -n, --namespace <namespace>:            Specify the Kubernetes namespace."
  echo "  -r, --dcache-image-repository <repo>:   The dCache Docker image repository."
  echo "  -i, --dcache-image-tag <tag>:           The DCache Docker image tag."
  echo "      --dcache-config <file>:             Optional values file overriding the dCache chart defaults."
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
  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      -h | --help) usage ;;
      -n|--namespace)
        namespace="$2"
        shift ;;
      -r|--dcache-image-repository)
        dcache_image_repository="$2"
        shift ;;
      -i|--dcache-image-tag)
        dcache_image_tag="$2"
        shift ;;
      --dcache-config)
        dcache_config="$2"
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


  helm_flags=()
  if [[ -n "$dcache_image_repository" ]]; then
    dcache_image_registry="${dcache_image_repository%%/*}"
    dcache_image_name="${dcache_image_repository#*/}"
    helm_flags+=(--set-string "dcache.image.registry=${dcache_image_registry}"
                 --set-string "dcache.image.repository=${dcache_image_name}")
  fi
  if [[ -n "$dcache_image_tag" ]]; then
    helm_flags+=(--set-string "dcache.image.tag=${dcache_image_tag}")
  fi
  if [[ -n "$dcache_config" ]]; then
    helm_flags+=(--values "$dcache_config")
  fi

  # Dependency update can take a few seconds to do a quick check to see if we can skip it
  if ! helm dependency list helm/dcache | grep -q '[[:space:]]ok[[:space:]]*$'; then
    log_run helm dependency update helm/dcache
  fi
  log_run helm upgrade --install store helm/dcache \
                     --namespace "$namespace" \
                     --wait --timeout 10m \
                     "${helm_flags[@]}"
}

check_helm_installed
deploy "$@"
log_success "Deployed dCache in namespace ${namespace}."
