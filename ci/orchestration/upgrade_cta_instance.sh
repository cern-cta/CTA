#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -eo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../utils/log_utils.sh"
source "$(dirname "${BASH_SOURCE[0]}")/../utils/helm_utils.sh"

usage() {
  echo
  echo "Performs a helm upgrade of a running CTA system. For any values not provided, this will reuse the existing values of the deployment."
  echo
  echo "Usage: $0 -n <namespace> [options]"
  echo
  echo "options:"
  echo "  -h, --help:                         Shows help output."
  echo "  -n, --namespace <namespace>:        Specify the Kubernetes namespace."
  echo "      --cta-values <file>:            Consumer values override; repeatable."
  echo "  -r, --cta-image-registry <registry>: CTA image registry (default: reuse existing)."
  echo "  -i, --cta-image-tag <tag>:          The CTA Docker image tag."
  echo "      --force:                        Force redeploy all pods in the CTA chart."
  echo
  exit "${1:-1}"
}

check_helm_installed() {
  # First thing we do is check whether helm is installed
  if ! command -v helm >/dev/null 2>&1; then
    die "Helm does not seem to be installed. To install Helm, see: https://helm.sh/docs/intro/install/"
  fi
}

upgrade_instance() {
  force=0
  consumer_values=()
  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      -h | --help) usage 0 ;;
      --cta-values)
        [[ -f "$2" ]] || die "CTA values file $2 does not exist"
        consumer_values+=(--values "$2")
        shift ;;
      -n|--namespace)
        namespace="$2"
        shift ;;
      -r|--cta-image-registry)
        cta_image_registry="$2"
        shift ;;
      -i|--cta-image-tag)
        cta_image_tag="$2"
        shift ;;
      --force)
        force=1
        ;;
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
  check_helm_installed
  if ! kubectl get namespace "$namespace" >/dev/null 2>&1; then
    die "Namespace $namespace does not exist. Upgrade failed"
  fi


  # Connection overrides and image tags are shared by services and their administration tools.
  shared_flags=()
  cta_flags=()
  if [[ $force == 1 ]]; then
    cta_flags+=(--force)
  fi
  if [[ -n "$cta_image_registry" ]]; then
    shared_flags+=(--set-string "global.image.registry=${cta_image_registry}")
  fi
  if [[ -n "$cta_image_tag" ]]; then
    shared_flags+=(--set-string "global.image.tag=${cta_image_tag}")
  fi
  shared_flags+=("${consumer_values[@]}")

  update_local_cta_chart_dependencies
  echo "Upgrading cta chart..."
  log_run helm upgrade cta helm/cta \
                      --namespace "$namespace" \
                      --wait --timeout 5m \
                      --reuse-values \
                      "${shared_flags[@]}" "${cta_flags[@]}"

  # Upgrades keep the same references without executing any reset scripts.
  log_run helm upgrade cta-tools helm/cta-admin-tools --namespace "$namespace" \
                      --reuse-values --wait --timeout 5m "${shared_flags[@]}" \
                      --set reset.allUnprotected=false --set reset.catalogue=false --set reset.scheduler=false

}

upgrade_instance "$@"
echo "CTA instance in ${namespace} successfully upgraded:"
kubectl --namespace ${namespace} get pods
