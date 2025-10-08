#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


set -e
source "$(dirname "${BASH_SOURCE[0]}")/../utils/log_wrapper.sh"

die() {
  echo "$@" 1>&2
  exit 1
}

log_run() {
  echo "Running: $*"
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
  echo "      --eos-config <file>:            Values file to use for the EOS chart. Defaults to presets/dev-eos-xrd-values.yaml."
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
  project_json_path="../../project.json"
  eos_server_chart_version=$(jq -r .dev.eosServerChartVersion ${project_json_path})

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
      *)
        echo "Unsupported argument: $1"
        usage
        ;;
    esac
    shift
  done

  # Argument checks
  if [ -z "${namespace}" ]; then
    echo "Missing mandatory argument: -n | --namespace"
    usage
  fi
  if ! kubectl get namespace "$namespace" >/dev/null 2>&1; then
    die "Namespace $namespace does not exist. Upgrade failed"
  fi


  helm_flags=""
  if [ -n "$eos_image_repository" ]; then
    helm_flags+=" --set global.repository=${eos_image_repository}"
  fi
  if [ -n "$eos_image_tag" ]; then
    helm_flags+=" --set global.tag=${eos_image_tag}"
  fi
  if [ -n "$eos_image_tag" ]; then
    helm_flags+=" --values ${eos_config}"
  fi

  log_run helm upgrade --install eos oci://registry.cern.ch/eos/charts/server --version ${eos_server_chart_version} \
                      --namespace ${namespace} \
                      --wait --timeout 5m \
                      --reuse-values \
                      ${helm_flags}

  EOS_MGM_POD=eos-mgm-0
  kubectl cp --namespace "${namespace}" ./setup/configure_eoscta_tape.sh ${EOS_MGM_POD}:/tmp -c eos-mgm
  kubectl exec --namespace "${namespace}" ${EOS_MGM_POD} -c eos-mgm -- /bin/bash -c "chmod +x /tmp/configure_eoscta_tape.sh && /tmp/configure_eoscta_tape.sh"
}

check_helm_installed
deploy "$@"
echo "EOS deployment in namespace ${namespace} successful"
