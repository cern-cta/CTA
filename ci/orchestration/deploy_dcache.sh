#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
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
  echo "Deploys dCache to a given namespace. If a deployment already exists, it will try and perform an upgrade."
  echo
  echo "Usage: $0 -n <namespace> [options]"
  echo
  echo "options:"
  echo "  -h, --help:                             Shows help output."
  echo "  -n, --namespace <namespace>:            Specify the Kubernetes namespace."
  echo "  -i, --dcache-image-tag <tag>:           The EOS Docker image tag."
  echo "      --dcache-config <file>:             Values file to use for the dCache chart. Defaults to presets/dev-dcache-values.yaml."
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
  dcache_chart_version=$(jq -r .dev.dCacheChartVersion ${project_json_path})

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      -h | --help) usage ;;
      -n|--namespace)
        namespace="$2"
        shift ;;
      -i|--dcache-image-tag)
        dcache_image_tag="$2"
        shift ;;
      --dcache-config)
        dcache_config="$2"
        shift ;;
      *)
        echo "Unsupported argument: $1"
        usage
        ;;
    esac
    shift
  done

  # Argument checks
  if [[ -z "${namespace}" ]]; then
    echo "Missing mandatory argument: -n | --namespace"
    usage
  fi
  if ! kubectl get namespace "$namespace" >/dev/null 2>&1; then
    die "Namespace $namespace does not exist. Upgrade failed"
  fi


  helm_flags=""
  if [[ -n "$dcache_image_tag" ]]; then
    helm_flags+=" --set image.tag=${dcache_image_tag}"
  fi
  if [[ -n "$dcache_config" ]]; then
    helm_flags+=" --values ${dcache_config}"
  fi

  helm repo add bitnami https://charts.bitnami.com/bitnami
  helm repo add dcache https://gitlab.desy.de/api/v4/projects/7648/packages/helm/test
  helm repo update
  log_run helm install -n ${namespace} chimera bitnami/postgresql \
                                       --replace --wait --timeout 10m0s \
                                       --set auth.username=dcache \
                                       --set auth.password=let-me-in \
                                       --set auth.database=chimera \
                                       --version 12.12.10
  log_run helm install -n ${namespace} cells bitnami/zookeeper \
                                       --replace --wait --timeout 10m0s
  log_run helm install -n ${namespace} billing bitnami/kafka \
                                       --replace --wait --timeout 10m0s \
                                       --set externalZookeeper.servers=cells-zookeeper \
                                       --set kraft.enabled=false \
                                       --version 23.0.7
  log_run helm install -n ${namespace} store dcache/dcache --version ${dcache_chart_version} \
                                       --replace --wait --timeout 10m0s \
                                       --set dcache.hsm.enabled=true \
                                       --values presets/dev-dcache-values.yaml \
                                       ${helm_flags}
}

check_helm_installed
deploy "$@"
echo "dCache deployment in namespace ${namespace} successful"
