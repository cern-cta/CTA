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
  echo "Deploys dCache to a given namespace. If a deployment already exists, it will try and perform an upgrade."
  echo ""
  echo "Usage: $0 -n <namespace> [options]"
  echo ""
  echo "options:"
  echo "  -h, --help:                             Shows help output."
  echo "  -n, --namespace <namespace>:            Specify the Kubernetes namespace."
  echo "  -i, --dcache-image-tag <tag>:           The EOS Docker image tag."
  echo "      --dcache-config <file>:             Values file to use for the dCache chart. Defaults to presets/dev-dcache-values.yaml."
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
      -i|--eos-image-tag)
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
  if [ -z "${namespace}" ]; then
    echo "Missing mandatory argument: -n | --namespace"
    usage
  fi
  if ! kubectl get namespace "$namespace" >/dev/null 2>&1; then
    die "Namespace $namespace does not exist. Upgrade failed"
  fi


  helm_flags=""
  if [ -n "$dcache_image_tag" ]; then
    helm_flags+=" --set image.tag=${dcache_image_tag}"
  fi
  if [ -n "$dcache_image_tag" ]; then
    helm_flags+=" --values ${dcache_config}"
  fi

  helm repo add bitnami https://charts.bitnami.com/bitnami
  helm repo add dcache https://gitlab.desy.de/api/v4/projects/7648/packages/helm/test
  helm repo update
  log_run helm install -n ${namespace} --replace --wait --timeout 10m0s --set auth.username=dcache --set auth.password=let-me-in --set auth.database=chimera chimera bitnami/postgresql --version=12.12.10
  log_run helm install -n ${namespace} --replace --wait --timeout 10m0s cells bitnami/zookeeper
  log_run helm install -n ${namespace} --replace --wait --timeout 10m0s --set externalZookeeper.servers=cells-zookeeper --set kraft.enabled=false billing bitnami/kafka --version 23.0.7
  log_run helm install -n ${namespace} --debug --replace --wait --timeout 10m0s --set dcache.hsm.enabled=true store dcache/dcache ${helm_flags}
}

check_helm_installed
deploy "$@"
echo "dCache deployment in namespace ${namespace} successful"
