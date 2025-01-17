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
source "$(dirname "${BASH_SOURCE[0]}")/../ci_helpers/log_wrapper.sh"

die() {
  echo "$@" 1>&2
  exit 1
}

log_run() {
  echo "Running: $*"
  "$@"
}

usage() {
  echo "Performs a helm upgrade of a running EOS system. For any values not provided, this will reuse the existing values of the deployment."
  echo ""
  echo "Usage: $0 -n <namespace> [options]"
  echo ""
  echo "options:"
  echo "  -h, --help:                         Shows help output."
  echo "  -n, --namespace <namespace>:        Specify the Kubernetes namespace."
  echo "  -r, --eos-image-repository <repo>:  The EOS Docker image name. Defaults to \"gitlab-registry.cern.ch/dss/eos/eos-ci\"."
  echo "  -i, --eos-image-tag <tag>:          The EOS Docker image tag."
  echo "      --force:                        Force redeploy all pods in the EOS chart."
  exit 1
}

check_helm_installed() {
  # First thing we do is check whether helm is installed
  if ! command -v helm >/dev/null 2>&1; then
    die "Helm does not seem to be installed. To install Helm, see: https://helm.sh/docs/intro/install/"
  fi
}

upgrade_instance() {
  force=0
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
      --force)
        force=1
        ;;
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
  if [ $force == 1 ]; then
    helm_flags+=" --force"
  fi
  if [ -n "$eos_image_repository" ]; then
    helm_flags+=" --set global.repository=${eos_image_repository}"
  fi
  if [ -n "$eos_image_tag" ]; then
    helm_flags+=" --set global.tag=${eos_image_tag}"
  fi

  echo "Upgrading EOS chart..."
  log_run helm upgrade eos oci://registry.cern.ch/eos/charts/server --version 0.2.2-tape \
                      --namespace ${namespace} \
                      --wait --timeout 5m \
                      --reuse-values \
                      ${helm_flags}

}

setup_system() {
  ./setup/configure_eos.sh -n "${namespace}" --mgm-name eos-mgm-0
}

check_helm_installed
upgrade_instance "$@"
setup_system
echo "EOS instance in ${namespace} successfully upgraded:"
kubectl --namespace ${namespace} get pods
