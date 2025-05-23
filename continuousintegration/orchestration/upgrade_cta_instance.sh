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
  echo "Performs a helm upgrade of a running CTA system. For any values not provided, this will reuse the existing values of the deployment."
  echo ""
  echo "Usage: $0 -n <namespace> [options]"
  echo ""
  echo "options:"
  echo "  -h, --help:                         Shows help output."
  echo "  -n, --namespace <namespace>:        Specify the Kubernetes namespace."
  echo "  -o, --scheduler-config <file>:      Path to the scheduler configuration values file."
  echo "      --tapeservers-config <file>:    Path to the tapeservers configuration values file."
  echo "  -r, --cta-image-repository <repo>:  The CTA Docker image name. Defaults to \"gitlab-registry.cern.ch/cta/ctageneric\"."
  echo "  -i, --cta-image-tag <tag>:          The CTA Docker image tag."
  echo "  -c, --catalogue-version <version>:  Set the catalogue schema version."
  echo "      --force:                        Force redeploy all pods in the CTA chart."
  exit 1
}

check_helm_installed() {
  # First thing we do is check whether helm is installed
  if ! command -v helm >/dev/null 2>&1; then
    die "Helm does not seem to be installed. To install Helm, see: https://helm.sh/docs/intro/install/"
  fi
}

update_local_cta_chart_dependencies() {
  # This is a hack to ensure we don't waste 30 seconds updating local dependencies
  # Once helm dependency update gets some performance improvements this can be removed
  TEMP_HELM_HOME=$(mktemp -d)
  trap 'rm -rf "$TEMP_HELM_HOME"' EXIT
  export HELM_CONFIG_HOME="$TEMP_HELM_HOME"

  echo "Updating chart dependencies"
  charts=(
    "common"
    "auth"
    "catalogue"
    "scheduler"
    "client"
    "cli"
    "frontend"
    "tpsrv"
    "cta"
  )
  for chart in "${charts[@]}"; do
    helm dependency update helm/"$chart" > /dev/null
  done
  unset HELM_CONFIG_HOME
}

upgrade_instance() {
  force=0
  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      -h | --help) usage ;;
      -o|--scheduler-config)
        scheduler_config="$2"
        test -f "${scheduler_config}" || die "Scheduler config file ${scheduler_config} does not exist"
        shift ;;
      --tapeservers-config)
        tapeservers_config="$2"
        test -f "${tapeservers_config}" || die "Scheduler config file ${tapeservers_config} does not exist"
        shift ;;
      -n|--namespace)
        namespace="$2"
        shift ;;
      -r|--cta-image-repository)
        cta_image_repository="$2"
        shift ;;
      -i|--cta-image-tag)
        cta_image_tag="$2"
        shift ;;
      -c|--catalogue-version)
        catalogue_schema_version="$2"
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
  if [ -n "$cta_image_repository" ]; then
    helm_flags+=" --set global.image.repository=${cta_image_repository}"
  fi
  if [ -n "$cta_image_tag" ]; then
    helm_flags+=" --set global.image.tag=${cta_image_tag}"
  fi
  if [ -n "$catalogue_schema_version" ]; then
    helm_flags+=" --set global.catalogueSchemaVersion=${catalogue_schema_version}"
  fi
  if [ -n "$scheduler_config" ]; then
    helm_flags+=" --set-file global.configuration.scheduler=${scheduler_config}"
  fi
  if [ -n "$tapeservers_config" ]; then
    devices_in_use=$(kubectl get all --all-namespaces -l cta/library-device -o jsonpath='{.items[*].metadata.labels.cta/library-device}' | tr ' ' '\n' | sort | uniq)
    # Check that all devices in the provided config are already in use
    for library_device in $(awk '/libraryDevice:/ {gsub("\"","",$2); print $2}' "$tapeservers_config"); do
      if ! echo "$devices_in_use" | grep -qw "$library_device"; then
        die "provided library config specifies a device that is not yet in use by the current deployment: $library_device"
      fi
    done
    helm_flags+=" --set-file tpsrv.tapeServers=${tapeservers_config}"
  fi

  update_local_cta_chart_dependencies
  echo "Upgrading cta chart..."
  log_run helm upgrade cta helm/cta \
                      --namespace ${namespace} \
                      --wait --timeout 5m \
                      --reuse-values \
                      ${helm_flags}

}

setup_system() {
  ./setup/init_kerberos.sh -n ${namespace}
}

check_helm_installed
upgrade_instance "$@"
setup_system
echo "CTA instance in ${namespace} successfully upgraded:"
kubectl --namespace ${namespace} get pods
