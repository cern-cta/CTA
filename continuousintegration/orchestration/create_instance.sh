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
  echo "Spawns a CTA system using Helm and Kubernetes. Requires a setup according to the Minikube CTA CI repository."
  echo ""
  echo "Usage: $0 -n <namespace> -i <image tag> [options]"
  echo ""
  echo "options:"
  echo "  -h, --help:                         Shows help output."
  echo "  -n, --namespace <namespace>:        Specify the Kubernetes namespace."
  echo "  -o, --scheduler-config <file>:      Path to the scheduler configuration values file. Defaults to the VFS preset."
  echo "  -d, --catalogue-config <file>:      Path to the catalogue configuration values file. Defaults to the Postgres preset"
  echo "      --tapeservers-config <file>:    Path to the tapeservers configuration values file. If not provided, this file will be auto-generated."
  echo "  -i, --image-tag <tag>:              Docker image tag for the deployment."
  echo "  -r, --registry-host <host>:         Provide the Docker registry host. Defaults to \"gitlab-registry.cern.ch/cta\"."
  echo "  -c, --catalogue-version <version>:  Set the catalogue schema version. Defaults to the latest version."
  echo "  -O, --reset-scheduler:              Reset scheduler datastore content during initialization phase. Defaults to false."
  echo "  -D, --reset-catalogue:              Reset catalogue content during initialization phase. Defaults to false."
  echo "      --num-libraries <n>:            If no tapeservers-config is provided, this will specifiy how many different libraries to generate the config for."
  echo "      --max-drives-per-tpsrv <n>:     If no tapeservers-config is provided, this will specifiy how many drives a single tape server (pod) can be responsible for."
  echo "      --max-tapservers <n>:           If no tapeservers-config is provided, this will specifiy the limit of the number of tape servers (pods)."
  echo "      --dry-run:                      Render the Helm-generated yaml files without touching any existing deployments."
  exit 1
}

check_helm_installed() {
  # First thing we do is check whether helm is installed
  if ! command -v helm >/dev/null 2>&1; then
    die "Helm does not seem to be installed. To install Helm, see: https://helm.sh/docs/intro/install/"
  fi
}

update_chart_dependencies() {
  echo "Updating chart dependencies"
  charts=(
    "common"
    "kdc"
    "catalogue"
    "scheduler"
    "cta/"
    "cta/charts/client"
    "cta/charts/ctacli"
    "cta/charts/ctaeos"
    "cta/charts/ctafrontend"
    "cta/charts/tpsrv"
  )
  for chart in "${charts[@]}"; do
    helm dependency update helm/"$chart" > /dev/null
  done
}

create_instance() {
  # Argument defaults
  # Not that some arguments below intentionally use false and not 0/1 as they are directly passed as a helm option
  # Note that it is fine for not all of these secrets to exist; eventually the reg-* format will be how the minikube_cta_ci setup inits things
  registry_secrets="ctaregsecret reg-eoscta-operations reg-ctageneric" # Secrets to be copied to the namespace (space separated)
  catalogue_config=presets/dev-catalogue-postgres-values.yaml
  scheduler_config=presets/dev-scheduler-vfs-values.yaml
  # By default keep Database and keep Scheduler datastore data
  # default should not make user loose data if he forgot the option
  reset_catalogue=false
  reset_scheduler=false
  registry_host="gitlab-registry.cern.ch/cta" # Used for the ctageneric pod image(s)
  dry_run=0 # Will not do anything with the namespace and just render the generated yaml files
  num_library_devices=1 # For the auto-generated tapeservers config
  max_drives_per_tpsrv=1
  max_tapeservers=2

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case $1 in
      -h | --help) usage ;;
      -o|--scheduler-config)
        scheduler_config="$2"
        test -f "${scheduler_config}" || die "Scheduler config file ${scheduler_config} does not exist"
        shift ;;
      -d|--catalogue-config)
        catalogue_config="$2"
        test -f "${catalogue_config}" || die "catalogue config file ${catalogue_config} does not exist"
        shift ;;
      --tapeservers-config)
        tapeservers_config="$2"
        shift ;;
      -n|--namespace)
        namespace="$2"
        shift ;;
      -r|--registry-host)
        registry_host="$2"
        shift ;;
      -i|--image-tag)
        image_tag="$2"
        shift ;;
      -c|--catalogue-version)
        catalogue_schema_version="$2"
        shift ;;
      --num-libraries)
        num_library_devices="$2"
        shift ;;
      --max-drives-per-tpsrv)
        max_drives_per_tpsrv="$2"
        shift ;;
      --max-tapeservers)
        max_tapeservers="$2"
        shift ;;
      -O|--reset-scheduler) reset_scheduler=true ;;
      -D|--reset-catalogue) reset_catalogue=true ;;
      --dry-run) dry_run=1 ;;
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
  if [ -z "${image_tag}" ]; then
    echo "Missing mandatory argument: -i | --image-tag"
    usage
  fi
  if [ -z "${catalogue_schema_version}" ]; then
    echo "No catalogue schema version provided: using latest tag"
    catalogue_major_ver=$(grep CTA_CATALOGUE_SCHEMA_VERSION_MAJOR ../../catalogue/cta-catalogue-schema/CTACatalogueSchemaVersion.cmake | sed 's/[^0-9]*//g')
    catalogue_minor_ver=$(grep CTA_CATALOGUE_SCHEMA_VERSION_MINOR ../../catalogue/cta-catalogue-schema/CTACatalogueSchemaVersion.cmake | sed 's/[^0-9]*//g')
    catalogue_schema_version="$catalogue_major_ver.$catalogue_minor_ver"
  fi

  if [ $dry_run == 1 ]; then
    helm_command="template --debug"
  else
    helm_command="install"
  fi

  if [ "$reset_catalogue" == "true" ] ; then
    echo "Catalogue content will be reset"
  else
    echo "Catalogue content will be kept"
  fi

  if [ "$reset_scheduler" == "true" ]; then
    echo "scheduler data store content will be reset"
  else
    echo "scheduler data store content will be kept"
  fi

  # This is where the actual scripting starts. All of the above is just initializing some variables, error checking and producing debug output

  devices_all=$(./../ci_helpers/tape/list_all_libraries.sh)
  devices_in_use=$(kubectl get all --all-namespaces -l cta/library-device -o jsonpath='{.items[*].metadata.labels.cta/library-device}' | tr ' ' '\n' | sort | uniq)
  unused_devices=$(comm -23 <(echo "$devices_all") <(echo "$devices_in_use"))
  if [ -z "$unused_devices" ]; then
    die "No unused library devices available. All the following libraries are in use: $devices_in_use"
  fi

  # Determine the library config to use
  if [ -z "${tapeservers_config}" ]; then
    echo "Library configuration not provided. Auto-generating..."
    # This file is cleaned up again by delete_instance.sh
    tapeservers_config=$(mktemp /tmp/${namespace}-tapeservers-XXXXXX-values.yaml)
    # Generate a comma separated list of library devices based on the number of library devices the user wants to generate
    library_devices=$(echo "$unused_devices" | head -n "$num_library_devices" | paste -sd ',' -)
    ./../ci_helpers/tape/generate_tapeservers_config.sh --target-file $tapeservers_config  \
                                                        --library-type "mhvtl" \
                                                        --library-devices $library_devices \
                                                        --max-drives-per-tpsrv $max_drives_per_tpsrv \
                                                        --max-tapeservers $max_tapeservers
  else
    # Check that all devices in the provided config are available
    for library_device in $(awk '/libraryDevice:/ {gsub("\"","",$2); print $2}' "$tapeservers_config"); do
      if ! echo "$unused_devices" | grep -qw "$library_device"; then
        die "provided library config specifies a device that is already in use: $library_device"
      fi
    done
  fi
  echo "---"
  cat $tapeservers_config
  echo "---"

  # Create the namespace if necessary
  if [ $dry_run == 0 ] ; then
    echo "Creating ${namespace} namespace"
    kubectl create namespace ${namespace}
    echo "Copying secrets into ${namespace} namespace"
    for secret_name in ${registry_secrets}; do
      # If the secret exists...
      if kubectl get secret ${secret_name} &> /dev/null; then
        kubectl get secret ${secret_name} -o yaml | grep -v '^ *namespace:' | kubectl --namespace ${namespace} create -f -
      else
        echo "Secret ${secret_name} not found. Skipping..."
      fi
    done
  fi

  update_chart_dependencies

  echo "Deploying with catalogue schema version: ${catalogue_schema_version}"
  echo "Installing kdc, catalogue and scheduler charts..."
  log_run helm ${helm_command} catalogue-${namespace} helm/catalogue \
                                --namespace ${namespace} \
                                --set resetImage.registry="${registry_host}" \
                                --set resetImage.tag="${image_tag}" \
                                --set schemaVersion="${catalogue_schema_version}" \
                                --set resetCatalogue=${reset_catalogue} \
                                --set-file configuration=${catalogue_config} \
                                --wait --wait-for-jobs --timeout 4m &
  catalogue_pid=$!

  log_run helm ${helm_command} scheduler-${namespace} helm/scheduler \
                                --namespace ${namespace} \
                                --set resetImage.registry="${registry_host}" \
                                --set resetImage.tag="${image_tag}" \
                                --set resetScheduler=${reset_scheduler} \
                                --set-file configuration=${scheduler_config} \
                                --wait --wait-for-jobs --timeout 4m &
  scheduler_pid=$!

  log_run helm ${helm_command} kdc-${namespace} helm/kdc \
                                --namespace ${namespace} \
                                --set image.registry="${registry_host}" \
                                --set image.tag="${image_tag}" \
                                --wait --wait-for-jobs --timeout 2m &
  kdc_pid=$!

  # Wait for the scheduler and catalogue charts to be installed (and exit if 1 failed)
  wait $catalogue_pid || exit 1
  wait $scheduler_pid || exit 1
  wait $kdc_pid || exit 1

  echo "Installing cta chart..."
  log_run helm ${helm_command} cta-${namespace} helm/cta \
                                --namespace ${namespace} \
                                --set global.image.registry="${registry_host}" \
                                --set global.image.tag="${image_tag}" \
                                --set global.catalogueSchemaVersion=${catalogue_schema_version} \
                                --set-file global.configuration.scheduler=${scheduler_config} \
                                --set-file tpsrv.tapeServers="${tapeservers_config}" \
                                --wait --timeout 8m

  if [ $dry_run == 1 ]; then
    exit 0
  fi
}

setup_system() {
  ./setup/reset_tapes.sh -n ${namespace}
  ./setup/init_kerberos.sh -n ${namespace}
  ./setup/set_eos_workflows.sh -n ${namespace}
}

check_helm_installed
create_instance "$@"
setup_system
echo "Instance ${namespace} successfully created:"
kubectl --namespace ${namespace} get pods
