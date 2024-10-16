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

die() {
  echo "$@" 1>&2
  exit 1
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
  echo "  -l, --library-config <file>:        Path to the library configuration values file. If not provided, this file will be auto-generated."
  echo "  -m, --library-model <model>:        Specify the library model to use. Defaults to mhvtl"
  echo "  -i, --image-tag <tag>:              Docker image tag for the deployment."
  echo "  -r, --registry-host <host>:         Provide the Docker registry host. Defaults to \"gitlab-registry.cern.ch/cta\"."
  echo "  -c, --catalogue-version <version>:  Set the catalogue schema version. Defaults to the latest version."
  echo "      --tpsrv-count <count>:          Set the number of tape servers to spawn. Defaults to 2."
  echo "  -S, --use-systemd:                  Use systemd to manage services inside containers. Defaults to false"
  echo "  -O, --wipe-scheduler:               Wipe scheduler datastore content during initialization phase. Defaults to false."
  echo "  -D, --wipe-catalogue:               Wipe catalogue content during initialization phase. Defaults to false."
  echo "      --upgrade:                      Upgrade the existing deployment."
  echo "      --dry-run:                      Render the Helm-generated yaml files without touching any existing deployments."
  exit 1
}


check_helm_installed() {
  # First thing we do is check whether helm is installed
  if ! command -v helm >/dev/null 2>&1; then
    die "ERROR: Helm does not seem to be installed. To install Helm, see: https://helm.sh/docs/intro/install/"
  fi
}

create_instance() {

  # Argument defaults
  # Not that some arguments below intentionally use false and not 0/1 as they are directly passed as a helm option
  registry_secrets="ctaregsecret" # Secrets to be copied to the namespace (space separated)
  catalogue_config=presets/dev-postgres-catalogue-values.yaml
  scheduler_config=presets/dev-file-scheduler-values.yaml
  library_model="mhvtl"
  # By default keep Database and keep Scheduler datastore data
  # default should not make user loose data if he forgot the option
  wipe_catalogue=false 
  wipe_scheduler=false
  use_systemd=false # By default to not use systemd to manage services inside the containers
  tpsrv_count=2
  registry_host="gitlab-registry.cern.ch/cta" # Used for the ctageneric pod image(s)
  upgrade=0 # Whether to keep the namespace and perform an upgrade of the Helm charts
  dry_run=0 # Will not do anything with the namespace and just render the generated yaml files

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case $1 in
      -h | --help) usage ;;
      -o|--scheduler-config) 
        scheduler_config="$2"
        test -f "${scheduler_config}" || die "ERROR: Scheduler config file ${scheduler_config} does not exist"
        shift ;;
      -d|--catalogue-config) 
        catalogue_config="$2" 
        test -f "${catalogue_config}" || die "ERROR: catalogue config file ${catalogue_config} does not exist"
        shift ;;
      -l|--library-config) 
        library_config="$2"
        shift ;;
      -m|--model) 
        library_model="$2"
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
      --tpsrv-count) 
        tpsrv_count="$2"
        shift ;;
      -c|--catalogue-version) 
        catalogue_schema_version="$2"
        shift ;;
      -S|--use-systemd) use_systemd=true ;;
      -O|--wipe-scheduler) wipe_scheduler=true ;;
      -D|--wipe-catalogue) wipe_catalogue=true ;;
      --upgrade) upgrade=1 ;;
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
  if [ $upgrade == 1 ] && [ -z $library_config ]; then
    echo "You are performing an upgrade, please provide an existing library configuration using: -l | --library-config"
    usage
  fi
  if [ -z "${catalogue_schema_version}" ]; then
    echo "No catalogue schema version provided: using latest tag"
    catalogue_major_ver=$(grep CTA_CATALOGUE_SCHEMA_VERSION_MAJOR ../../catalogue/cta-catalogue-schema/CTACatalogueSchemaVersion.cmake | sed 's/[^0-9]*//g')
    catalogue_minor_ver=$(grep CTA_CATALOGUE_SCHEMA_VERSION_MINOR ../../catalogue/cta-catalogue-schema/CTACatalogueSchemaVersion.cmake | sed 's/[^0-9]*//g')
    catalogue_schema_version="$catalogue_major_ver.$catalogue_minor_ver"
  fi
  if [ "-${library_model}-" != "-ibm-" ] && [ "-${library_model}-" != "-mhvtl-" ] ; then 
    die "ERROR: Library model ${library_model} does not exist\n"
  fi

  if [ $dry_run == 1 ]; then
    helm_command="template --debug"
  elif [ $upgrade == 1 ]; then
    helm_command="install --upgrade"
  else
    helm_command="install"
  fi

  if [ "$wipe_catalogue" == "true" ] ; then
    echo "Catalogue content will be wiped"
  else
    echo "Catalogue content will be kept"
  fi

  if [ "$wipe_scheduler" == "true" ]; then
    echo "scheduler data store content will be wiped"
  else
    echo "scheduler data store content will be kept"
  fi

  # This is where the actual scripting starts. All of the above is just initializing some variables, error checking and producing debug output

  # Grab a unique library
  devices_all=$(lsscsi -g | grep mediumx | awk {'print $7'} | sed -e 's%/dev/%%' | sort)
  devices_in_use=$(kubectl get all --all-namespaces -l cta/library-device -o jsonpath='{.items[*].metadata.labels.cta/library-device}' | tr ' ' '\n' | sort | uniq)
  unused_devices=$(comm -23 <(echo "$devices_all") <(echo "$devices_in_use"))
  if [ -z "$unused_devices" ]; then
    die "ERROR: No unused library devices available. All the following libraries are in use: $devices_in_use"
  fi

  # Determine the library config to use
  if [ -z "${library_config}" ]; then
    echo "Library configuration not provided. Auto-generating..."
    library_config=$(mktemp /tmp/library-XXXXXX-values.yaml)
    # Ensure the temporary file is deleted on script exit or interruption
    trap 'rm -f "$library_config"' EXIT
    library_device=$(echo "$unused_devices" | head -n 1)
    ./../ci_helpers/generate_library_config.sh --target-file $library_config  \
                                               --library-device $library_device \
                                               --library-type $library_model
  else
    # See what device was provided in the config and check that it is not in use
    library_device=$(awk '/device:/ {gsub("\"","",$2); print $2}' $library_config)
    if ! echo "$unused_devices" | grep -qw "$library_device"; then
      die "ERROR: provided library config specifies a device that is already in use: $library_device"
    fi
  fi
  echo "Using library device: ${library_device}"
  echo "---"
  cat $library_config
  echo "---"

  # Create the namespace if necessary
  if [ $upgrade == 0 ] && [ $dry_run == 0 ] ; then
    echo "Creating ${namespace} namespace"
    kubectl create namespace ${namespace}
    # Copy secrets needed for image pulling
    for secret_name in ${registry_secrets}; do
      # If the secret exists...
      if kubectl get secret ${secret_name} &> /dev/null; then
        echo "Copying ${secret_name} secret into ${namespace} namespace"
        kubectl get secret ${secret_name} -o yaml | grep -v '^ *namespace:' | kubectl --namespace ${namespace} create -f -
      else
        echo "Secret ${secret_name} not found"
      fi
    done
  fi

  echo "Installing ci-init chart..."
  set -x
  helm ${helm_command} ci-init helm/ci-init --namespace ${namespace} \
                                            --set global.image.registry="${registry_host}" \
                                            --set global.image.tag="${image_tag}" \
                                            --set-file tapeConfig=${library_config} \
                                            --wait --wait-for-jobs --timeout 2m

  # Technically the catalogue and scheduler charts can be installed in parallel, but let's keep things simple for now
  echo "Installing catalogue chart..."
  echo "Deploying with catalogue schema version: ${catalogue_schema_version}"
  helm ${helm_command} catalogue helm/catalogue --namespace ${namespace} \
                                                --set initImage.registry="${registry_host}" \
                                                --set initImage.tag="${image_tag}" \
                                                --set schemaVersion="${catalogue_schema_version}" \
                                                --set wipeCatalogue=${wipe_catalogue} \
                                                --set-file configuration=${catalogue_config} \
                                                --wait --wait-for-jobs --timeout 2m

  echo "Installing scheduler chart..."
  helm ${helm_command} scheduler helm/scheduler --namespace ${namespace} \
                                                --set initImage.registry="${registry_host}" \
                                                --set initImage.tag="${image_tag}" \
                                                --set wipeScheduler=${wipe_scheduler} \
                                                --set-file configuration=${scheduler_config} \
                                                --wait --wait-for-jobs --timeout 2m
  set +x
  echo ""
  echo "Processing dependencies of cta chart..."
  helm dependency update helm/cta
  echo "Installing cta chart..."
  set -x
  helm ${helm_command} cta helm/cta --namespace ${namespace} \
                                    --set global.image.registry="${registry_host}" \
                                    --set global.image.tag="${image_tag}" \
                                    --set global.useSystemd=${use_systemd} \
                                    --set tpsrv.tpsrv.replicaCount=${tpsrv_count} \
                                    --set-file global.configuration.scheduler=${scheduler_config} \
                                    --set-file tpsrv.tapeConfig="${library_config}" \
                                    --wait --timeout 5m
  set +x

  if [ $dry_run == 1 ] || [ $upgrade == 1 ]; then
    exit 0  
  fi
}

# TODO: the following part is configuration that is not (and should not) be part of the Helm setup.
setup_system() {
  # Eventually, the user should be able to provide a setup script that will be executed here.
  # Set up kerberos
  echo "XrdSecPROTOCOL=krb5,unix" | kubectl --namespace=${namespace} exec -i client -- bash -c "cat >> /etc/xrootd/client.conf"
  echo -n "Using kinit for ctacli and client"
  kubectl --namespace=${namespace} exec ctacli -- kinit -kt /root/ctaadmin1.keytab ctaadmin1@TEST.CTA
  kubectl --namespace=${namespace} exec client -- kinit -kt /root/user1.keytab user1@TEST.CTA
  # space=${namespace} exec -i client -- bash -c "cat >> /etc/xrootd/client.conf"
  # May be needed for the client to make sure that SSS is not used by default but krb5...
  #echo "XrdSecPROTOCOL=krb5,unix" | kubectl --namespace=${namespace} exec -i client -- bash -c "cat >> /etc/xrootd/client.conf"
  echo "klist for client:"
  kubectl --namespace=${namespace} exec client -- klist
  echo "klist for ctacli:"
  kubectl --namespace=${namespace} exec ctacli -- klist


  # Set the workflow rules for archiving, creating tape file replicas in the EOS namespace, retrieving
  # files from tape and deleting files.
  # EOS short instance name
  eos_instance=ctaeos
  echo "Setting workflows in namespace ${namespace} pod ${eos_instance}:"
  cta_workflow_dir=/eos/${eos_instance}/proc/cta/workflow
  for workflow in sync::create.default sync::closew.default sync::archived.default sync::archive_failed.default sync::prepare.default sync::abort_prepare.default sync::evict_prepare.default sync::closew.retrieve_written sync::retrieve_failed.default sync::delete.default; do
    echo "eos attr set sys.workflow.${workflow}=\"proto\" ${cta_workflow_dir}"
    kubectl --namespace=${namespace} exec ${eos_instance} -- bash -c "eos attr set sys.workflow.${workflow}=\"proto\" ${cta_workflow_dir}"
  done

  echo "Instance ${namespace} successfully created:"
  kubectl --namespace=${namespace} get pods
}

check_helm_installed
create_instance "$@"
setup_system