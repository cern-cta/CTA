#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -e
source "$(dirname "${BASH_SOURCE[0]}")/../utils/log_wrapper.sh"

log_run() {
  echo "Running: $*"
  "$@"
}

usage() {
  echo "Spawns a CTA system using Helm and Kubernetes. Requires access to a kubernetes cluster setup with one or more nodes that have mhvtl installed."
  echo ""
  echo "Usage: $0 -n <namespace> -i <image tag> [options]"
  echo
  echo "options:"
  echo "  -h, --help:                         Shows help output."
  echo "  -n, --namespace <namespace>:        Specify the Kubernetes namespace."
  echo "  -o, --scheduler-config <file>:      Path to the scheduler configuration values file. Defaults to the VFS preset."
  echo "  -d, --catalogue-config <file>:      Path to the catalogue configuration values file. Defaults to the Postgres preset"
  echo "  -r, --cta-image-repository <repo>:  Docker image name for the CTA chart. Defaults to \"gitlab-registry.cern.ch/cta/ctageneric\"."
  echo "  -i, --cta-image-tag <tag>:          Docker image tag for the CTA chart."
  echo "  -c, --catalogue-version <version>:  Set the catalogue schema version. Defaults to the latest version."
  echo "  -O, --reset-scheduler:              Reset scheduler datastore content during initialization phase. Defaults to false."
  echo "  -D, --reset-catalogue:              Reset catalogue content during initialization phase. Defaults to false."
  echo "      --max-drives <n>:               If no tapeservers-config is provided, this will specifiy how many drives to use in the deployment."
  echo "      --dry-run:                      Render the Helm-generated yaml files without touching any existing deployments."
  echo "      --no-setup:                     Skip the setup scripts for EOS and tape resets."
  echo "      --eos-image-tag <tag>:          Docker image tag for the EOS chart."
  echo "      --eos-image-repository <repo>:  Docker image for EOS chart. Should be the full image name, e.g. \"gitlab-registry.cern.ch/dss/eos/eos-ci\"."
  echo "      --eos-config <file>:            Values file to use for the EOS chart. Defaults to presets/dev-eos-xrd-values.yaml."
  echo "      --eos-enabled <true|false>:     Whether to spawn an EOS instance or not. Defaults to true."
  echo "      --dcache-enabled <true|false>:  Whether to spawn a dCache instance or not. Defaults to false."
  echo "      --cta-config <file>:            Values file to use for the CTA chart. Defaults to presets/dev-cta-xrd-values.yaml."
  echo "      --local-telemetry:              Spawns an OpenTelemetry and Collector and Prometheus scraper. Changes the default cta-config to presets/dev-cta-telemetry-values.yaml"
  echo "      --publish-telemetry:            Publishes telemetry to a pre-configured central observability backend. See presets/ci-cta-telemetry-http-values.yaml"
  echo "      --extra-cta-values:            Extra verbatim values for the CTA chart. These will override any previous values from files."
  exit 1
}

# This should all go once we have auto-discovery and auto-scaling of hardware resources
generate_tape_values_files() {
  echo "Auto-generating rmcd config..."
  rmcd_config=$(mktemp "/tmp/${namespace}-rmcd-XXXXXX-values.yaml")
  library_device=$(./../utils/tape/list_libraries_on_host.sh | jq -r .[0].device)
  cat <<EOF > $rmcd_config
rmcd:
  libraryDevice: $library_device
EOF

  echo "---"
  cat "$rmcd_config"
  echo "---"

  echo "Auto-generating taped config..."
  # This file is cleaned up again by delete_instance.sh
  taped_config=$(mktemp "/tmp/${namespace}-taped-XXXXXX-values.yaml")
  drives_json=$(./../utils/tape/list_drives_in_library.sh --library-device "$library_device" --max-drives $max_drives)
  echo "taped:" > $taped_config
  echo "  drives:" >> $taped_config
  echo $drives_json | jq -r '.[] | "    - name: \(.name)\n      device: \(.device)\n      logicalLibraryName: \(.logicalLibraryName)\n      controlPath: \(.controlPath)"' >> $taped_config
  echo "---"
  cat "$taped_config"
  echo "---"
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
  add_trap 'rm -rf "$TEMP_HELM_HOME"' EXIT
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
    "taped"
    "rmcd"
    "maintd"
    "cta"
  )
  for chart in "${charts[@]}"; do
    helm dependency update helm/"$chart" > /dev/null
  done
  unset HELM_CONFIG_HOME
}

create_instance() {
  project_json_path="../../project.json"
  # Argument defaults
  # Not that some arguments below intentionally use false and not 0/1 as they are directly passed as a helm option
  # Note that it is fine for not all of these secrets to exist
  secrets="reg-eoscta-operations reg-ctageneric monit-it-sd-tab-ci-pwd" # Secrets to be copied to the namespace (space separated)
  catalogue_config=presets/dev-catalogue-postgres-values.yaml
  scheduler_config=presets/dev-scheduler-vfs-values.yaml
  cta_config="presets/dev-cta-xrd-values.yaml"
  prometheus_config="presets/dev-prometheus-values.yaml"
  opentelemetry_collector_config="presets/dev-otel-collector-values.yaml"
  # By default keep Database and keep Scheduler datastore data
  # default should not make user loose data if he forgot the option
  reset_catalogue=false
  reset_scheduler=false
  setup_enabled=true
  cta_image_repository=$(jq -r .dev.ctaImageRepository ${project_json_path}) # Used for the ctageneric pod image(s)
  dry_run=0 # Will not do anything with the namespace and just render the generated yaml files
  max_drives=2
  # EOS related
  eos_image_tag=$(jq -r .dev.eosImageTag ${project_json_path})
  eos_image_repository=$(jq -r .dev.eosImageRepository ${project_json_path})
  eos_config=presets/dev-eos-xrd-values.yaml
  eos_enabled=true
  # dCache
  dcache_image_tag=$(jq -r .dev.dCacheImageTag ${project_json_path})
  dcache_config=presets/dev-dcache-values.yaml
  dcache_enabled=false
  # Telemetry
  local_telemetry=false
  publish_telemetry=false

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      -h | --help) usage ;;
      -o|--scheduler-config)
        scheduler_config="$2"
        test -f "${scheduler_config}" || die "Scheduler config file ${scheduler_config} does not exist"
        shift ;;
      -d|--catalogue-config)
        catalogue_config="$2"
        test -f "${catalogue_config}" || die "catalogue config file ${catalogue_config} does not exist"
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
      --max-drives)
        max_drives="$2"
        shift ;;
      -O|--reset-scheduler) reset_scheduler=true ;;
      -D|--reset-catalogue) reset_catalogue=true ;;
      --no-setup) setup_enabled=false ;;
      --local-telemetry) local_telemetry=true ;;
      --dry-run) dry_run=1 ;;
      --eos-config)
        eos_config="$2"
        test -f "${eos_config}" || die "EOS config file ${eos_config} does not exist"
        shift ;;
      --eos-image-repository)
        eos_image_repository="$2"
        shift ;;
      --eos-image-tag)
        eos_image_tag="$2"
        shift ;;
      --eos-enabled)
        eos_enabled="$2"
        shift ;;
      --dcache-enabled)
        dcache_enabled="$2"
        shift ;;
      --cta-config)
        cta_config="$2"
        test -f "${cta_config}" || die "CTA config file ${cta_config} does not exist"
        shift ;;
      --extra-cta-values)
        extra_cta_values="$2"
        shift ;;
      --publish-telemetry) publish_telemetry=true ;;
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
  if [[ -z "${cta_image_tag}" ]]; then
    die_usage "Missing mandatory argument: -i | --cta-image-tag"
  fi
  if [[ -z "${catalogue_schema_version}" ]]; then
    echo "No catalogue schema version provided: using project.json value"
    catalogue_schema_version=$(jq .catalogueVersion ${project_json_path})
  fi

  if [[ "$local_telemetry" == "true" ]] && [[ "$publish_telemetry" == "true" ]]; then
    die "--local-telemetry and --publish-telemetry cannot be active at the same time"
  fi

  if [[ $dry_run == 1 ]]; then
    helm_command="template --debug"
  else
    helm_command="install"
  fi

  if [ "$reset_catalogue" == "true" ] ; then
    echo "Catalogue content will be reset"
  else
    echo "Catalogue content will be kept"
  fi

  if [[ "$reset_scheduler" == "true" ]]; then
    echo "scheduler data store content will be reset"
  else
    echo "scheduler data store content will be kept"
  fi

  # This is where the actual scripting starts. All of the above is just initializing some variables, error checking and producing debug output

  # As we have no nice way of locking resources, just fail if another CTA release already exists
  if [[ $(helm list --all-namespaces | grep cta | wc -l) -ge 1 ]]; then
    die "Another CTA release was found. Currently, installing multiple CTA releases on the same machine is not supported."
  fi

  # Determine the library config to use
  if [[ -z "${tapeservers_config}" ]]; then
    generate_tape_values_files
  fi

  if [[ "$scheduler_config" == "presets/dev-scheduler-vfs-values.yaml" ]]; then
    if kubectl get sc local-path >/dev/null 2>&1; then
      echo "Local path provisioning is enabled. Using VFS scheduler is okay."
    else
      echo "==============================================================================="
      echo "!!!!!!!!DEPRECATION WARNING!!!!!!!!"
      echo "==============================================================================="
      echo
      echo "It seems that your machine does not have local path provisioning enabled"
      echo "Support for running without local path provisioning will be removed soon."
      echo
      echo "Please follow these instructions to enable local path provisioning."
      echo " 1. ssh into your machine as root"
      echo " 2. navigate to the minikube_cta_ci repo you used to instantiate your dev machine"
      echo " 3. Run: git pull"
      echo " 4. Run: ./01_bootstrap_minikube.sh"
      echo " 5. Reboot the machine"
      echo "The storage-provisioner-rancher addon should now be enabled."
      echo "This addon provides dynamic local path provisioning"
      echo
      echo "Alternatively if you prefer to do it manually:"
      echo " 1. Run: minikube addons enable storage-provisioner-rancher"
      echo " 2. Add this same line to /usr/local/bin/start_minikube.sh to ensure it persists over restarts"
      echo
      echo "Changing scheduler config to \"presets/dev-scheduler-vfs-deprecated-values.yaml\"...."
      echo "==============================================================================="
      echo
      echo
      scheduler_config=presets/dev-scheduler-vfs-deprecated-values.yaml
    fi
  fi


  # Create the namespace if necessary
  if [ $dry_run == 0 ] ; then
    echo "Creating ${namespace} namespace"
    kubectl create namespace "${namespace}"
    echo "Copying secrets into ${namespace} namespace"
    for secret_name in ${secrets}; do
      # If the secret exists...
      if kubectl get secret "${secret_name}" &> /dev/null; then
        kubectl get secret "${secret_name}" -o yaml | grep -v '^ *namespace:' | kubectl --namespace "${namespace}" create -f -
      fi
    done
  fi

  update_local_cta_chart_dependencies

  if [ "$local_telemetry" == "true" ] ; then
    echo "Cleaning up clusterroles..."
    kubectl delete clusterrole otel-opentelemetry-collector --ignore-not-found
    kubectl delete clusterrolebinding otel-opentelemetry-collector --ignore-not-found
    kubectl delete clusterrole prometheus-server --ignore-not-found
    kubectl delete clusterrolebinding prometheus-server --ignore-not-found
    kubectl delete clusterrole prometheus-kube-state-metrics --ignore-not-found
    kubectl delete clusterrolebinding prometheus-kube-state-metrics --ignore-not-found
    echo "Installing Telemetry and Prometheus charts..."
    helm repo add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts
    helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
    helm install otel open-telemetry/opentelemetry-collector \
          --namespace "${namespace}" \
          --values "${opentelemetry_collector_config}" \
          --wait --timeout 2m
    helm install prometheus prometheus-community/prometheus \
          --namespace "${namespace}" \
          --values "${prometheus_config}" \
          --wait --timeout 2m
  fi

  # Note that some of these charts are installed in parallel
  # See README.md for details on the order
  echo "Installing Authentication, Catalogue and Scheduler charts..."
  log_run helm ${helm_command} auth helm/auth \
                                --namespace "${namespace}" \
                                --wait --wait-for-jobs --timeout 2m &
  auth_pid=$!

  echo "Deploying with catalogue schema version: ${catalogue_schema_version}"
  log_run helm ${helm_command} cta-catalogue helm/catalogue \
                                --namespace "${namespace}" \
                                --set resetImage.repository="${cta_image_repository}" \
                                --set resetImage.tag="${cta_image_tag}" \
                                --set schemaVersion="${catalogue_schema_version}" \
                                --set resetCatalogue="${reset_catalogue}" \
                                --set-file configuration="${catalogue_config}" \
                                --wait --wait-for-jobs --timeout 4m &
  catalogue_pid=$!

  log_run helm ${helm_command} cta-scheduler helm/scheduler \
                                --namespace "${namespace}" \
                                --set resetImage.repository="${cta_image_repository}" \
                                --set resetImage.tag="${cta_image_tag}" \
                                --set resetScheduler="${reset_scheduler}" \
                                --set-file configuration="${scheduler_config}" \
                                --wait --wait-for-jobs --timeout 4m &
  scheduler_pid=$!

  wait $auth_pid || exit 1

  if [ $eos_enabled == "true" ] ; then
    ./deploy_eos.sh --namespace "${namespace}" \
                    --eos-config "${eos_config}" \
                    --eos-image-repository "${eos_image_repository}" \
                    --eos-image-tag "${eos_image_tag}" \
                    --setup-enabled "${setup_enabled}" &
    eos_pid=$!
  fi

  if [ $dcache_enabled == "true" ] ; then
    ./deploy_dcache.sh --namespace "${namespace}" \
                       --dcache-config "${dcache_config}" \
                       --dcache-image-tag "${dcache_image_tag}" &
    dcache_pid=$!
  fi


  # Wait for the scheduler and catalogue charts to be installed (and exit if 1 failed)
  wait $catalogue_pid || exit 1
  wait $scheduler_pid || exit 1

  extra_cta_chart_flags=""
  if [[ "$local_telemetry" == "true" ]]; then
    extra_cta_chart_flags+=" --values presets/dev-cta-telemetry-values.yaml"
  fi
  if [[ "$publish_telemetry" == "true" ]]; then
    extra_cta_chart_flags+=" --values presets/ci-cta-telemetry-http-values.yaml"
  fi
  # This is a bit hacky, but will be removed when either the objectstore is gone or when I get around to cleaning up all the values files (hopefully soon TM)
  if grep -q "postgres" "$scheduler_config"; then
    extra_cta_chart_flags+=" --values presets/dev-cta-maintd-postgres-values.yaml"

  fi
  if [ "$extra_cta_values" ]; then
    extra_cta_chart_flags+=" ${extra_cta_values} "
  fi


  echo "Installing CTA chart..."
  log_run helm ${helm_command} cta helm/cta \
                                --namespace "${namespace}" \
                                -f "${cta_config}" \
                                --set global.image.repository="${cta_image_repository}" \
                                --set global.image.tag="${cta_image_tag}" \
                                --set-file global.configuration.scheduler="${scheduler_config}" \
                                -f "${taped_config}" \
                                -f "${rmcd_config}" \
                                --wait --timeout 5m ${extra_cta_chart_flags}

  # At this point the disk buffer(s) should also be ready
  if [ $eos_enabled == "true" ] ; then
    wait $eos_pid || exit 1
  fi
  if [ $dcache_enabled == "true" ] ; then
    wait $dcache_pid || exit 1
  fi
  if [[ $dry_run == 1 ]]; then
    exit 0
  fi

  if [[ "$setup_enabled" == "true" ]]; then
    ./setup/reset_tapes.sh -n "${namespace}"
    ./setup/kinit_clients.sh -n "${namespace}"
  fi
}

check_helm_installed
create_instance "$@"
echo "Instance ${namespace} successfully created:"
kubectl --namespace "${namespace}" get pods
