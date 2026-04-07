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
  echo "      --ci-setup-config <file>:       Path to the scheduler configuration values file."
  echo "  -r, --cta-image-repository <repo>:  Docker image name for the CTA chart. Defaults to \"gitlab-registry.cern.ch/cta/ctageneric\"."
  echo "  -i, --cta-image-tag <tag>:          Docker image tag for the CTA chart."
  echo "      --catalogue-version <version>:  Set the catalogue schema version. Defaults to the latest version."
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
  echo "      --publish-telemetry:            Publishes telemetry to a pre-configured central observability backend. See presets/ci-cta-telemetry-values.yaml"
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

update_local_cta_chart_dependencies() {
  # This is a hack to ensure we don't waste 30 seconds updating local dependencies
  # Once helm dependency update gets some performance improvements this can be removed
  TEMP_HELM_HOME=$(mktemp -d)
  add_trap 'rm -rf "$TEMP_HELM_HOME"' EXIT
  export HELM_CONFIG_HOME="$TEMP_HELM_HOME"

  echo "Updating chart dependencies"
  charts=(
    "common"
    "ci-setup"
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
  ci_setup_config=presets/dev-ci-setup-vfs-scheduler-values.yaml
  cta_config="presets/dev-cta-xrd-values.yaml"
  prometheus_config="presets/dev-prometheus-values.yaml"
  opentelemetry_collector_config="presets/dev-otel-collector-values.yaml"
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
      --ci-setup-config)
        ci_setup_config="$2"
        test -f "${ci_setup_config}" || die "ci-setup config file ${ci_setup_config} does not exist"
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
      --catalogue-version)
        catalogue_schema_version="$2"
        shift ;;
      --max-drives)
        max_drives="$2"
        shift ;;
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

  # This is where the actual scripting starts. All of the above is just initializing some variables, error checking and producing debug output

  # As we have no nice way of locking resources (yet), just fail if another CTA release already exists
  if [[ $(helm list --all-namespaces | grep cta | wc -l) -ge 1 ]]; then
    die "Another CTA release was found. Currently, installing multiple CTA releases on the same machine is not supported."
  fi

  # Determine the library config to use
  if [[ -z "${tapeservers_config}" ]]; then
    generate_tape_values_files
  fi

  # Create the namespace if necessary
  if [ $dry_run == 0 ] ; then
    echo "Creating ${namespace} namespace"
    kubectl create namespace "${namespace}"
    echo "Copying secrets into ${namespace} namespace"
    secrets=$(kubectl get secrets -o json | jq -r '.items[].metadata.name')
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
  echo "Installing CI Setup chart..."
  echo "Deploying with catalogue schema version: ${catalogue_schema_version}"
  log_run helm ${helm_command} ci-setup helm/ci-setup \
                                --namespace "${namespace}" \
                                --set catalogueReset.image.repository="${cta_image_repository}" \
                                --set catalogueReset.image.tag="${cta_image_tag}" \
                                --set catalogueReset.schemaVersion="${catalogue_schema_version}" \
                                --set schedulerReset.image.repository="${cta_image_repository}" \
                                --set schedulerReset.image.tag="${cta_image_tag}" \
                                -f ${ci_setup_config} \
                                --wait --wait-for-jobs --timeout 4m

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

  extra_cta_chart_flags=""
  if [[ "$local_telemetry" == "true" ]]; then
    extra_cta_chart_flags+=" --values presets/dev-cta-telemetry-values.yaml"
  fi
  if [[ "$publish_telemetry" == "true" ]]; then
    extra_cta_chart_flags+=" --values presets/ci-cta-telemetry-values.yaml"
  fi
  # This is a bit hacky, but will be removed when either the objectstore is gone or when I get around to cleaning up all the values files (hopefully soon TM)
  if grep -q "postgres" "$scheduler_config"; then
    extra_cta_chart_flags+=" --values presets/dev-cta-maintd-postgres-values.yaml"
  else
    extra_cta_chart_flags+=" --values presets/dev-cta-maintd-objectstore-values.yaml"
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

  # Can be removed once all system tests use the Python test framework
  if [[ "$setup_enabled" == "true" ]]; then
    ./setup/reset_tapes.sh -n "${namespace}"
    ./setup/kinit_clients.sh -n "${namespace}"
  fi
}

create_instance "$@"
echo "Instance ${namespace} successfully created:"
kubectl --namespace "${namespace}" get pods
