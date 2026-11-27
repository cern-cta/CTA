#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -eo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../utils/log_utils.sh"

usage() {
  echo "Spawns a CTA system using Helm and Kubernetes. Requires access to a kubernetes cluster setup with one or more nodes that have mhvtl installed."
  echo ""
  echo "Usage: $0 -n <namespace> -i <image tag> [options]"
  echo
  echo "options:"
  echo "  -h, --help:                         Shows help output."
  echo "  -n, --namespace <namespace>:        Specify the Kubernetes namespace."
  echo "      --ci-setup-config <file>:       CI setup values file. Defaults to the VFS scheduler preset."
  echo "  -o, --scheduler-config <file>:      Existing scheduler profile to overlay on the CI setup values."
  echo "  -d, --catalogue-config <file>:      Existing catalogue profile to overlay on the CI setup values."
  echo "  -r, --cta-image-registry <repo>:    Registry to find the CTA images in. Defaults to \"gitlab-registry.cern.ch\"."
  echo "  -i, --cta-image-tag <tag>:          Docker image tag for the CTA chart."
  echo "  -c, --catalogue-version <version>:  Set the catalogue schema version. Defaults to the latest version."
  echo "      --max-drives <n>:               If no tapeservers-config is provided, this will specifiy how many drives to use in the deployment."
  echo "      --eos-image-tag <tag>:          Docker image tag for the EOS chart."
  echo "      --eos-image-repository <repo>:  Docker image for EOS chart. Should be the full image name, e.g. \"gitlab-registry.cern.ch/dss/eos/eos-ci\"."
  echo "      --eos-config <file>:            Values file to use for the EOS chart."
  echo "      --eos-enabled <true|false>:     Whether to spawn an EOS instance or not. Defaults to true."
  echo "      --dcache-image-repository <repo>: Docker image repository for the dCache chart."
  echo "      --dcache-enabled <true|false>:  Whether to spawn a dCache instance or not. Defaults to false."
  echo "      --cta-config <files>:           Values file(s) to use for the CTA chart. Comma-separated for composition."
  echo "      --chart-install-timeout <min>:  CTA Helm chart installation timeout in minutes."
  echo "      --one-logical-library           Will use only one logical library name for all drives except the default creating one library name for each drive."
  echo "      --local-telemetry:              Spawns an OpenTelemetry and Collector and Prometheus scraper. Changes the default cta-config to presets/dev-cta-telemetry-values.yaml"
  echo "      --publish-telemetry:            Publishes telemetry to a pre-configured central observability backend. See presets/ci-cta-telemetry-values.yaml"
  echo "      --extra-cta-values:             Extra verbatim values for the CTA chart. These will override any previous values from files."
  exit 1
}

check_deploy_commands() {
  local errors=0 required_command
  for required_command in jq kubectl helm lsscsi sg_inq; do
    if ! command -v "$required_command" >/dev/null 2>&1; then
      log_error "Deploy requirement missing: $required_command"
      errors=$((errors + 1))
    fi
  done
  (( errors == 0 )) || die "Deployment requirements are not satisfied."
}

# This should all go once we have auto-discovery and auto-scaling of hardware resources
generate_tape_values_files() {
  local library_device drives_json
  log_task "Generating rmcd configuration..."
  rmcd_config=$(mktemp "/tmp/${namespace}-rmcd-XXXXXX-values.yaml")
  library_device=$(
    ./../utils/tape/list_libraries_on_host.sh | jq -r .[0].device || \
    die "Couldn't find any tape libraries on this host. Make sure mhvtl is installed and running, using \`systemctl status mhvtl.target\` as root."
  )
  [[ -n $library_device && $library_device != null ]] || \
    die "Couldn't find any tape libraries on this host. Make sure mhvtl is installed and running."
  cat <<EOF > "$rmcd_config"
rmcd:
  libraryDevice: $library_device
EOF

  echo "Content of rmcd values file $rmcd_config:"
  echo
  cat "$rmcd_config"
  echo

  log_task "Generating taped configuration..."
  # This file is cleaned up again by delete_instance.sh
  taped_config=$(mktemp "/tmp/${namespace}-taped-XXXXXX-values.yaml")

  local drives_json_args=(
    --library-device "$library_device"
    --max-drives "$max_drives"
  )
  if [[ "$one_logical_library" = true ]]; then
    drives_json_args+=(-l)
  fi

  drives_json=$(./../utils/tape/list_drives_in_library.sh "${drives_json_args[@]}") || \
    die "Could not inspect drives for tape library '$library_device'."
  [[ $drives_json == *'"device"'* ]] || \
    die "No tape drives were detected for tape library '$library_device'."

  echo "taped:" > "$taped_config"
  echo "  drives:" >> "$taped_config"
  jq -r '.[] | "    - name: \(.name)\n      device: \(.device)\n      logicalLibraryName: \(.logicalLibraryName)\n      controlPath: \(.controlPath)"' \
    <<< "$drives_json" >> "$taped_config"
  echo "Content of taped values file $taped_config:"
  echo
  cat "$taped_config"
  echo
}

update_local_cta_chart_dependencies() {
  # Local dependencies need no remote repositories; skip both index loading and refreshes.

  log_task "Updating Helm chart dependencies..."
  charts=(
    "common"
    "ci-setup"
    "cli"
    "frontend-wfe"
    "frontend-admin"
    "taped"
    "rmcd"
    "maintd"
    "cta"
  )
  for chart in "${charts[@]}"; do
    helm dependency update helm/"$chart" --skip-refresh --repository-config /dev/null > /dev/null
  done
}

create_instance() {
  check_deploy_commands

  project_json_path="../../project.json"
  # Argument defaults
  # Not that some arguments below intentionally use false and not 0/1 as they are directly passed as a helm option
  ci_setup_config=presets/dev-ci-setup-vfs-scheduler-values.yaml
  catalogue_config=""
  scheduler_config=""
  cta_config="-f presets/dev-cta-common.yaml -f presets/frontend-wfe/auth-jwt.yaml -f presets/frontend-admin/auth-jwt.yaml"
  prometheus_config="presets/dev-prometheus-values.yaml"
  opentelemetry_collector_config="presets/dev-otel-collector-values.yaml"
  cta_image_registry=$(jq -r .dev.ctaImageRegistry ${project_json_path})
  max_drives=2
  # EOS related
  eos_image_tag=$(jq -r .dev.eosImageTag ${project_json_path})
  eos_image_repository=$(jq -r .dev.eosImageRepository ${project_json_path})
  eos_config=presets/eos/auth-jwt.yaml
  eos_enabled=true
  # dCache
  dcache_image_repository=$(jq -r .dev.dCacheImageRepository ${project_json_path})
  dcache_image_tag=$(jq -r .dev.dCacheImageTag ${project_json_path})
  dcache_enabled=false
  # CTA chart timeout
  chart_install_timeout=5
  # Telemetry
  local_telemetry=false
  publish_telemetry=false
  one_logical_library=false

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      -h | --help) usage ;;
      --ci-setup-config)
        ci_setup_config="$2"
        [[ -f "${ci_setup_config}" ]] || die "CI setup config file ${ci_setup_config} does not exist"
        shift ;;
      -o|--scheduler-config)
        scheduler_config="$2"
        [[ -f "${scheduler_config}" ]] || die "Scheduler config file ${scheduler_config} does not exist"
        shift ;;
      -d|--catalogue-config)
        catalogue_config="$2"
        [[ -f "${catalogue_config}" ]] || die "catalogue config file ${catalogue_config} does not exist"
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
      -c|--catalogue-version)
        catalogue_schema_version="$2"
        shift ;;
      --max-drives)
        max_drives="$2"
        shift ;;
      --local-telemetry) local_telemetry=true ;;
      --eos-config)
        eos_config="$2"
        [[ -f "${eos_config}" ]] || die "EOS config file ${eos_config} does not exist"
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
      --dcache-image-repository)
        dcache_image_repository="$2"
        shift ;;
      --cta-config)
        cta_config=""
        IFS=',' read -ra configs <<< "$2"
        for config in "${configs[@]}"; do
          # Trim whitespace from config path
          config="${config#"${config%%[![:space:]]*}"}"
          config="${config%"${config##*[![:space:]]}"}"
          cta_config+=" -f ${config}"
        done
        shift ;;
      --extra-cta-values)
        extra_cta_values="$2"
        shift ;;
      --chart-install-timeout)
        chart_install_timeout="$2"
        shift ;;
      --publish-telemetry) publish_telemetry=true ;;
      --one-logical-library) one_logical_library=true ;;
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
    catalogue_schema_version="latest"
  fi
  echo "Catalogue schema version: ${catalogue_schema_version}"

  if [[ "$local_telemetry" == "true" ]] && [[ "$publish_telemetry" == "true" ]]; then
    die "--local-telemetry and --publish-telemetry cannot be active at the same time"
  fi

  SECONDS=0

  # This is where the actual scripting starts. All of the above is just initializing some variables, error checking and producing debug output

  # As we have no nice way of locking resources, just fail if another CTA release already exists
  if [[ $(helm list --all-namespaces | grep cta | wc -l) -ge 1 ]]; then
    die "Another CTA release was found. Currently, installing multiple CTA releases on the same machine is not supported."
  fi

  # Determine the library config to use
  if [[ -z "${tapeservers_config}" ]]; then
    generate_tape_values_files
  fi

  # Resolve setup and daemon values together before changing the cluster.
  local setup_python scheduler_backend
  local setup_profile_args=()
  setup_values_dir=$(mktemp -d)
  trap 'rm -rf -- "$setup_values_dir"' EXIT
  setup_python="${UV_PROJECT_ENVIRONMENT:-../../.venv}/bin/python"
  [[ -x "$setup_python" ]] || setup_python=python3
  [[ -n "$catalogue_config" ]] && setup_profile_args+=(--catalogue-config "$catalogue_config")
  [[ -n "$scheduler_config" ]] && setup_profile_args+=(--scheduler-config "$scheduler_config")
  scheduler_backend=$("$setup_python" ./prepare_setup_values.py \
    --ci-setup-config "$ci_setup_config" \
    --output-dir "$setup_values_dir" \
    "${setup_profile_args[@]}")

  # Create the namespace
  log_task "Creating namespace ${namespace}..."
  kubectl create namespace "${namespace}"
  kubectl label namespace "$namespace" "app.kubernetes.io/managed-by=cta-create-instance"
  log_task "Copying secrets into namespace ${namespace}..."
  # Setup values may reference additional connection or authentication Secrets from the source namespace.
  secrets=$(kubectl get secrets -o json | jq -r '.items[].metadata.name')
  for secret_name in ${secrets}; do
    # If the secret exists...
    if kubectl get secret "${secret_name}" &> /dev/null; then
      kubectl get secret "${secret_name}" -o yaml | grep -v '^ *namespace:' | kubectl --namespace "${namespace}" create -f -
    fi
  done

  update_local_cta_chart_dependencies

  log_task "Starting deployment..."

  if [[ "$local_telemetry" == "true" ]]; then
    log_task "Installing telemetry and Prometheus charts..."
    helm repo add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts
    helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
    # Keep the chart compatible with the collector image pinned in the default values.
    helm install otel open-telemetry/opentelemetry-collector \
          --version 0.128.0 \
          --namespace "${namespace}" \
          --values "${opentelemetry_collector_config}" \
          --wait --timeout 2m
    helm install prometheus prometheus-community/prometheus \
          --namespace "${namespace}" \
          --values "${prometheus_config}" \
          --wait --timeout 2m
  fi

  # Disk buffers need both the generated credentials and a responsive KDC.
  log_task "Installing CI setup chart..."
  log_run helm upgrade --install ci-setup helm/ci-setup \
                                --namespace "${namespace}" \
                                -f "${setup_values_dir}/setup-values.json" \
                                --set catalogueReset.image.registry="${cta_image_registry}" \
                                --set catalogueReset.image.tag="${cta_image_tag}" \
                                --set catalogueReset.schemaVersion="${catalogue_schema_version}" \
                                --set schedulerReset.image.registry="${cta_image_registry}" \
                                --set schedulerReset.image.tag="${cta_image_tag}" \
                                --wait --wait-for-jobs --timeout 4m

  if [[ $eos_enabled == "true" ]]; then
    ./deploy_eos.sh --namespace "${namespace}" \
                    --eos-config "${eos_config}" \
                    --eos-image-repository "${eos_image_repository}" \
                    --eos-image-tag "${eos_image_tag}" &
    eos_pid=$!
  fi

  if [[ $dcache_enabled == "true" ]]; then
    ./deploy_dcache.sh --namespace "${namespace}" \
                       --dcache-image-repository "${dcache_image_repository}" \
                       --dcache-image-tag "${dcache_image_tag}" &
    dcache_pid=$!
  fi


  extra_cta_chart_flags=""
  if [[ "$dcache_enabled" == "true" ]]; then
    # The dCache CTA nearline-storage driver only supports mTLS.
    extra_cta_chart_flags+=" --values presets/frontend-wfe/auth-mtls.yaml"
  fi
  if [[ "$local_telemetry" == "true" ]]; then
    extra_cta_chart_flags+=" --values presets/dev-cta-telemetry-values.yaml"
  fi
  if [[ "$publish_telemetry" == "true" ]]; then
    extra_cta_chart_flags+=" --values presets/ci-cta-telemetry-values.yaml"
  fi
  if [[ "$scheduler_backend" == "postgres" ]]; then
    extra_cta_chart_flags+=" --values presets/dev-cta-maintd-postgres-values.yaml"
  else
    extra_cta_chart_flags+=" --values presets/dev-cta-maintd-objectstore-values.yaml"
  fi
  if [[ "$extra_cta_values" ]]; then
    extra_cta_chart_flags+=" ${extra_cta_values} "
  fi


  log_run helm upgrade --install cta helm/cta \
                                --namespace "${namespace}" \
                                -f "${setup_values_dir}/cta-values.json" \
                                ${cta_config} \
                                --set global.image.registry="${cta_image_registry}" \
                                --set global.image.tag="${cta_image_tag}" \
                                -f "${taped_config}" \
                                -f "${rmcd_config}" \
                                --wait --timeout "${chart_install_timeout}"m ${extra_cta_chart_flags}
  log_success "Deployed CTA in namespace ${namespace}."

  # At this point the disk buffer(s) should also be ready
  if [[ $eos_enabled == "true" ]]; then
    wait $eos_pid || exit 1
  fi
  if [[ $dcache_enabled == "true" ]]; then
    wait $dcache_pid || exit 1
  fi

  echo
  echo "Deployed pods:"
  kubectl --namespace "${namespace}" get pods
  echo
  log_success "Deployed CTA instance ${namespace} in ${SECONDS} seconds."
}

create_instance "$@"
