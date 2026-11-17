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
  echo "  -o, --scheduler-config <file>:      Path to the scheduler configuration values file. Defaults to the VFS preset."
  echo "  -d, --catalogue-config <file>:      Path to the catalogue configuration values file. Defaults to the Postgres preset"
  echo "  -r, --cta-image-registry <repo>:    Registry to find the CTA images in. Defaults to \"gitlab-registry.cern.ch\"."
  echo "  -i, --cta-image-tag <tag>:          Docker image tag for the CTA chart."
  echo "  -c, --catalogue-version <version>:  Set the catalogue schema version. Defaults to the latest version."
  echo "  -O, --reset-scheduler:              Reset scheduler content during initialization phase. Defaults to false."
  echo "  -D, --reset-catalogue:              Reset catalogue content during initialization phase. Defaults to false."
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
  # Only infrastructure has remote dependencies; the remaining charts use local sources.

  log_task "Updating Helm chart dependencies..."
  helm dependency update helm/ci-infra --repository-config /dev/null > /dev/null
  charts=(
    "common"
    "cta-admin-tools"
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

# Only requested resets run; a failed reset prevents CTA services from starting.
prepare_cta_databases() {
  local namespace=$1 reset_catalogue=$2 reset_scheduler=$3 schema_version=$4
  if [[ "$reset_catalogue" == true ]]; then
    log_run kubectl -n "$namespace" exec deployment/cta-tools-admin -c admin -- \
      env SCHEMA_VERSION="$schema_version" \
      timeout 240 bash /scripts/reset_catalogue.sh || return 1
  fi
  if [[ "$reset_scheduler" == true ]]; then
    log_run kubectl -n "$namespace" exec deployment/cta-tools-admin -c admin -- \
      timeout 240 bash /scripts/reset_scheduler.sh || return 1
  fi
}

create_instance() {
  check_deploy_commands

  project_json_path="../../project.json"
  # Argument defaults
  # Not that some arguments below intentionally use false and not 0/1 as they are directly passed as a helm option
  # Note that it is fine for not all of these secrets to exist
  # Copy available shared credentials and runner connections into the deployment namespace.
  secrets="reg-eoscta-operations reg-ctageneric monit-it-sd-tab-ci-pwd monit-it-sd-tab-ci-credentials"
  secrets+=" cta-oracle-catalogue-conf cta-ceph-scheduler-conf cta-stress-scheduler-conf"
  catalogue_config=presets/dev-catalogue-postgres-values.yaml
  scheduler_config=presets/dev-scheduler-vfs-values.yaml
  cta_config="-f presets/dev-cta-common.yaml -f presets/frontend-wfe/auth-jwt.yaml -f presets/frontend-admin/auth-jwt.yaml"
  # By default keep the catalogue and keep the scheduler
  # default should not make user loose data if he forgot the option
  reset_catalogue=false
  reset_scheduler=false
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
      -O|--reset-scheduler) reset_scheduler=true ;;
      -D|--reset-catalogue) reset_catalogue=true ;;
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

  if [[ "$reset_catalogue" == "true" ]]; then
    log_warn "Catalogue content will be reset."
  else
    echo "Catalogue will be left unchanged."
  fi

  if [[ "$reset_scheduler" == "true" ]]; then
    log_warn "Scheduler content will be reset."
  else
    echo "Scheduler will be left unchanged."
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

  if [[ "$scheduler_config" == "presets/dev-scheduler-vfs-values.yaml" ]]; then
    kubectl get sc local-path >/dev/null 2>&1 ||
      die "The default VFS preset requires the local-path StorageClass. Enable the provisioner or select another preset."
  fi

  # Create the namespace
  log_task "Creating namespace ${namespace}..."
  kubectl create namespace "${namespace}"
  kubectl label namespace "$namespace" "app.kubernetes.io/managed-by=cta-create-instance"
  log_task "Copying secrets into namespace ${namespace}..."
  for secret_name in ${secrets}; do
    # If the secret exists...
    if kubectl --namespace default get secret "${secret_name}" &> /dev/null; then
      kubectl --namespace default get secret "${secret_name}" -o json |
        jq --arg namespace "$namespace" \
          '{apiVersion, kind, type, data, metadata: {name: .metadata.name, namespace: $namespace}}' |
        kubectl --namespace "${namespace}" create -f -
    fi
  done

  update_local_cta_chart_dependencies

  log_task "Starting deployment..."

  # Presets request infrastructure resources; the chart itself does not select application backends.
  infra_flags=()
  if [[ "$local_telemetry" == true ]]; then
    infra_flags+=(--values presets/dev-telemetry-infra-values.yaml)
  fi
  if [[ "$dcache_enabled" == true ]]; then
    infra_flags+=(--values presets/dev-dcache-infra-values.yaml)
  fi

  # Provision shared services first; schema operations belong to a separate tools release.
  log_run helm upgrade --install ci-infra helm/ci-infra \
                                --namespace "${namespace}" \
                                "${infra_flags[@]}" \
                                --values "${catalogue_config}" \
                                --values "${scheduler_config}" \
                                --timeout 4m

  consumer_values=$(mktemp)
  trap 'rm -f "$consumer_values"' EXIT
  helm get values ci-infra --namespace "$namespace" --all --output json | \
    jq '{global: .global, maintd: (.maintd // {})}' > "$consumer_values"
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
  if [[ "$extra_cta_values" ]]; then
    extra_cta_chart_flags+=" ${extra_cta_values} "
  fi

  # Install once: both the API client and direct database tools remain available after bootstrap.
  log_run helm install cta-tools helm/cta-admin-tools \
                       --namespace "${namespace}" \
                       ${cta_config} \
                       --values "$consumer_values" \
                       --set database.enabled=true --set client.enabled=true \
                       --set global.image.registry="${cta_image_registry}" \
                       --set global.image.tag="${cta_image_tag}" \
                       --wait --timeout 5m ${extra_cta_chart_flags}

  # The tools now mount the VFS PVC, so Helm can wait for infrastructure without a binding deadlock.
  log_run helm upgrade ci-infra helm/ci-infra --namespace "$namespace" \
                       --reuse-values --wait --wait-for-jobs --timeout 4m
  prepare_cta_databases "$namespace" "$reset_catalogue" "$reset_scheduler" "$catalogue_schema_version"

  if [[ $eos_enabled == "true" ]]; then
    ./deploy_eos.sh --namespace "${namespace}" \
                    --eos-config "${eos_config}" \
                    --eos-image-repository "${eos_image_repository}" \
                    --eos-image-tag "${eos_image_tag}" &
    eos_pid=$!
  fi

  if [[ $dcache_enabled == "true" ]]; then
    dcache_database_secret=$(helm get values ci-infra --namespace "$namespace" --all --output json | \
      jq -r .dcache.databaseSecret)
    ./deploy_dcache.sh --namespace "${namespace}" \
                       --database-secret "$dcache_database_secret" \
                       --dcache-image-repository "${dcache_image_repository}" \
                       --dcache-image-tag "${dcache_image_tag}" &
    dcache_pid=$!
  fi

  log_run helm upgrade --install cta helm/cta \
                                --namespace "${namespace}" \
                                ${cta_config} \
                                --set global.image.registry="${cta_image_registry}" \
                                --set global.image.tag="${cta_image_tag}" \
                                --values "$consumer_values" \
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
