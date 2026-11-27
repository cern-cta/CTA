#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -eo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../utils/log_utils.sh"
source "$(dirname "${BASH_SOURCE[0]}")/../utils/helm_utils.sh"

usage() {
  cat <<EOF
Deploy a CTA system using Helm and Kubernetes.
Requires a Kubernetes cluster and tape hardware or mhvtl.

Usage: $0 --namespace <namespace> --cta-image-tag <tag> [options]

General:
  -h, --help                             Show this help.
  -n, --namespace <namespace>            Kubernetes namespace (required).
      --chart-install-timeout <min>      CTA installation timeout (default: 5 minutes).

CTA and infrastructure:
  -r, --cta-image-registry <registry>    CTA image registry (default: dev.ctaImageRegistry).
  -i, --cta-image-tag <tag>              CTA image tag (required).
      --cta-values <file>                Helm values for CTA services and administration tools (repeatable).
      --catalogue-secret <name>          Read catalogue connection settings from cta-catalogue.conf
                                         in Kubernetes Secret <name>.
      --scheduler-secret <name>          Read scheduler connection settings from cta-scheduler.conf
                                         in Kubernetes Secret <name>.
      --copy-secret <name>               Copy an additional Kubernetes Secret into the deployment
                                         namespace (repeatable).
      --infra-values <file>              Helm values for local infrastructure, replacing the
                                         default presets (repeatable).
      --local-telemetry                  Deploy a local collector and Prometheus.

Database initialization:
  -c, --catalogue-version <version>      Catalogue schema version to create on reset (default: latest).
      --reset-catalogue                  Initialize/reset the selected catalogue.
      --reset-scheduler                  Initialize/reset the selected scheduler.

  No connections are reset unless a reset option is supplied.
  Resets delete existing data and reinitialize the selected catalogue or scheduler.

Tape configuration:
      --max-drives <n>                   Maximum number of discovered drives to use (default: 2).
      --one-logical-library              Use one logical library for all drives.

EOS:
      --eos-enabled <true|false>         Deploy EOS (default: true).
      --eos-image-repository <repo>      Full EOS image repository (default: dev.eosImageRepository).
      --eos-image-tag <tag>              EOS image tag (default: dev.eosImageTag).
      --eos-values <file>                Helm values for EOS (repeatable).

dCache:
      --dcache-enabled <true|false>      Deploy dCache (default: false).
      --dcache-image-repository <repo>   Full dCache image repository (default: dev.dCacheImageRepository).
      --dcache-image-tag <tag>           dCache image tag (default: dev.dCacheImageTag).
      --dcache-values <file>             Helm values for dCache (repeatable).

Connection Secrets:
  Secrets named by --catalogue-secret, --scheduler-secret and --copy-secret must
  already exist in the Kubernetes namespace 'default'; they are copied into the deployment namespace.
  For example, create a catalogue Secret from your connection file:
    kubectl -n default create secret generic my-catalogue --from-file=cta-catalogue.conf=/path/to/cta-catalogue.conf
  Then pass --catalogue-secret my-catalogue when deploying.
EOF
  exit "${1:-1}"
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

create_instance() {
  # Argument defaults
  # Copy available registry credentials into the deployment namespace.
  local secrets=(reg-eoscta-operations reg-ctageneric)
  local copy_secrets=()
  local catalogue_secret="" scheduler_secret=""
  local -A required_secrets=()
  # Chart defaults select the PostgreSQL catalogue and VFS scheduler connections.
  cta_config=(--values presets/cta/base.yaml --values presets/cta/auth/frontend-wfe-jwt.yaml
              --values presets/cta/auth/frontend-admin-jwt.yaml)
  cta_overrides=()
  infra_config=()
  infra_config_explicit=false
  # Reset database content only when explicitly requested.
  reset_catalogue=false
  reset_scheduler=false
  max_drives=2
  # EOS related
  eos_values=()
  eos_enabled=true
  # dCache
  dcache_enabled=false
  dcache_values=()
  # CTA chart timeout
  chart_install_timeout=5
  # Telemetry
  local_telemetry=false
  one_logical_library=false

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      -h | --help) usage 0 ;;
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
      --reset-scheduler) reset_scheduler=true ;;
      --reset-catalogue) reset_catalogue=true ;;
      --local-telemetry) local_telemetry=true ;;
      --eos-values)
        [[ -f "$2" ]] || die "EOS values file $2 does not exist"
        eos_values+=(--eos-values "$2")
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
      --dcache-values)
        [[ -f "$2" ]] || die "dCache values file $2 does not exist"
        dcache_values+=(--dcache-values "$2")
        shift ;;
      --dcache-image-tag)
        dcache_image_tag="$2"
        shift ;;
      --dcache-image-repository)
        dcache_image_repository="$2"
        shift ;;
      --cta-values)
        [[ -f "$2" ]] || die "CTA values file $2 does not exist"
        cta_overrides+=(--values "$2")
        shift ;;
      --copy-secret|--catalogue-secret|--scheduler-secret)
        [[ -n ${2:-} && ${#2} -le 253 && $2 =~ ^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$ ]] ||
          die "$1 requires a Kubernetes Secret name"
        case "$1" in
          --copy-secret) copy_secrets+=("$2") ;;
          --catalogue-secret) catalogue_secret="$2" ;;
          --scheduler-secret) scheduler_secret="$2" ;;
        esac
        shift ;;
      --infra-values)
        [[ -f "$2" ]] || die "Infrastructure values file $2 does not exist"
        infra_config_explicit=true
        infra_config+=(--values "$2")
        shift ;;
      --chart-install-timeout)
        chart_install_timeout="$2"
        shift ;;
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

  # Explicit connection flags win over values overlays, regardless of argument order.
  if [[ -n $catalogue_secret ]]; then
    copy_secrets+=("$catalogue_secret")
    cta_overrides+=(--set-string "global.catalogue.existingSecret=$catalogue_secret")
  elif [[ $infra_config_explicit == false ]]; then
    infra_config+=(--values presets/infra/catalogue-postgres.yaml)
  fi
  if [[ -n $scheduler_secret ]]; then
    copy_secrets+=("$scheduler_secret")
    cta_overrides+=(--set-string "global.scheduler.existingSecret=$scheduler_secret")
  elif [[ $infra_config_explicit == false ]]; then
    infra_config+=(--values presets/infra/scheduler-vfs.yaml)
  fi

  # Check requested Secrets before creating deployment resources.
  local secret_name
  for secret_name in "${copy_secrets[@]}"; do
    [[ $namespace != default ]] || die "Cannot copy Secrets from default into a deployment replacing default"
    kubectl --namespace default get secret "$secret_name" -o name >/dev/null ||
      die "Cannot read requested Secret '$secret_name' in namespace 'default'; create it before deploying"
    required_secrets[$secret_name]=true
  done

  # Only explicit CLI requests enable reset Jobs, including when values overlays request them.
  reset_flags=(--set "reset.catalogue=$reset_catalogue" --set "reset.scheduler=$reset_scheduler")
  check_deploy_commands
  update_local_cta_chart_dependencies >&2
  project_json_path="../../project.json"
  cta_image_registry=${cta_image_registry:-$(jq -r .dev.ctaImageRegistry "${project_json_path}")}
  eos_image_tag=${eos_image_tag:-$(jq -r .dev.eosImageTag "${project_json_path}")}
  eos_image_repository=${eos_image_repository:-$(jq -r .dev.eosImageRepository "${project_json_path}")}
  dcache_image_repository=${dcache_image_repository:-$(jq -r .dev.dCacheImageRepository "${project_json_path}")}
  dcache_image_tag=${dcache_image_tag:-$(jq -r .dev.dCacheImageTag "${project_json_path}")}

  echo "Catalogue schema version: ${catalogue_schema_version}"

  if [[ $reset_catalogue == true ]]; then
    log_warn "Catalogue content will be reset."
  else
    echo "Catalogue will be left unchanged."
  fi
  if [[ $reset_scheduler == true ]]; then
    log_warn "Scheduler content will be reset."
  else
    echo "Scheduler will be left unchanged."
  fi

  SECONDS=0

  # As we have no nice way of locking resources, just fail if another CTA release already exists
  if [[ $(helm list --all-namespaces | grep cta | wc -l) -ge 1 ]]; then
    die "Another CTA release was found. Currently, installing multiple CTA releases on the same machine is not supported."
  fi

  generate_tape_values_files

  if [[ " ${infra_config[*]} " == *" presets/infra/scheduler-vfs.yaml "* ]]; then
    kubectl get sc local-path >/dev/null 2>&1 ||
      die "The VFS infrastructure preset requires the local-path StorageClass."
  fi

  # Create the namespace
  log_task "Creating namespace ${namespace}..."
  kubectl create namespace "${namespace}"
  kubectl label namespace "$namespace" "app.kubernetes.io/managed-by=cta-create-instance"
  log_task "Copying secrets into namespace ${namespace}..."
  for secret_name in "${copy_secrets[@]}" "${secrets[@]}"; do
    # Explicit requests must still exist; only the built-in runner Secrets are optional.
    if [[ ${required_secrets[$secret_name]:-false} == true ]] ||
       kubectl --namespace default get secret "${secret_name}" &> /dev/null; then
      kubectl --namespace default get secret "${secret_name}" -o json |
        jq --arg namespace "$namespace" \
          '{apiVersion, kind, type, data, metadata: {name: .metadata.name, namespace: $namespace}}' |
        kubectl --namespace "${namespace}" create -f -
    fi
  done

  # Infrastructure has remote dependencies; consumer charts use local sources.
  # Downloading the remote telemetry charts takes seconds; skip it while every archive is current.
  if helm dependency list helm/ci-infra | awk 'NR > 1 && NF && $NF != "ok" {stale = 1} END {exit !stale}'; then
    log_task "Updating infrastructure chart dependencies..."
    helm dependency update helm/ci-infra --repository-config /dev/null > /dev/null
  fi

  log_task "Starting deployment..."

  # Presets request infrastructure resources; the chart itself does not select application backends.
  infra_flags=()
  if [[ $local_telemetry == true ]]; then
    infra_flags+=(--values presets/infra/telemetry.yaml)
  fi
  if [[ "$dcache_enabled" == true ]]; then
    infra_flags+=(--values presets/infra/dcache.yaml)
  fi
  infra_flags+=("${infra_config[@]}")

  # Wait in the background: the tools must mount the VFS PVC before infrastructure can become ready.
  log_run helm install ci-infra helm/ci-infra \
                       --namespace "${namespace}" \
                       "${infra_flags[@]}" \
                       --wait --wait-for-jobs --timeout 4m &
  infra_pid=$!

  # EOS needs shared credentials, but can initialize alongside the CTA reset Jobs.
  if [[ $eos_enabled == "true" ]]; then
    ./deploy_eos.sh --namespace "${namespace}" \
                    "${eos_values[@]}" \
                    --eos-image-repository "${eos_image_repository}" \
                    --eos-image-tag "${eos_image_tag}" &
    eos_pid=$!
  fi

  # Both consumer releases use the same public connection settings and overrides.
  shared_flags=("${cta_config[@]}"
                --set-string "global.image.registry=$cta_image_registry" --set-string "global.image.tag=$cta_image_tag")
  if [[ "$dcache_enabled" == true ]]; then
    # The dCache CTA nearline-storage driver only supports mTLS.
    shared_flags+=(--values presets/cta/auth/frontend-wfe-mtls.yaml)
  fi
  if [[ $local_telemetry == true ]]; then
    shared_flags+=(--values presets/cta/telemetry/local.yaml)
  fi

  # The client waits for credentials and the KDC; it does not need running CTA services.
  log_run helm install cta-tools helm/cta-tools \
                       --namespace "${namespace}" \
                       "${shared_flags[@]}" \
                       --set backend.enabled=true --set cli.enabled=true \
                       "${reset_flags[@]}" \
                       --set-string "reset.catalogueSchemaVersion=$catalogue_schema_version" \
                       --wait --wait-for-jobs --timeout 5m "${cta_overrides[@]}"

  wait "$infra_pid" || die "Infrastructure installation failed."

  if [[ $dcache_enabled == "true" ]]; then
    ./deploy_dcache.sh --namespace "${namespace}" \
                       "${dcache_values[@]}" \
                       --dcache-image-repository "${dcache_image_repository}" \
                       --dcache-image-tag "${dcache_image_tag}" &
    dcache_pid=$!
  fi

  log_run helm install cta helm/cta \
                       --namespace "${namespace}" \
                       "${shared_flags[@]}" \
                       -f "${taped_config}" \
                       -f "${rmcd_config}" \
                       --wait --timeout "${chart_install_timeout}"m "${cta_overrides[@]}"
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
