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
  echo "      --chart-install-timeout <min>:  CTA Helm chart installation timeout in minutes.
      --eos-deploy-timeout <min>:    EOS Helm chart installation timeout in minutes (default: 5)."
  echo "      --one-logical-library           Will use only one logical library name for all drives except the default creating one library name for each drive."
  echo "      --local-telemetry:              Spawns an OpenTelemetry and Collector and Prometheus scraper. Changes the default cta-config to presets/dev-cta-telemetry-values.yaml"
  echo "      --publish-telemetry:            Publishes telemetry to a pre-configured central observability backend. See presets/ci-cta-telemetry-values.yaml"
  echo "      --extra-cta-values:             Extra verbatim values for the CTA chart. These will override any previous values from files."
  echo "      --no-hardware-drives:           Use virtual stress drives instead of mhvtl/SCSI hardware.  Deploys the virtual-tape-library chart and generates a drive list from --max-drives."
  echo "      --num-stress-libraries <n>:     Number of independent logical libraries for stress drives (default: 1). Requires --no-hardware-drives."
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

deploy_virtual_tape_library() {
  log_task "Deploying virtual-tape-library chart (PVC + directory layout + library.toml)..."
  local num_libs="${num_stress_libraries:-1}"
  local -a extra_flags=()
  if kubectl get sc fast-ssd >/dev/null 2>&1; then
    # fast-ssd uses WaitForFirstConsumer with static (no-provisioner) PVs.
    # Collect all available fast-ssd PVs; if there are enough, bind each
    # library PVC directly to its own PV (bypasses WaitForFirstConsumer so
    # the prepopulate Job starts immediately).
    # On teardown, reclaim_released_pvs in delete_instance.sh catches the PVs
    # (Released, claimRef.namespace matches, not local-path) and reclaims them.
    readarray -t fast_ssd_pvs < <(kubectl get pv -o json | jq -r \
      '[.items[] | select(.spec.storageClassName=="fast-ssd" and .status.phase=="Available") | .metadata.name] | .[]')
    if [[ ${#fast_ssd_pvs[@]} -ge "${num_libs}" ]]; then
      echo "  fast-ssd StorageClass found — binding ${num_libs} PVC(s) to: ${fast_ssd_pvs[*]:0:${num_libs}}"
      extra_flags+=("--set" "pvc.storageClass=fast-ssd")
      for i in $(seq 0 $((num_libs - 1))); do
        extra_flags+=("--set" "pvc.volumeNames[${i}]=${fast_ssd_pvs[$i]}")
      done
    elif [[ ${#fast_ssd_pvs[@]} -gt 0 ]]; then
      log_warn "Found ${#fast_ssd_pvs[@]} fast-ssd PV(s) but need ${num_libs}; using cluster default StorageClass."
    else
      echo "  fast-ssd StorageClass found but no Available PVs — using cluster default StorageClass."
    fi
  elif kubectl get sc local-path >/dev/null 2>&1; then
    # local-path is the default dynamic provisioner on k3s / k3d clusters.
    # It creates a directory under /var/lib/rancher/k3s/storage/ on the node.
    echo "  local-path StorageClass found — using it for library PVCs."
    extra_flags+=("--set" "pvc.storageClass=local-path")
  else
    # No suitable StorageClass found.  Create one hostPath PV per library
    # under /tmp so the prepopulate Job and taped pods can bind immediately.
    # This covers single-node clusters (CI workers, local kind/minikube)
    # where the node filesystem is directly accessible from kubectl.
    # Cleanup: delete_instance.sh removes these PVs and dirs (pass --keep-pvs to skip).
    echo "  No fast-ssd or local-path StorageClass — creating hostPath PVs under /tmp."
    for i in $(seq 0 $((num_libs - 1))); do
      local dir="/tmp/cta-tape-lib-${namespace}-${i}"
      local pv_name="cta-tape-lib-${namespace}-${i}"
      mkdir -p "${dir}"
      kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolume
metadata:
  name: ${pv_name}
  labels:
    cta-hostpath-ns: "${namespace}"
spec:
  capacity:
    storage: 50Gi
  accessModes:
    - ReadWriteOnce
  persistentVolumeReclaimPolicy: Retain
  storageClassName: ""
  hostPath:
    path: "${dir}"
EOF
      extra_flags+=("--set" "pvc.volumeNames[${i}]=${pv_name}")
    done
    # storageClassName: "" in the PVC matches the PVs above (no StorageClass).
    extra_flags+=("--set" "pvc.storageClass=-")
  fi
  extra_flags+=("--set" "libraryCount=${num_libs}")
  helm upgrade --install virtual-tape-library helm/virtual-tape-library \
    --namespace "${namespace}" \
    --wait --wait-for-jobs --timeout 5m \
    "${extra_flags[@]}"
  log_success "virtual-tape-library chart deployed."
}

generate_stress_drive_values() {
  local num_libs="${num_stress_libraries:-1}"
  local drives_per_lib=$(( (max_drives + num_libs - 1) / num_libs ))
  log_task "Generating stress drive configuration (${max_drives} drives across ${num_libs} librar$([ "$num_libs" -eq 1 ] && echo y || echo ies))..."

  # Per-drive taped values.  Drives are split across libraries by ceiling division;
  # the last library may have fewer drives if max_drives is not a multiple of num_libs.
  # All library PVCs are mounted into every taped pod so that library.toml is always
  # readable from any pod, regardless of which library the drive belongs to.
  taped_config=$(mktemp "/tmp/${namespace}-taped-stress-XXXXXX-values.yaml")
  {
    echo "taped:"
    echo "  drives:"
    # Drive naming: S{lib}D{index_within_lib} — mirrors the tape VID prefix S{lib},
    # so everything in library N is immediately identifiable in logs and monitoring.
    # Example: 10 drives across 3 libraries → S0D0000..S0D0009, S1D0000..S1D0009, S2D0000..S2D0004
    for i in $(seq 0 $((max_drives - 1))); do
      lib_index=$(( i / drives_per_lib ))
      (( lib_index >= num_libs )) && lib_index=$(( num_libs - 1 ))
      drive_in_lib=$(( i % drives_per_lib ))
      printf "    - name: \"S%dD%04d\"\n" "$lib_index" "$drive_in_lib"
      printf "      device: \"virtual:///cta-stress-lib-%d\"\n" "$lib_index"
      printf "      logicalLibraryName: \"stress-lib-%d\"\n" "$lib_index"
      printf "      controlPath: \"smc0\"\n"
    done
    echo "  extraVolumes:"
    echo "    volumes:"
    for l in $(seq 0 $((num_libs - 1))); do
      printf "      - name: \"stress-lib-%d\"\n" "$l"
      printf "        persistentVolumeClaim:\n"
      printf "          claimName: \"cta-virtual-tape-library-%d\"\n" "$l"
    done
    echo "    volumeMounts:"
    for l in $(seq 0 $((num_libs - 1))); do
      printf "      - name: \"stress-lib-%d\"\n" "$l"
      printf "        mountPath: \"/cta-stress-lib-%d\"\n" "$l"
    done
  } > "$taped_config"

  echo "Content of stress drive values file $taped_config:"
  echo
  cat "$taped_config"
  echo

  # Disable rmcd: no tape library hardware is present; taped never contacts rmcd
  # for virtual drives, and /dev/smc does not exist on a node without mhvtl.
  rmcd_config=$(mktemp "/tmp/${namespace}-rmcd-XXXXXX-values.yaml")
  printf "rmcd:\n  enabled: false\n" > "$rmcd_config"
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
    "auth"
    "catalogue"
    "scheduler"
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
  # Note that it is fine for not all of these secrets to exist
  secrets="reg-eoscta-operations reg-ctageneric monit-it-sd-tab-ci-pwd monit-it-sd-tab-ci-credentials" # Secrets to be copied to the namespace (space separated)
  catalogue_config=presets/dev-catalogue-postgres-values.yaml
  scheduler_config=presets/dev-scheduler-vfs-values.yaml
  cta_config="-f presets/dev-cta-common.yaml -f presets/frontend-wfe/auth-jwt.yaml -f presets/frontend-admin/auth-jwt.yaml"
  prometheus_config="presets/dev-prometheus-values.yaml"
  opentelemetry_collector_config="presets/dev-otel-collector-values.yaml"
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
  # EOS chart timeout
  eos_deploy_timeout=5
  # Telemetry
  local_telemetry=false
  publish_telemetry=false
  one_logical_library=false
  no_hardware_drives=false
  num_stress_libraries=2

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
      --eos-deploy-timeout)
        eos_deploy_timeout="$2"
        shift ;;
      --publish-telemetry) publish_telemetry=true ;;
      --one-logical-library) one_logical_library=true ;;
      --no-hardware-drives) no_hardware_drives=true ;;
      --num-stress-libraries)
        num_stress_libraries="$2"
        shift ;;
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
    echo "Catalogue content will be kept."
  fi

  if [[ "$reset_scheduler" == "true" ]]; then
    log_warn "Scheduler content will be reset."
  else
    echo "Scheduler content will be kept."
  fi

  SECONDS=0

  # This is where the actual scripting starts. All of the above is just initializing some variables, error checking and producing debug output

  # As we have no nice way of locking resources, just fail if another CTA release already exists
  if [[ $(helm list --all-namespaces | grep cta | wc -l) -ge 1 ]]; then
    die "Another CTA release was found. Currently, installing multiple CTA releases on the same machine is not supported."
  fi

  # Determine the library config to use (drive values only; the virtual-tape-library
  # chart itself is deployed after the namespace is created, see below).
  taped_config=""
  rmcd_config=""
  if [[ "${no_hardware_drives}" == "true" ]]; then
    generate_stress_drive_values
  else
    generate_tape_values_files
  fi

  if [[ "$scheduler_config" == "presets/dev-scheduler-vfs-values.yaml" ]]; then
    if kubectl get sc local-path >/dev/null 2>&1; then
      echo "Local path provisioning is enabled; using the VFS scheduler."
    else
      echo "==============================================================================="
      log_warn "Local path provisioning is not enabled. Support for this configuration will be removed soon."
      echo "==============================================================================="
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
      log_warn "Falling back to presets/dev-scheduler-vfs-deprecated-values.yaml."
      echo "==============================================================================="
      echo
      echo
      scheduler_config=presets/dev-scheduler-vfs-deprecated-values.yaml
    fi
  fi


  # Create the namespace
  log_task "Creating namespace ${namespace}..."
  kubectl create namespace "${namespace}"
  kubectl label namespace "$namespace" "app.kubernetes.io/managed-by=cta-create-instance"

  # Deploy the virtual-tape-library chart now that the namespace exists.
  if [[ "${no_hardware_drives}" == "true" ]]; then
    deploy_virtual_tape_library
  fi
  log_task "Copying secrets into namespace ${namespace}..."
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
    # For virtual-drive (stress/scale) tests, overlay relaxed scrape settings on
    # top of the base Prometheus config: 100 pods export simultaneously, which
    # temporarily slows the OTel collector's :8889 Prometheus endpoint and causes
    # holes in Grafana when the default 3 s scrape_timeout fires first.
    stress_prometheus_overlay=""
    if [[ "${no_hardware_drives}" == "true" ]]; then
      stress_prometheus_overlay="--values presets/stress-prometheus-values.yaml"
    fi
    helm install prometheus prometheus-community/prometheus \
          --namespace "${namespace}" \
          --values "${prometheus_config}" \
          ${stress_prometheus_overlay} \
          --wait --timeout 2m
  fi

  # Note that some of these charts are installed in parallel
  log_run helm upgrade --install auth helm/auth \
                                --namespace "${namespace}" \
                                --wait --wait-for-jobs --timeout 2m &
  auth_pid=$!

  log_run helm upgrade --install cta-catalogue helm/catalogue \
                                --namespace "${namespace}" \
                                --set resetImage.registry="${cta_image_registry}" \
                                --set resetImage.tag="${cta_image_tag}" \
                                --set schemaVersion="${catalogue_schema_version}" \
                                --set resetCatalogue="${reset_catalogue}" \
                                --set-file configuration="${catalogue_config}" \
                                --wait --wait-for-jobs --timeout 4m &
  catalogue_pid=$!

  log_run helm upgrade --install cta-scheduler helm/scheduler \
                                --namespace "${namespace}" \
                                --set resetImage.registry="${cta_image_registry}" \
                                --set resetImage.tag="${cta_image_tag}" \
                                --set resetScheduler="${reset_scheduler}" \
                                --set-file configuration="${scheduler_config}" \
                                --wait --wait-for-jobs --timeout 4m &
  scheduler_pid=$!

  # The disk-buffer init containers need both the auth secrets and a responsive KDC.
  # Waiting only for the secrets can make their first kadmin call race KDC startup.
  wait $auth_pid || exit 1

  if [[ $eos_enabled == "true" ]]; then
    ./deploy_eos.sh --namespace "${namespace}" \
                    --eos-config "${eos_config}" \
                    --eos-image-repository "${eos_image_repository}" \
                    --eos-image-tag "${eos_image_tag}" \
                    --eos-deploy-timeout "${eos_deploy_timeout}" &
    eos_pid=$!
  fi

  if [[ $dcache_enabled == "true" ]]; then
    ./deploy_dcache.sh --namespace "${namespace}" \
                       --dcache-image-repository "${dcache_image_repository}" \
                       --dcache-image-tag "${dcache_image_tag}" &
    dcache_pid=$!
  fi


  # Wait for the scheduler and catalogue charts to be installed (and exit if 1 failed)
  wait $catalogue_pid || exit 1
  wait $scheduler_pid || exit 1

  # Flags applied BEFORE the cta_config chain so that user-supplied presets
  # (e.g. stress-cta-pgsched-values.yaml) can override telemetry defaults.
  pre_cta_chart_flags=""
  if [[ "$local_telemetry" == "true" ]]; then
    pre_cta_chart_flags+=" --values presets/dev-cta-telemetry-values.yaml"
  fi
  if [[ "$publish_telemetry" == "true" ]]; then
    pre_cta_chart_flags+=" --values presets/ci-cta-telemetry-values.yaml"
  fi

  extra_cta_chart_flags=""
  if [[ "$dcache_enabled" == "true" ]]; then
    # The dCache CTA nearline-storage driver only supports mTLS.
    extra_cta_chart_flags+=" --values presets/frontend-wfe/auth-mtls.yaml"
  fi
  # This is a bit hacky, but will be removed when either the objectstore is gone or when I get around to cleaning up all the values files (hopefully soon TM)
  if grep -q "postgres" "$scheduler_config"; then
    extra_cta_chart_flags+=" --values presets/dev-cta-maintd-postgres-values.yaml"
  else
    extra_cta_chart_flags+=" --values presets/dev-cta-maintd-objectstore-values.yaml"
  fi
  if [[ "$extra_cta_values" ]]; then
    extra_cta_chart_flags+=" ${extra_cta_values} "
  fi
  if [[ -n "${taped_config}" ]]; then
    extra_cta_chart_flags+=" -f ${taped_config}"
  fi
  if [[ -n "${rmcd_config}" ]]; then
    extra_cta_chart_flags+=" -f ${rmcd_config}"
  fi


  log_run helm upgrade --install cta helm/cta \
                                --namespace "${namespace}" \
                                ${pre_cta_chart_flags} \
                                ${cta_config} \
                                --set global.image.registry="${cta_image_registry}" \
                                --set global.image.tag="${cta_image_tag}" \
                                --set-file global.configuration.scheduler="${scheduler_config}" \
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
