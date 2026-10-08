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
  echo "      --no-setup:                     Skip the setup scripts for EOS and tape resets."
  echo "      --eos-image-tag <tag>:          Docker image tag for the EOS chart."
  echo "      --eos-image-repository <repo>:  Docker image for EOS chart. Should be the full image name, e.g. \"gitlab-registry.cern.ch/dss/eos/eos-ci\"."
  echo "      --eos-config <file>:            Values file to use for the EOS chart."
  echo "      --eos-enabled <true|false>:     Whether to spawn an EOS instance or not. Defaults to true."
  echo "      --dcache-enabled <true|false>:  Whether to spawn a dCache instance or not. Defaults to false."
  echo "      --cta-config <files>:           Values file(s) to use for the CTA chart. Comma-separated for composition."
  echo "      --chart-install-timeout <min>:  CTA Helm chart installation timeout in minutes."
  echo "      --one-logical-library           Will use only one logical library name for all drives except the default creating one library name for each drive."
  echo "      --local-telemetry:              Spawns an OpenTelemetry and Collector and Prometheus scraper. Changes the default cta-config to presets/dev-cta-telemetry-values.yaml"
  echo "      --publish-telemetry:            Publishes telemetry to a pre-configured central observability backend. See presets/ci-cta-telemetry-values.yaml"
  echo "      --extra-cta-values:             Extra verbatim values for the CTA chart. These will override any previous values from files."
  echo "      --no-hardware-drives:           Skip mhvtl/SCSI drive detection entirely.  Use this when --cta-config already defines drives (e.g. stress-drive mode)."
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

label_stress_node() {
  # If the user already labelled a node manually, respect that and skip.
  local already_labelled
  already_labelled=$(kubectl get nodes -l cta-stress-node=true \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  if [[ -n "${already_labelled}" ]]; then
    log_task "Node ${already_labelled} already has cta-stress-node=true; skipping auto-labelling."
    # Always record the node name — needed by mount_stress_tmpfs and delete_instance.sh
    # regardless of whether this run or a previous one applied the label.
    echo "${already_labelled}" > "/tmp/${namespace}-stress-node.txt"
    return
  fi
  # Pick the first available node.
  local node
  node=$(kubectl get nodes --no-headers -o custom-columns=':metadata.name' | head -1)
  [[ -n "${node}" ]] || die "No Kubernetes nodes found to label for stress-drive scheduling."
  log_task "Labelling node ${node} with cta-stress-node=true for stress-drive pod scheduling..."
  kubectl label node "${node}" cta-stress-node=true
  # Record the node so delete_instance.sh can remove the label and unmount the tmpfs on cleanup.
  echo "${node}" > "/tmp/${namespace}-stress-node.txt"
  log_success "Node ${node} labelled."
}

# Verify that the dedicated stress-drive tmpfs has been mounted on the host.
#
# ─── MANUAL PREREQUISITE ────────────────────────────────────────────────────
#
# This script does not mount the tmpfs itself because it does not run as root.
# A system administrator MUST run the following command on the stress node AS
# ROOT before deploying the stress-drive test, and once again after a reboot:
#
#   mount -t tmpfs \
#     -o size=20g,nr_inodes=4096,noatime,huge=within_size \
#     tmpfs /dev/shm/cta-stress
#
#   # If the kernel does not support huge=within_size, omit that option:
#   mount -t tmpfs -o size=20g,nr_inodes=4096,noatime tmpfs /dev/shm/cta-stress
#
#   chmod 1777 /dev/shm/cta-stress
#
# To make the mount survive reboots add a line to /etc/fstab on the node:
#   tmpfs  /dev/shm/cta-stress  tmpfs  size=20g,nr_inodes=4096,noatime,huge=within_size  0  0
#
# To tear down after the test (also as root):
#   umount /dev/shm/cta-stress
#
# Mount option rationale:
#   size=20g          10 M files × ~656 B/file across 20 tapes ≈ 6.6 GB data;
#                     × 2 for atomic save (tape.bin + tape.bin.tmp coexist on
#                     every dismount) ≈ 13 GB; 20 GB cap gives headroom.
#   nr_inodes=4096    ~100 inodes needed; explicit cap prevents the kernel from
#                     auto-sizing the table to RAM/4096 (~8 MB wasted metadata).
#   noatime           Skip in-memory atime updates on tape.bin reads.
#   huge=within_size  Use 2 MB transparent huge pages for tape.bin files once
#                     they exceed 2 MB, reducing TLB pressure on the sequential
#                     serialise/deserialise pass at each mount/dismount.
#
# ────────────────────────────────────────────────────────────────────────────
mount_stress_tmpfs() {
  local base_dir="/dev/shm/cta-stress"

  if ! mountpoint -q "${base_dir}" 2>/dev/null; then
    die "Stress-drive tmpfs is not mounted at ${base_dir}." \
        "Run the following as root on the stress node before deploying:" \
        "  mount -t tmpfs -o size=20g,nr_inodes=4096,noatime,huge=within_size tmpfs ${base_dir}" \
        "  chmod 1777 ${base_dir}" \
        "See the comment above mount_stress_tmpfs() in create_instance.sh for full details."
  fi

  log_success "Stress-drive tmpfs is mounted at ${base_dir}."
}

generate_stress_drive_values() {
  log_task "Generating stress drive configuration (${max_drives} drives)..."
  taped_config=$(mktemp "/tmp/${namespace}-taped-stress-XXXXXX-values.yaml")
  {
    echo "taped:"
    echo "  drives:"
    for i in $(seq 0 $((max_drives - 1))); do
      printf "    - name: \"STRESS%04d\"\n" "$i"
      printf "      device: \"stress://\"\n"
      printf "      logicalLibraryName: \"stress-lib\"\n"
      printf "      controlPath: \"smc0\"\n"
    done
  } > "$taped_config"
  echo "Content of stress drive values file $taped_config:"
  echo
  cat "$taped_config"
  echo
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
  # This is a hack to ensure we don't waste 30 seconds updating local dependencies
  # Once helm dependency update gets some performance improvements this can be removed
  TEMP_HELM_HOME=$(mktemp -d)
  add_trap 'rm -rf "$TEMP_HELM_HOME"' EXIT
  export HELM_CONFIG_HOME="$TEMP_HELM_HOME"

  log_task "Updating Helm chart dependencies..."
  charts=(
    "common"
    "auth"
    "catalogue"
    "scheduler"
    "client"
    "cli"
    "frontend-wfe"
    "frontend-admin"
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
  setup_enabled=true
  cta_image_registry=$(jq -r .dev.ctaImageRegistry ${project_json_path})
  max_drives=2
  # EOS related
  eos_image_tag=$(jq -r .dev.eosImageTag ${project_json_path})
  eos_image_repository=$(jq -r .dev.eosImageRepository ${project_json_path})
  eos_config=presets/eos/auth-jwt.yaml
  eos_enabled=true
  # dCache
  dcache_image_tag=$(jq -r .dev.dCacheImageTag ${project_json_path})
  dcache_config=presets/dev-dcache-values.yaml
  dcache_enabled=false
  # CTA chart timeout
  chart_install_timeout=5
  # Telemetry
  local_telemetry=false
  publish_telemetry=false
  one_logical_library=false
  no_hardware_drives=false

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
      --no-setup) setup_enabled=false ;;
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
      --no-hardware-drives) no_hardware_drives=true ;;
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

  # Determine the library config to use.
  taped_config=""
  rmcd_config=""
  if [[ "${no_hardware_drives}" == "true" ]]; then
    # Stress-drive mode: generate drive list from --max-drives; no rmcd config needed.
    label_stress_node
    mount_stress_tmpfs
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
                    --setup-enabled "${setup_enabled}" &
    eos_pid=$!
  fi

  if [[ $dcache_enabled == "true" ]]; then
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
    extra_cta_chart_flags+=" --values presets/ci-cta-telemetry-values.yaml"
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

  if [[ "$setup_enabled" == "true" ]]; then
    ./setup/reset_tapes.sh -n "${namespace}"
    ./setup/kinit_clients.sh -n "${namespace}"
  fi

  echo
  echo "Deployed pods:"
  kubectl --namespace "${namespace}" get pods
  echo
  log_success "Deployed CTA instance ${namespace} in ${SECONDS} seconds."
}

create_instance "$@"
