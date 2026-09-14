#!/bin/bash

# SPDX-FileCopyrightText: 2021 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -eo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/../utils/log_utils.sh"

local_die() {
  log_error "$@"
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Finished $0 "
  echo "================================================================================"
  exit 1
}

# While this script looks like it does a lot, it essentially just does fancy namespace deletion.
# `kubectl delete namespace <ns>` will accomplish mostly the same, except you lose log collection
# and a few central resources may stick around. These are typically not harmful though.
usage() {
  echo
  echo "Deletes a given Kubernetes namespace and optionally collects the logs in said namespace."
  echo
  echo "Usage: $0 -n <namespace> [options]"
  echo
  echo "options:"
  echo "  -h, --help:                             Shows help output."
  echo "  -n, --namespace <namespace>:            Specify the Kubernetes namespace."
  echo "      --collect-logs <dir>:               Collect pod logs and diagnostics in the specified directory."
  echo "      --keep-pvs:                         Skip the wiping and reclaiming of released Persistent Volumes after namespace cleanup."
  echo "      --keep-cluster-resources:           Keep cluster roles and bindings owned by this namespace."
  echo "      --force-delete-cluster-resources:   Delete named cluster roles and bindings even when ownership cannot be verified."
  echo
  exit 1
}

save_logs() {
  namespace="$1"
  destination="$2"

  mkdir -p "${destination}" || return 1
  log_task "Collecting logs in ${destination}..."

  # We get all the pod details in one go so that we don't have to do too many kubectl calls
  pods=$(kubectl --namespace "${namespace}" get pods -o json)

  # Iterate over pods
  # Use a non-whitespace delimiter so that Bash preserves empty fields such as a missing instance label.
  echo "${pods}" | jq -r '
    .items[]
    | [
        .metadata.name,
        (.metadata.labels["app.kubernetes.io/instance"] // ""),
        (.status.phase // ""),
        (
          (
            ((.spec.initContainers // []) | map(.name + ":init"))
            + (.spec.containers | map(.name + ":regular"))
            + ((.spec.ephemeralContainers // []) | map(.name + ":ephemeral"))
          )
          | join(" ")
        )
      ]
    | join("\u001f")
  ' | while IFS=$'\x1f' read -r pod instance phase containers; do
    pod_dir="${destination}/${pod}"
    mkdir -p "${pod_dir}"

    kubectl -n "${namespace}" describe pod "${pod}" > "${pod_dir}/describe.log" || {
      log_warn "Failed to collect describe output for ${pod}."
      rm -f "${pod_dir}/describe.log"
    }

    for container_entry in ${containers}; do
      container="${container_entry%:*}"
      container_type="${container_entry##*:}"

      # Collect stdout logs
      if ! kubectl -n "${namespace}" logs "${pod}" -c "${container}" > "${pod_dir}/${container}.stdout.log"; then
        log_warn "Failed to collect stdout logs for ${pod}/${container}."
        rm -f "${pod_dir}/${container}.stdout.log"
      fi

      # Init and ephemeral containers are not expected to hold the service's /var/log contents.
      if [[ ("${instance}" == "cta" || "${instance}" == "eos") \
        && "${phase}" == "Running" && "${container_type}" == "regular" ]]; then
        max_allowed_size=$((2 * 1024 * 1024 * 1024)) # 2 GB
        var_log_size=$(kubectl -n "${namespace}" exec "${pod}" -c "${container}" -- \
          du -sb /var/log 2>/dev/null || true)
        var_log_size="${var_log_size%%[[:space:]]*}"
        [[ "${var_log_size}" =~ ^[0-9]+$ ]] || var_log_size=0

        if (( var_log_size > max_allowed_size )); then
          log_warn "Skipping /var/log for ${pod}/${container}: ${var_log_size} bytes is too large."
          kubectl -n "${namespace}" exec "${pod}" -c "${container}" -- du -h /var/log >&2 || true
          log_error "Failed to collect /var/log contents for ${pod}/${container}."
          continue
        fi
        # Only tar part of the logs
        subdirs_to_tar=("cta" "eos" "tmp" "xrootd" "*/xrd_errors")
        if ! existing_dirs=$(
          kubectl -n "${namespace}" exec "${pod}" -c "${container}" -- \
            bash -c "cd /var/log && find ${subdirs_to_tar[*]} -maxdepth 0 -type d 2>/dev/null || true" 2>/dev/null
        ); then
          log_warn "Failed to discover /var/log contents for ${pod}/${container}."
          continue
        fi
        [[ -n "${existing_dirs}" ]] || continue

        if ! kubectl -n "${namespace}" exec "${pod}" -c "${container}" -- \
          tar --warning=no-file-removed --ignore-failed-read -C /var/log -cf - ${existing_dirs} \
          | XZ_OPT='-0 -T0' xz > "${pod_dir}/${container}.varlog.tar.xz"; then
          log_warn "Failed to collect /var/log contents for ${pod}/${container}."
          rm -f "${pod_dir}/${container}.varlog.tar.xz"
        fi
      fi
    done
  done

}

reclaim_released_pvs() {
  wipe_namespace="$1"
  released_pvs=$(kubectl get pv -o json | jq -r \
    --arg ns "$wipe_namespace" \
    '.items[]
    | select(
        .status.phase == "Released"
        and .spec.claimRef.namespace == $ns
        and .spec.storageClassName != "local-path"
      )
    | .metadata.name')

  for pv in $released_pvs; do
    log_task "Processing persistent volume ${pv}..."

    path=$(kubectl get pv "$pv" -o jsonpath='{.spec.local.path}')
    if [[ -z "$path" ]]; then
      log_warn "Skipping ${pv}: no local path was found."
      continue
    fi
    echo "  Found path: $path"

    if [[ -d "$path" ]]; then
      log_task "Wiping contents of ${path}..."
      # We need sudo here as files in the mount path can be owned by root
      # Note that this requires explicit permission in the sudoers file to ensure the user executing this
      # Can remove the contents of these mount paths
      (
        shopt -s dotglob
        sudo rm -rf "${path:?}/"*
      )
    else
      log_warn "${path} does not exist on this node."
      continue
    fi

    # Remove claimRef to mark PV as Available again
    log_task "Removing claimRef from persistent volume ${pv}..."
    kubectl patch pv "$pv" --type=json -p='[{"op": "remove", "path": "/spec/claimRef"}]'
    log_success "Wiped and reclaimed persistent volume ${pv}."
  done
}

delete_cluster_resource_if_owned() {
  local namespace="$1" resource_type="$2" resource_name="$3" force_delete="$4" release_namespace
  if ! kubectl get "$resource_type" "$resource_name" >/dev/null 2>&1; then
    return
  fi
  if [[ $force_delete == true ]]; then
    log_warn "Force deleting ${resource_type}/${resource_name} without checking ownership."
    kubectl delete "$resource_type" "$resource_name"
    return
  fi
  if ! release_namespace=$(kubectl get "$resource_type" "$resource_name" \
    -o 'jsonpath={.metadata.annotations.meta\.helm\.sh/release-namespace}'); then
    log_warn "Could not verify ownership of ${resource_type}/${resource_name}; skipping it."
    return
  fi
  if [[ $release_namespace == "$namespace" ]]; then
    kubectl delete "$resource_type" "$resource_name"
  else
    log_warn "Skipping shared ${resource_type}/${resource_name}: it is not owned by namespace ${namespace}"
  fi
}

delete_instance() {
  local log_directory=""
  local wipe_pvs=true
  local delete_cluster_resources=true
  local force_delete_cluster_resources=false
  local namespace=""

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      -h | --help) usage ;;
      -n|--namespace)
        namespace="$2"
        shift ;;
      --collect-logs)
        [[ $# -ge 2 && -n "$2" ]] || die_usage "--collect-logs requires a directory"
        log_directory="$2"
        shift ;;
      --keep-pvs) wipe_pvs=false ;;
      --keep-cluster-resources) delete_cluster_resources=false ;;
      --force-delete-cluster-resources) force_delete_cluster_resources=true ;;
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
  if [[ $delete_cluster_resources == false && $force_delete_cluster_resources == true ]]; then
    die_usage "--keep-cluster-resources and --force-delete-cluster-resources cannot be used together"
  fi

  if ! kubectl get namespace "$namespace" >/dev/null 2>&1; then
    log_task "Namespace ${namespace} does not exist; nothing to delete..."
    exit 0
  fi

  SECONDS=0

  log_task "Deleting namespace $namespace"
  echo "Namespace to be deleted:"
  kubectl get pods --namespace ${namespace}

  # Optional log collection
  if [[ -n "${log_directory}" ]]; then
    if ! save_logs "$namespace" "$log_directory"; then
      log_error "Log collection failed for namespace ${namespace}; continuing with namespace deletion."
    fi
  fi

  # Cleanup of old library values files:
  log_task "Removing auto-generated values files..."
  rm -f /tmp/${namespace}-rmcd-*-values.yaml
  rm -f /tmp/${namespace}-taped-*-values.yaml

  # Delete the actual namespace
  log_task "Deleting CTA instance ${namespace}..."
  kubectl delete pods,jobs,deployments,statefulsets,pvc --all -n "${namespace}" --now --wait=false >/dev/null
  kubectl delete namespace "${namespace}" --wait=true >/dev/null

  # Reclaim any PVs
  if [[ "$wipe_pvs" = true ]]; then
    reclaim_released_pvs $namespace
  else
    log_warn "Skipping reclamation of released persistent volumes."
  fi
  if [[ $delete_cluster_resources == true ]]; then
    delete_cluster_resource_if_owned "$namespace" clusterrole otel-opentelemetry-collector "$force_delete_cluster_resources"
    delete_cluster_resource_if_owned "$namespace" clusterrolebinding otel-opentelemetry-collector "$force_delete_cluster_resources"
    delete_cluster_resource_if_owned "$namespace" clusterrole prometheus-server "$force_delete_cluster_resources"
    delete_cluster_resource_if_owned "$namespace" clusterrolebinding prometheus-server "$force_delete_cluster_resources"
    delete_cluster_resource_if_owned "$namespace" clusterrole prometheus-kube-state-metrics "$force_delete_cluster_resources"
    delete_cluster_resource_if_owned "$namespace" clusterrolebinding prometheus-kube-state-metrics "$force_delete_cluster_resources"
  else
    log_warn "Skipping cleanup of shared cluster-level resources."
  fi
  echo
  log_success "Deleted CTA instance ${namespace} in ${SECONDS} seconds."
}

delete_instance "$@"
