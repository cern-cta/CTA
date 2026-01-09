#!/bin/bash

# SPDX-FileCopyrightText: 2021 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

source "$(dirname "${BASH_SOURCE[0]}")/../utils/log_wrapper.sh"

die() {
  echo "$@" 1>&2
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Finished $0 "
  echo "================================================================================"
  exit 1
}

usage() {
  echo
  echo "Deletes a given Kubernetes namespace and optionally collects the logs in said namespace."
  echo
  echo "Usage: $0 -n <namespace> [options]"
  echo
  echo "options:"
  echo "  -h, --help:                         Shows help output."
  echo "  -n, --namespace <namespace>:        Specify the Kubernetes namespace."
  echo "  -l, --log-dir <dir>:                Base directory to output the logs to. Defaults to /tmp."
  echo "  -D, --discard-logs:                 Do not collect the logs when deleting an instance."
  echo "      --keep-pvs:                     Skip the wiping and reclaiming of released Persistent Volumes after namespace cleanup."
  echo
  exit 1
}

save_logs() {
  namespace="$1"
  log_dir="$2"

  # Temporary directory for logs
  tmpdir=$(mktemp --tmpdir="${log_dir}" -d -t "${namespace}-deletion-logs-XXXX")
  # Ensure tmp dir is always cleaned up
  add_trap 'rm -rf -- "$tmpdir"' EXIT
  mkdir -p "${tmpdir}/varlogs"
  echo "Collecting logs to ${tmpdir}"

  # We get all the pod details in one go so that we don't have to do too many kubectl calls
  pods=$(kubectl --namespace "${namespace}" get pods -o json)

  # Iterate over pods
  echo "${pods}" | jq -r '
    .items[]
    | [
        .metadata.name,
        (.metadata.labels["app.kubernetes.io/instance"] // ""),
        (.spec.containers | map(.name) | join(" "))
      ]
    | @tsv
  ' | while IFS=$'\t' read -r pod instance containers; do

    # Pod-level probe: cheap and avoids SIGPIPE/pipefail issues
    if ! kubectl -n "${namespace}" logs "${pod}" --limit-bytes=1 >/dev/null 2>&1; then
      echo "Pod: ${pod} failed to start. Logging describe output"
      kubectl -n "${namespace}" describe pod "${pod}" > "${tmpdir}/${pod}-describe.log"
      continue
    fi

    set -- ${containers}
    num_containers=$#

    for container in ${containers}; do
      # Name of the (sub)directory to output logs to
      output_dir="${pod}"
      [ "${num_containers}" -gt 1 ] && output_dir="${pod}-${container}"

      max_allowed_size=$((2 * 1024 * 1024 * 1024)) # 2 GB
      var_log_size=$(
        kubectl -n "${namespace}" exec "${pod}" -c "${container}" -- \
          du -sb /var/log 2>/dev/null | awk '{print $1}'
      )

      if (( var_log_size > max_allowed_size )); then
        echo "Contents of /var/log are too big: ${var_log_size} bytes" >&2
        kubectl -n "${namespace}" exec "${pod}" -c "${container}" -- du -h /var/log >&2
        echo "Failed to collect /var/log from pod ${pod}, container ${container}" >&2
        continue
      fi

      # Collect stdout logs
      kubectl -n "${namespace}" logs "${pod}" -c "${container}" > "${tmpdir}/${output_dir}.log"

      # Collect /var/log for any pod part of the eos or cta instances
      if [[ "${instance}" == "cta" || "${instance}" == "eos" ]]; then
        echo "Collecting /var/log from ${pod} - ${container}"
        mkdir -p "${tmpdir}/varlogs/${output_dir}"
        # Only tar part of the logs
        subdirs_to_tar=("cta" "eos" "tmp" "xrootd" "*/xrd_errors")
        existing_dirs=$(
          kubectl -n "${namespace}" exec "${pod}" -c "${container}" -- \
            bash -c "cd /var/log && find ${subdirs_to_tar[*]} -maxdepth 0 -type d 2>/dev/null || true"
        )

        kubectl -n "${namespace}" exec "${pod}" -c "${container}" -- \
          tar --warning=no-file-removed --ignore-failed-read -C /var/log -cf - ${existing_dirs} \
          | tar -C "${tmpdir}/varlogs/${output_dir}" -xf - \
          || echo "Failed to collect /var/log from pod ${pod}, container ${container}" >&2
        # Remove empty files and directories to prevent polluting the output logs
        find "${tmpdir}/varlogs/${output_dir}" -type d -empty -delete -o -type f -empty -delete
      fi
    done
  done

  # Compress /var/log contents
  echo "Compressing all /var/log contents into single archive"
  XZ_OPT='-0 -T0' tar --warning=no-file-removed --ignore-failed-read -C "${tmpdir}/varlogs" -Jcf "${tmpdir}/varlog.tar.xz" .
  # Clean up uncompressed files
  rm -rf "${tmpdir}/varlogs"

  # Save artifacts if running in CI
  if [[ -n "${CI_PIPELINE_ID}" ]]; then
    echo "Saving logs as artifacts"
    # Note that this directory must be in the repository so that they can be properly saved as artifacts
    mkdir -p "../../pod_logs/${namespace}"
    cp -r "${tmpdir}"/* "../../pod_logs/${namespace}"
    local CLIENT_POD="cta-client-0"
    kubectl -n "${namespace}" cp ${CLIENT_POD}:/root/trackerdb.db "../../pod_logs/${namespace}/trackerdb.db" -c client || echo "Failed to copy trackerdb.db"
  fi
}

reclaim_released_pvs() {
  wipe_namespace="$1"
  released_pvs=$(kubectl get pv -o json | jq -r \
    --arg ns "$wipe_namespace" \
    '.items[] | select(.status.phase == "Released" and .spec.claimRef.namespace == $ns) | .metadata.name')

  for pv in $released_pvs; do
    echo "Processing PV: $pv"

    path=$(kubectl get pv "$pv" -o jsonpath='{.spec.local.path}')
    if [[ -z "$path" ]]; then
      echo "  Skipping: no local path found (not a local volume?)"
      continue
    fi
    echo "  Found path: $path"

    if [[ -d "$path" ]]; then
      echo "  Wiping contents of $path"
      # We need sudo here as files in the mount path can be owned by root
      # Note that this requires explicit permission in the sudoers file to ensure the user executing this
      # Can remove the contents of these mount paths
      (
        shopt -s dotglob
        sudo rm -rf "${path:?}/"*
      )
    else
      echo "  Warning: $path does not exist on this node"
      continue
    fi

    # Remove claimRef to mark PV as Available again
    echo "  Removing claimRef from PV $pv"
    kubectl patch pv "$pv" --type=json -p='[{"op": "remove", "path": "/spec/claimRef"}]'
    echo "  PV $pv wiped and reclaimed successfully"
  done
}

delete_instance() {
  local log_dir=/tmp
  local collect_logs=true
  local wipe_pvs=true
  local namespace=""

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      -h | --help) usage ;;
      -n|--namespace)
        namespace="$2"
        shift ;;
      -D|--discard-logs) collect_logs=false ;;
      --keep-pvs) wipe_pvs=false ;;
      -l|--log-dir)
        log_dir="$2"
        test -d "${log_dir}" || die "ERROR: Log directory ${log_dir} does not exist"
        test -w "${log_dir}" || die "ERROR: Canot write to log directory ${log_dir}"
        shift ;;
      *)
        echo "Unsupported argument: $1"
        usage
        ;;
    esac
    shift
  done

  # Argument checks
  if [[ -z "${namespace}" ]]; then
    echo "Missing mandatory argument: -n | --namespace"
    usage
  fi

  if ! kubectl get namespace "$namespace" >/dev/null 2>&1; then
    echo "Namespace $namespace does not exist. Nothing to delete"
    exit 0
  fi
  echo "Namespace to be deleted:"
  kubectl get pods --namespace ${namespace}

  # Optional log collection
  if [[ "$collect_logs" = true ]]; then
    save_logs $namespace $log_dir
  else
    echo "Discarding logs for the current run"
  fi

  # Cleanup of old library values files:
  echo "Removing auto-generated /tmp/${namespace}-tapeservers-*-values.yaml files"
  rm -f /tmp/${namespace}-tapeservers-*-values.yaml

  # Delete the actual namespace
  echo "Deleting ${namespace} instance"
  kubectl delete namespace ${namespace} --now
  echo "Deletion finished"

  # Reclaim any PVs
  if [[ "$wipe_pvs" = true ]]; then
    reclaim_released_pvs $namespace
  else
    echo "Skipping reclaiming of released Persistent Volumes"
  fi
  # Clean up remaining cluster-level resources
  # These are only created when telemetry is enabled
  kubectl delete clusterrole otel-opentelemetry-collector --ignore-not-found
  kubectl delete clusterrolebinding otel-opentelemetry-collector --ignore-not-found
  kubectl delete clusterrole prometheus-server --ignore-not-found
  kubectl delete clusterrolebinding prometheus-server --ignore-not-found
  kubectl delete clusterrole prometheus-kube-state-metrics --ignore-not-found
  kubectl delete clusterrolebinding prometheus-kube-state-metrics --ignore-not-found
}

delete_instance "$@"
