#!/bin/bash

# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2021-2024 CERN
# @license        This program is free software: you can redistribute it and/or modify
#                 it under the terms of the GNU General Public License as published by
#                 the Free Software Foundation, either version 3 of the License, or
#                 (at your option) any later version.
#
#                 This program is distributed in the hope that it will be useful,
#                 but WITHOUT ANY WARRANTY; without even the implied warranty of
#                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#                 GNU General Public License for more details.
#
#                 You should have received a copy of the GNU General Public License
#                 along with this program.  If not, see <http://www.gnu.org/licenses/>.

source "$(dirname "${BASH_SOURCE[0]}")/../ci_helpers/log_wrapper.sh"

die() {
  echo "$@" 1>&2
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Finished $0 "
  echo "================================================================================"
  exit 1
}

usage() {
  echo "Deletes a given Kubernetes namespace and optionally collects the logs in said namespace."
  echo ""
  echo "Usage: $0 -n <namespace> [options]"
  echo ""
  echo "options:"
  echo "  -h, --help:                         Shows help output."
  echo "  -n, --namespace <namespace>:        Specify the Kubernetes namespace."
  echo "  -l, --log-dir <dir>:                Base directory to output the logs to. Defaults to /tmp."
  echo "  -D, --discard-logs:                 Do not collect the logs when deleting an instance."
  exit 1
}

save_logs() {
  namespace="$1"
  log_dir="$2"

  # Temporary directory for logs
  tmpdir=$(mktemp --tmpdir="${log_dir}" -d -t "${namespace}-deletion-logs-XXXX")
  mkdir -p "${tmpdir}/varlogs"
  echo "Collecting logs to ${tmpdir}"

  # We get all the pod details in one go so that we don't have to do too many kubectl calls
  pods=$(kubectl --namespace "${namespace}" get pods -o json)

  # Iterate over pods
  for pod in $(echo "${pods}" | jq -r '.items[] | .metadata.name'); do
    # Check if logs are accessible
    if ! kubectl --namespace "${namespace}" logs "${pod}" > /dev/null 2>&1; then
      echo "Pod: ${pod} failed to start. Logging describe output"
      kubectl --namespace "${namespace}" describe pod "${pod}" > "${tmpdir}/${pod}-describe.log"
      continue
    fi

    # Get containers for the pod
    containers=$(echo "${pods}" | jq -r ".items[] | select(.metadata.name==\"${pod}\") | .spec.containers[].name")
    num_containers=$(echo "${containers}" | wc -w)

    for container in ${containers}; do
      # Check for backtraces
      backtracefiles=$(kubectl --namespace "${namespace}" exec "${pod}" -c "${container}" -- find /var/log/tmp/ -type f -name '*.bt' 2>/dev/null || true)
      if [ -n "${backtracefiles}" ]; then
        echo "Found backtrace in pod ${pod} - container ${container}:"
        for backtracefile in ${backtracefiles}; do
          kubectl --namespace "${namespace}" exec "${pod}" -c "${container}" -- cat "${backtracefile}"
        done
      fi

      # Name of the (sub)directory to output logs to
      output_dir="${pod}"
      [ "${num_containers}" -gt 1 ] && output_dir="${pod}-${container}"

      # Collect stdout logs
      kubectl --namespace "${namespace}" logs "${pod}" -c "${container}" > "${tmpdir}/${output_dir}.log"

      # Collect /var/log for any pod with the label "collect-varlog"
      if echo "${pods}" | jq -e ".items[] | select(.metadata.name==\"${pod}\") | .metadata.labels[\"collect-varlog\"] == \"true\"" > /dev/null; then
        echo "Collecting /var/log from ${pod} - ${container}"
        mkdir -p "${tmpdir}/varlogs/${output_dir}"
        kubectl exec -n "${namespace}" "${pod}" -c "${container}" -- tar --warning=no-file-removed --ignore-failed-read -C /var/log -cf - . \
          | tar -C "${tmpdir}/varlogs/${output_dir}" -xf - \
          || echo "Failed to collect /var/log from pod ${pod}, container ${container}"
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
  if [ -n "${CI_PIPELINE_ID}" ]; then
    echo "Saving logs as artifacts"
    # Note that this directory must be in the repository so that they can be properly saved as artifacts
    mkdir -p "../../pod_logs/${namespace}"
    cp -r "${tmpdir}"/* "../../pod_logs/${namespace}"
    local CLIENT_POD="client-0"
    kubectl -n "${namespace}" cp ${CLIENT_POD}:/root/trackerdb.db "../../pod_logs/${namespace}/trackerdb.db" -c client || echo "Failed to copy trackerdb.db"
    # Prevent polluting the runner by cleaning up the original dir in /tmp
    rm -rf ${tmpdir}
  fi
}

delete_instance() {
  local log_dir=/tmp
  local collect_logs=true
  local namespace=""

  # Parse command line arguments
  while [[ "$#" -gt 0 ]]; do
    case $1 in
      -h | --help) usage ;;
      -n|--namespace)
        namespace="$2"
        shift ;;
      -D|--discard-logs) collect_logs=false ;;
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
  if [ -z "${namespace}" ]; then
    echo "Missing mandatory argument: -n | --namespace"
    usage
  fi

  if ! kubectl get namespace "$namespace" >/dev/null 2>&1; then
    echo "Namespace $namespace does not exist. Nothing to delete"
    exit 0
  fi

  # Optional log collection
  if [ "$collect_logs" = true ]; then
    save_logs $namespace $log_dir
  else
    echo "Discarding logs for the current run"
  fi

  # Cleanup of old library values files:
  echo "Removing auto-generated /tmp/${namespace}-tapeservers-*-values.yaml files"
  rm -f /tmp/${namespace}-tapeservers-*-values.yaml

  # Finally delete the actual namespace
  echo "Deleting ${namespace} instance"
  kubectl delete namespace ${namespace}
  echo "Deletion finished"
}

delete_instance "$@"
