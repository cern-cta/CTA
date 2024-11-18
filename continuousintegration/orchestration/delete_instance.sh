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
  echo "Deletes a given Kubernetes namespace and collects their logs."
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

collect_container_logs() {
  local namespace="$1"
  local pod="$2"
  local container="$3"
  local output_dir="$4"
  local output_prefix="$5"
  # Collect stdout logs
  kubectl --namespace ${namespace} logs ${pod} -c ${container} > ${output_dir}/${output_prefix}.log
  # Collect all logs from /var/log for the pods with the label "collect-varlog"
  label="collect-varlog"
  if kubectl get pod ${pod} -n ${namespace} -o jsonpath="{.metadata.labels.${label}}" | grep -q '.'; then
    kubectl cp ${namespace}/${pod}:/var/log ${output_dir}/varlog/${output_prefix}
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
    echo "Namespace $namespace does not exist. Skipping deletion"
    exit 0
  fi

  ###
  # Collect the logs?
  ###
  if [ $collect_logs == true ] ; then
    # First in a temporary directory so that we can get the logs on the gitlab runner if something bad happens
    # indeed if the system test fails, artifacts are not collected for the build
    tmpdir=$(mktemp --tmpdir=${log_dir} -d -t ${namespace}_deletion_logs_XXXX)
    echo "Collecting pod logs to ${tmpdir}"

    # Collect logs for each pod
    pods=$(kubectl --namespace ${namespace} get pods -o jsonpath='{.items[*].metadata.name}')
    for pod in $pods; do
      # If we cannot get logs, then log the output of kubectl describe on the pod
      if ! kubectl --namespace ${namespace} logs ${pod} > /dev/null 2>&1; then
        echo "Pod: ${pod} failed to start. Logging describe output"
        kubectl --namespace ${namespace} describe pod ${pod} > ${tmpdir}/${pod}-describe.log
        continue
      fi
      containers=$(kubectl --namespace ${namespace} get pod ${pod} -o jsonpath='{.spec.containers[*].name}')
      num_containers=$(echo $containers | wc -w)
      for container in $containers; do
        # Display backtraces if any
        for backtracefile in $(kubectl --namespace ${namespace} exec ${pod} -c ${container} -- bash -c \
                                        'find /var/log/tmp/ | grep core | grep bt$' 2>/dev/null); do
          echo "Found backtrace in pod ${pod} - container ${container}:"
          kubectl --namespace ${namespace} exec ${pod} -c ${container} -- cat ${backtracefile}
        done

        # Don't include the container name if there is only 1 container in the pod
        if [ $num_containers -gt 1 ]; then output_name=${pod}-${container}; else output_name=${pod}; fi
        # Collect stdout logs
        kubectl --namespace ${namespace} logs ${pod} -c ${container} > ${tmpdir}/${output_name}.log
        # Collect /var/log for pods with collect-logs label
        if kubectl get pod ${pod_name} -n ${namespace} -o jsonpath="{.metadata.labels.collect-logs}" | grep -q "true"; then
          kubectl cp -n ${namespace} ${pod}:${container}:/var/log ${tmpdir}/varlogs/${output_name} \
          || echo "Failed to collect /var/log from pod ${pod}, container ${container}"
        fi
      done
    done

    # At this point, we have all the /var/log/ contents in $tmpdir/varlogs
    XZ_OPT='-0 -T0' tar --warning=no-file-removed --ignore-failed-read -C $tmpdir/varlogs -Jcf ${tmpdir}/varlog.tar.xz .
    # Clean up uncompressed files
    rm -rf $tmpdir/varlogs/

    if [ -n "${CI_PIPELINE_ID}" ]; then
      # we are in the context of a CI run => save artifacts in the directory structure of the build
      echo "Saving logs as artifacts"
      mkdir -p ../../pod_logs/${namespace}
      cp -r ${tmpdir}/* ../../pod_logs/${namespace}
      kubectl -n ${namespace} cp client:/root/trackerdb.db ../../pod_logs/${namespace}/trackerdb.db
    fi
  else
    # Do not Collect logs
    echo "Discarding logs for the current run"
  fi

  # Remove old library values files:
  echo "Removing auto-generated /tmp/${namespace}-tapeservers-*-values.yaml files"
  rm -f /tmp/${namespace}-tapeservers-*-values.yaml

  echo "Deleting ${namespace} instance"
  kubectl delete namespace ${namespace}
}

delete_instance "$@"
