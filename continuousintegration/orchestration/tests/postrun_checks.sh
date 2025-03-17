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

usage() { cat <<EOF 1>&2
Usage: $0 -n <namespace>
EOF
exit 1
}

while getopts "n:q" o; do
    case "${o}" in
        n)
            NAMESPACE=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${NAMESPACE}" ]; then
    usage
fi

echo "Performing postrun checks"

general_errors=0
core_dump_counter=0
uncaught_exc_counter=0
# We get all the pod details in one go so that we don't have to do too many kubectl calls
pods=$(kubectl --namespace "${NAMESPACE}" get pods -o json | jq -r '.items[]')

# Iterate over pods
for pod in $(echo "${pods}" | jq -r '.metadata.name'); do
  # Check if logs are accessible
  if ! kubectl --namespace "${NAMESPACE}" logs "${pod}" > /dev/null 2>&1; then
    echo "Pod contents are not accessible: ${pod}"
    exit 1
  fi

  # Check for pod restarts
  restart_count=$(echo "${pods}" | jq -r "select(.metadata.name==\"${pod}\") | .status.containerStatuses[].restartCount" | awk '{s+=$1} END {print s}')
  if [[ "$restart_count" -gt 0 ]]; then
    echo "WARNING: Pod ${pod} has restarted ${restart_count} times."
  fi

  # Check for pod crashes/failures
  pod_status=$(echo "${pods}" | jq -r "select(.metadata.name==\"${pod}\") | .status.phase")
  if [ "${pod_status}" != "Succeeded" ] && [ "${pod_status}" != "Running" ] ; then
    echo "Error: ${pod} is in state ${pod_status}"
    general_errors=$((general_errors + 1))
    continue
  fi
  # Only pods that are running can be executed into
  if [[ "${pod_status}" != "Running" ]]; then
    continue
  fi

  # Get containers for the pod
  containers=$(echo "${pods}" | jq -r " select(.metadata.name==\"${pod}\") | .spec.containers[].name")
  for container in ${containers}; do
    # Check for core dumps
    # We ignore core dumps of xrdcp on the client pod (see #1113)
    coredumpfiles=$(kubectl --namespace "${NAMESPACE}" exec "${pod}" -c "${container}" -- \
                      bash -c "find /var/log/tmp/ -type f -name '*.core' 2>/dev/null \
                               | grep -v -E 'client-0-[0-9]+-xrdcp-.*\.core$'
                               || true")
    if [ -n "${coredumpfiles}" ]; then
      num_files=$(wc -l <<< "${coredumpfiles}")
      echo "Found ${num_files} core dump files in pod ${pod} - container ${container}"
      core_dump_counter=$((core_dump_counter + num_files))
    fi
    # Check for uncaught exceptions if there are logs in the /var/log/cta directory
    false_positives=("Cannot update status for drive VDSTK[^\s]*. Drive not found")
    exclude_patterns=""
    for pattern in "${false_positives[@]}"; do
        exclude_patterns+=" | grep -v -E '${pattern}'"
    done
    uncaught_exc=$(kubectl --namespace "${NAMESPACE}" exec "${pod}" -c "${container}" -- \
                  bash -c "cat /var/log/cta/* 2> /dev/null \
                           | grep -a '\"log_level\":\"ERROR\"' \
                           | grep uncaught -i \
                           ${exclude_patterns} \
                           || true")
    if [ -n "${uncaught_exc}" ]; then
      num_exc=$(wc -l <<< "${uncaught_exc}")
      echo "Found ${num_exc} uncaught exceptions in pod ${pod} - container ${container}"
      uncaught_exc_counter=$((uncaught_exc_counter + num_exc))
    fi
  done
done

echo "Summary:"
echo "Found ${core_dump_counter} core dumps."
echo "Found ${uncaught_exc_counter} uncaught exceptions."
if [ "${core_dump_counter}" -gt 0 ] || [ "${uncaught_exc_counter}" -gt 0 ] || [ "${general_errors}" -gt 0 ]; then
  echo "Failing..."
  exit 1
fi
echo "Success"
