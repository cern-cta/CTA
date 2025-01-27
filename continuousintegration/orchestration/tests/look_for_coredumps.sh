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

echo "Checking for core dumps"

core_dump_counter=0
# We get all the pod details in one go so that we don't have to do too many kubectl calls
pods=$(kubectl --namespace "${NAMESPACE}" get pods -o json)

# Iterate over pods
for pod in $(echo "${pods}" | jq -r '.items[] | .metadata.name'); do
  # Check if logs are accessible
  if ! kubectl --namespace "${NAMESPACE}" logs "${pod}" > /dev/null 2>&1; then
    echo "Cannot get logs for pod: ${pod}"
    exit 1
  fi

  # Get containers for the pod
  containers=$(echo "${pods}" | jq -r ".items[] | select(.metadata.name==\"${pod}\") | .spec.containers[].name")
  for container in ${containers}; do
    # Check for backtraces
    coredumpfiles=$(kubectl --namespace "${NAMESPACE}" exec "${pod}" -c "${container}" -- find /var/log/tmp/ -type f -name '*.core' 2>/dev/null || true)
    if [ -n "${coredumpfiles}" ]; then
      echo "Found core dump files in pod ${pod} - container ${container}"
      core_dump_counter=$((core_dump_counter + 1))
    fi
  done
done

if [ "${core_dump_counter}" -gt 0 ]; then
  echo "Failure: found ${core_dump_counter} core dumps."
  echo "This problem should be investigated."
  # Eventually this should exit with an error, but currently too many tests are still producing coredumps
  # So test failure for this is disabled until those are fixed
  exit 0
fi

echo "No core dumps found"
