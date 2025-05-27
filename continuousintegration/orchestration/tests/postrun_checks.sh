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


set -euo pipefail

usage() { cat <<EOF 1>&2
Usage: $0 -n <namespace> [-w <whitelist-file-path>]
EOF
exit 1
}

WHITELIST_FILE=""

while getopts "n:w:q" o; do
    case "${o}" in
        n)
            NAMESPACE=${OPTARG}
            ;;
        w)
            WHITELIST_FILE=${OPTARG}
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
logged_error_counter=0
all_logged_errors=""

pods=$(kubectl --namespace "${NAMESPACE}" get pods -o json | jq -r '.items[]')

for pod in $(echo "${pods}" | jq -r '.metadata.name'); do
  if ! kubectl --namespace "${NAMESPACE}" logs "${pod}" > /dev/null 2>&1; then
    echo "Pod contents are not accessible: ${pod}"
    exit 1
  fi

  restart_count=$(echo "${pods}" | jq -r "select(.metadata.name==\"${pod}\") | .status.containerStatuses[].restartCount" | awk '{s+=$1} END {print s}')
  if [[ "$restart_count" -gt 0 ]]; then
    echo "WARNING: Pod ${pod} has restarted ${restart_count} times."
  fi

  pod_status=$(echo "${pods}" | jq -r "select(.metadata.name==\"${pod}\") | .status.phase")
  if [ "${pod_status}" != "Succeeded" ] && [ "${pod_status}" != "Running" ]; then
    echo "Error: ${pod} is in state ${pod_status}"
    general_errors=$((general_errors + 1))
    continue
  fi
  if [[ "${pod_status}" != "Running" ]]; then
    continue
  fi

  containers=$(echo "${pods}" | jq -r " select(.metadata.name==\"${pod}\") | .spec.containers[].name")
  for container in ${containers}; do
    coredumpfiles=$(kubectl --namespace "${NAMESPACE}" exec "${pod}" -c "${container}" -- \
                      bash -c "find /var/log/tmp/ -type f -name '*.core' 2>/dev/null \
                               | grep -v -E 'client-0-[0-9]+-xrdcp-.*\.core$' \
                               || true")
    if [ -n "${coredumpfiles}" ]; then
      num_files=$(wc -l <<< "${coredumpfiles}")
      echo "Found ${num_files} core dump files in pod ${pod} - container ${container}"
      core_dump_counter=$((core_dump_counter + num_files))
    fi

    # We hardcode ignore the error here for cta_admin because it occurs in the exceptionmessage
    # With the test rewrite, this should be done in a better way; or we should revisit whether this should not
    # be a fatal error instead (instead of an uncaught exception)
    logged_errors=$(kubectl --namespace "${NAMESPACE}" exec "${pod}" -c "${container}" -- \
                  bash -c "cat /var/log/cta/* 2> /dev/null \
                           | grep -a -E '\"log_level\":\"(ERROR|CRITICAL)\"' \
                           | grep -v -E 'Cannot update status for drive [A-Za-z0-9_-]+. Drive not found' \
                           || true")
    if [ -n "${logged_errors}" ]; then
      num_errors=$(wc -l <<< "${logged_errors}")
      all_logged_errors+="$logged_errors"$'\n'
      echo "Found ${num_errors} logged errors in pod ${pod} - container ${container}"
      logged_error_counter=$((logged_error_counter + num_errors))
    fi
  done
done

echo ""
echo "Summary of logged error messages:"
non_whitelisted_errors=0
if [ -n "${all_logged_errors}" ]; then
  while read -r count message; do
    if grep -Fxq "${message}" "${WHITELIST_FILE}"; then
      echo "(whitelisted) Count: ${count}, Message: \"${message}\""
    else
      echo "              Count: ${count}, Message: \"${message}\""
      non_whitelisted_errors=$(( non_whitelisted_errors + count ))
    fi
  done < <(echo "${all_logged_errors}" \
    | jq -r '.message' \
    | sort \
    | uniq -c \
    | sort -nr)
else
  echo "No logged errors found."
fi

echo ""
echo "Summary of other issues:"
echo "Found ${general_errors} general pod errors."
echo "Found ${core_dump_counter} core dumps."
echo "Found ${non_whitelisted_errors} logged errors not in the whitelist."

if [ "${core_dump_counter}" -gt 0 ] || [ "${non_whitelisted_errors}" -gt 0 ] || [ "${general_errors}" -gt 0 ]; then
  echo "Failing due to non-whitelisted errors or core dumps or general errors."
  exit 1
fi
echo "Success"

