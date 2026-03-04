#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -euo pipefail

usage() { cat <<EOF 1>&2
Usage: $0 -n <namespace>
EOF
exit 1
}

get_single_pod_by_type() {
  local type="$1"
  local namespace="$2"

  mapfile -t pods < <(kubectl get pod -l "app.kubernetes.io/component=${type}" -n "${namespace}" \
    --no-headers -o custom-columns=":metadata.name")

  if (( ${#pods[@]} != 1 )); then
    echo "Expected exactly 1 pod for component '${type}' in namespace '${namespace}', found ${#pods[@]}:" >&2
    printf '  %s\n' "${pods[@]}" >&2
    exit 1
  fi

  echo "${pods[0]}"
}

wait_for_drive_ready() {
  local timeout_seconds="${1:-60}"
  echo "Waiting for tape device ${device} to become ready (timeout ${timeout_seconds}s)..."

  kubectl -n "${NAMESPACE}" exec "${CTA_TAPED_POD}" -c cta-taped -- bash -c "
set -e
for i in \$(seq 1 ${timeout_seconds}); do
  mt -f '${device}' status >/dev/null 2>&1 && exit 0
  sleep 1
done
echo 'Tape device still busy after ${timeout_seconds}s' >&2
mt -f '${device}' status || true
exit 1
"
}

reload_loaded_tape() {
  echo "Querying changer state..."

  local status
  status=$(kubectl -n "${NAMESPACE}" exec "${CTA_RMCD_POD}" -c cta-rmcd -- mtx -f /dev/smc status)

  # Extract drive+slot from the SAME "Full" line:
  # Data Transfer Element 0:Full (Storage Element 3 Loaded):VolumeTag = ...
  local drive slot
  read -r drive slot < <(echo "${status}" | awk '
    /Data Transfer Element/ && /:Full/ && /Storage Element/ && /Loaded/ {
      match($0, /Data Transfer Element ([0-9]+)/, a)
      match($0, /Storage Element ([0-9]+)/, b)
      if (a[1] != "" && b[1] != "") { print a[1], b[1]; exit }
    }
  ')

  if [[ -z "${drive:-}" || -z "${slot:-}" ]]; then
    echo "Failed to determine loaded drive/slot from changer status:" >&2
    echo "${status}" >&2
    exit 1
  fi

  echo "Loaded tape located in drive ${drive} from slot ${slot}"
  echo "Reloading tape to reset device state"

  # IMPORTANT: mtx syntax is SLOT first, then DRIVE.
  kubectl -n "${NAMESPACE}" exec "${CTA_RMCD_POD}" -c cta-rmcd -- mtx -f /dev/smc unload "${slot}" "${drive}"
  kubectl -n "${NAMESPACE}" exec "${CTA_RMCD_POD}" -c cta-rmcd -- mtx -f /dev/smc load   "${slot}" "${drive}"

  # Give the kernel/driver a moment to settle
  sleep 2
}

NAMESPACE=""

while getopts "n:" o; do
  case "${o}" in
    n) NAMESPACE=${OPTARG} ;;
    *) usage ;;
  esac
done
shift $((OPTIND-1))

if [[ -z "${NAMESPACE}" ]]; then
  usage
fi

CTA_RMCD_POD=$(get_single_pod_by_type rmcd "${NAMESPACE}")
CTA_TAPED_POD=$(get_single_pod_by_type taped "${NAMESPACE}")

echo "Installing cta systest rpms in ${CTA_TAPED_POD}..."
kubectl -n "${NAMESPACE}" exec "${CTA_TAPED_POD}" -c cta-taped -- bash -c "dnf -y install cta-integrationtests"

echo "Obtaining drive device and name"
device_name=$(kubectl -n "${NAMESPACE}" exec "${CTA_TAPED_POD}" -c cta-taped -- printenv DRIVE_NAME)
device=$(kubectl -n "${NAMESPACE}" exec "${CTA_TAPED_POD}" -c cta-taped -- printenv DRIVE_DEVICE)

if [[ -z "${device_name}" || -z "${device}" ]]; then
  echo "Missing DRIVE_NAME or DRIVE_DEVICE in taped container env" >&2
  exit 1
fi

echo "Using device: ${device}; name ${device_name}"

############################################################
# Enstore test
############################################################
kubectl -n "${NAMESPACE}" cp read_enstore_tape.sh "${CTA_RMCD_POD}:/root/read_enstore_tape.sh" -c cta-rmcd
kubectl -n "${NAMESPACE}" exec "${CTA_RMCD_POD}" -c cta-rmcd -- bash /root/read_enstore_tape.sh "${device}"

wait_for_drive_ready 30
reload_loaded_tape
wait_for_drive_ready 30

kubectl -n "${NAMESPACE}" exec "${CTA_TAPED_POD}" -c cta-taped -- cta-enstoreReaderTest "${device_name}" "${device}"

############################################################
# EnstoreLarge test
############################################################
kubectl -n "${NAMESPACE}" cp read_enstore_large_tape.sh "${CTA_RMCD_POD}:/root/read_enstore_large_tape.sh" -c cta-rmcd
kubectl -n "${NAMESPACE}" exec "${CTA_RMCD_POD}" -c cta-rmcd -- bash /root/read_enstore_large_tape.sh "${device}"

wait_for_drive_ready 30
reload_loaded_tape
wait_for_drive_ready 30

kubectl -n "${NAMESPACE}" exec "${CTA_TAPED_POD}" -c cta-taped -- cta-enstoreLargeReaderTest "${device_name}" "${device}"

############################################################
# OSM test
############################################################
kubectl -n "${NAMESPACE}" cp read_osm_tape.sh "${CTA_RMCD_POD}:/root/read_osm_tape.sh" -c cta-rmcd
kubectl -n "${NAMESPACE}" exec "${CTA_RMCD_POD}" -c cta-rmcd -- bash /root/read_osm_tape.sh "${device}"

wait_for_drive_ready 30
reload_loaded_tape
wait_for_drive_ready 30

kubectl -n "${NAMESPACE}" exec "${CTA_TAPED_POD}" -c cta-taped -- cta-osmReaderTest "${device_name}" "${device}"

exit 0