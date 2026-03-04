#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -euo pipefail

usage() { cat <<EOF 1>&2
Usage: $0 -n <namespace>
EOF
exit 1
}

get_pods_by_type() {
  local type="$1"
  local namespace="$2"
  kubectl get pod -l "app.kubernetes.io/component=${type}" -n "$namespace" \
    --no-headers -o custom-columns=":metadata.name"
}

rmcd_exec() {
  kubectl -n "${NAMESPACE}" exec "${CTA_RMCD_POD}" -c cta-rmcd -- "$@"
}

taped_exec() {
  kubectl -n "${NAMESPACE}" exec "${CTA_TAPED_POD}" -c cta-taped -- "$@"
}

wait_for_drive_ready() {
  local timeout_seconds="${1:-30}"
  echo "Waiting for tape device ${device} to become ready (timeout ${timeout_seconds}s)..."
  taped_exec bash -c "
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

# Unload any loaded tapes, so probing does not accidentally match another loaded drive
unload_all_loaded_drives() {
  local status
  status=$(rmcd_exec mtx -f /dev/smc status)

  # For each "Data Transfer Element X:Full (Storage Element Y Loaded)" unload Y -> X
  echo "$status" | awk '
/Data Transfer Element/ && /:Full/ && /Storage Element/ && /Loaded/ {
  match($0, /Data Transfer Element ([0-9]+)/, d)
  match($0, /Storage Element ([0-9]+)/, s)
  if (d[1] != "" && s[1] != "") print s[1], d[1]
}' | while read -r slot drive; do
  echo "Unloading loaded tape: drive ${drive} -> slot ${slot}"
  rmcd_exec mtx -f /dev/smc unload "${slot}" "${drive}"
done
}

# Determine which /dev/nstX corresponds to changer "drive element 0"
# This matches your existing read_* scripts which use "load 3 0"
resolve_device_for_drive0_slot3() {
  # Verify slot 3 exists and is full
  local status
  status=$(rmcd_exec mtx -f /dev/smc status)
  if ! echo "$status" | grep -Eq 'Storage Element 3:Full'; then
    echo "Expected Storage Element 3 to be Full (your read_* scripts use slot 3). Current changer status:" >&2
    echo "$status" >&2
    exit 1
  fi

  unload_all_loaded_drives

  echo "Temporarily loading slot 3 into drive 0 to resolve the tape device node..."
  rmcd_exec mtx -f /dev/smc load 3 0

  # Probe /dev/nst* and find which one reports a valid position via mt tell
  local found=""
  found=$(rmcd_exec bash -c '
shopt -s nullglob
for d in /dev/nst*; do
  mt -f "$d" tell >/dev/null 2>&1 && { echo "$d"; exit 0; }
done
exit 1
') || {
    echo "Failed to resolve tape device for drive element 0. /dev/nst* probe did not find a loaded drive." >&2
    rmcd_exec mtx -f /dev/smc unload 3 0 || true
    exit 1
  }

  echo "Resolved drive element 0 -> ${found}"

  echo "Unloading back to slot 3..."
  rmcd_exec mtx -f /dev/smc unload 3 0

  echo "${found}"
}

select_taped_pod_for_device() {
  local target_device="$1"
  local pod dev

  for pod in "${TAPED_PODS[@]}"; do
    dev=$(kubectl -n "${NAMESPACE}" exec "${pod}" -c cta-taped -- printenv DRIVE_DEVICE | tr -d '\r' | xargs || true)
    if [[ -n "${dev}" && "${dev}" == "${target_device}" ]]; then
      echo "${pod}"
      return 0
    fi
  done

  echo "No taped pod advertises DRIVE_DEVICE='${target_device}'. Available taped pods/devices:" >&2
  for pod in "${TAPED_PODS[@]}"; do
    dev=$(kubectl -n "${NAMESPACE}" exec "${pod}" -c cta-taped -- printenv DRIVE_DEVICE | tr -d '\r' | xargs || true)
    echo "  ${pod}: ${dev}" >&2
  done
  exit 1
}

reload_loaded_tape() {
  echo "Querying changer state..."
  local status
  status=$(rmcd_exec mtx -f /dev/smc status)

  # Pair drive+slot from the same line
  local drive slot
  read -r drive slot < <(echo "${status}" | awk '
/Data Transfer Element/ && /:Full/ && /Storage Element/ && /Loaded/ {
  match($0, /Data Transfer Element ([0-9]+)/, d)
  match($0, /Storage Element ([0-9]+)/, s)
  if (d[1] != "" && s[1] != "") { print d[1], s[1]; exit }
}')

  if [[ -z "${drive:-}" || -z "${slot:-}" ]]; then
    echo "Failed to determine loaded drive/slot from changer status:" >&2
    echo "${status}" >&2
    exit 1
  fi

  echo "Loaded tape located in drive ${drive} from slot ${slot}"
  echo "Reloading tape to reset device state"

  # mtx uses: unload <slot> <drive>, load <slot> <drive>
  rmcd_exec mtx -f /dev/smc unload "${slot}" "${drive}"
  rmcd_exec mtx -f /dev/smc load   "${slot}" "${drive}"

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

CTA_RMCD_POD=$(get_pods_by_type rmcd "$NAMESPACE" | head -1)
mapfile -t TAPED_PODS < <(get_pods_by_type taped "$NAMESPACE")

if [[ -z "${CTA_RMCD_POD}" ]]; then
  echo "No rmcd pod found in namespace ${NAMESPACE}" >&2
  exit 1
fi
if (( ${#TAPED_PODS[@]} == 0 )); then
  echo "No taped pods found in namespace ${NAMESPACE}" >&2
  exit 1
fi

# Resolve which /dev/nstX corresponds to the drive element used by your read_* scripts (slot 3 -> drive 0)
device=$(resolve_device_for_drive0_slot3)

# Select the taped pod that actually owns that device
CTA_TAPED_POD=$(select_taped_pod_for_device "${device}")

echo "Selected taped pod: ${CTA_TAPED_POD}"

echo "Installing cta systest rpms in ${CTA_TAPED_POD}..."
taped_exec bash -c "dnf -y install cta-integrationtests"

echo "Obtaining drive name"
device_name=$(taped_exec printenv DRIVE_NAME | tr -d '\r' | xargs)

if [[ -z "${device_name}" || -z "${device}" ]]; then
  echo "Missing DRIVE_NAME or resolved device path" >&2
  exit 1
fi

echo "Using device: ${device}; name ${device_name}"

############################################################
# Enstore test
############################################################
kubectl -n "${NAMESPACE}" cp read_enstore_tape.sh "${CTA_RMCD_POD}:/root/read_enstore_tape.sh" -c cta-rmcd
rmcd_exec bash /root/read_enstore_tape.sh "${device}"

wait_for_drive_ready 30
reload_loaded_tape
wait_for_drive_ready 30

taped_exec cta-enstoreReaderTest "${device_name}" "${device}"

############################################################
# EnstoreLarge test
############################################################
kubectl -n "${NAMESPACE}" cp read_enstore_large_tape.sh "${CTA_RMCD_POD}:/root/read_enstore_large_tape.sh" -c cta-rmcd
rmcd_exec bash /root/read_enstore_large_tape.sh "${device}"

wait_for_drive_ready 30
reload_loaded_tape
wait_for_drive_ready 30

taped_exec cta-enstoreLargeReaderTest "${device_name}" "${device}"

############################################################
# OSM test
############################################################
kubectl -n "${NAMESPACE}" cp read_osm_tape.sh "${CTA_RMCD_POD}:/root/read_osm_tape.sh" -c cta-rmcd
rmcd_exec bash /root/read_osm_tape.sh "${device}"

wait_for_drive_ready 30
reload_loaded_tape
wait_for_drive_ready 30

taped_exec cta-osmReaderTest "${device_name}" "${device}"

exit 0