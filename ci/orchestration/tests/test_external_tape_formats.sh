#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -euo pipefail

usage() { cat <<EOF 1>&2
Usage: $0 -n <namespace> -f <osm|enstore|enstore-large>
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

wait_for_drive_readable() {
  local timeout_seconds="${1:-30}"
  echo "Waiting for tape device ${device} to become readable (timeout ${timeout_seconds}s)..."
  taped_exec bash -c "
for i in \$(seq 1 ${timeout_seconds}); do
  if mt -f '${device}' rewind >/dev/null 2>&1 &&
     dd if='${device}' of=/dev/null bs=262144 count=1 >/dev/null 2>&1 &&
     mt -f '${device}' rewind >/dev/null 2>&1; then
    exit 0
  fi
  sleep 1
done
echo 'Tape device still not readable after ${timeout_seconds}s' >&2
mt -f '${device}' status || true
exit 1
"
}

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
  echo "Unloading loaded tape: drive ${drive} -> slot ${slot}" >&2
  rmcd_exec mtx -f /dev/smc unload "${slot}" "${drive}" >&2
done
}

verify_slot_full() {
  local slot="$1"
  local status
  status=$(rmcd_exec mtx -f /dev/smc status)
  if ! echo "$status" | grep -Eq "Storage Element ${slot}:Full"; then
    echo "Expected Storage Element ${slot} to be Full. Current changer status:" >&2
    echo "$status" >&2
    exit 1
  fi
}

select_taped_drive() {
  local pod dev control_path

  for pod in "${TAPED_PODS[@]}"; do
    dev=$(kubectl -n "${NAMESPACE}" exec "${pod}" -c cta-taped -- printenv DRIVE_DEVICE | tr -d '\r' | xargs || true)
    control_path=$(kubectl -n "${NAMESPACE}" exec "${pod}" -c cta-taped -- awk '$1 == "taped" && $2 == "DriveControlPath" { print $3; exit }' /etc/cta/cta-taped.conf | tr -d '\r' | xargs || true)
    if [[ -n "${dev}" && "${control_path}" =~ ^smc[0-9]+$ ]]; then
      CTA_TAPED_POD="${pod}"
      device="${dev}"
      drive_index="${control_path#smc}"
      return 0
    fi
  done

  echo "No taped pod advertises a usable DRIVE_DEVICE and DriveControlPath. Available taped pods:" >&2
  for pod in "${TAPED_PODS[@]}"; do
    dev=$(kubectl -n "${NAMESPACE}" exec "${pod}" -c cta-taped -- printenv DRIVE_DEVICE | tr -d '\r' | xargs || true)
    control_path=$(kubectl -n "${NAMESPACE}" exec "${pod}" -c cta-taped -- awk '$1 == "taped" && $2 == "DriveControlPath" { print $3; exit }' /etc/cta/cta-taped.conf | tr -d '\r' | xargs || true)
    echo "  ${pod}: DRIVE_DEVICE=${dev:-<unset>} DriveControlPath=${control_path:-<unset>}" >&2
  done
  exit 1
}

reload_loaded_tape() {
  echo "Querying changer state..."
  local status
  status=$(rmcd_exec mtx -f /dev/smc status)

  # Pair drive+slot from the same line
  local drive slot
  read -r drive slot < <(echo "${status}" | awk -v target_drive="${drive_index}" '
/Data Transfer Element/ && /:Full/ && /Storage Element/ && /Loaded/ {
  match($0, /Data Transfer Element ([0-9]+)/, d)
  match($0, /Storage Element ([0-9]+)/, s)
  if (d[1] == target_drive && s[1] != "") { print d[1], s[1]; exit }
}') || true

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

run_tape_format_test() {
  local prep_script="$1"
  local reader_test="$2"
  local remote_script="/root/${prep_script}"

  kubectl -n "${NAMESPACE}" cp "${prep_script}" "${CTA_RMCD_POD}:${remote_script}" -c cta-rmcd
  rmcd_exec bash "${remote_script}" "${device}" /dev/smc "${drive_index}"

  wait_for_drive_ready 30
  reload_loaded_tape
  wait_for_drive_readable 30

  taped_exec "${reader_test}" "${device_name}" "${device}"
}

NAMESPACE=""
TEST_FORMAT=""

while getopts "n:f:" o; do
  case "${o}" in
    n) NAMESPACE=${OPTARG} ;;
    f) TEST_FORMAT=${OPTARG} ;;
    *) usage ;;
  esac
done
shift $((OPTIND-1))

if [[ -z "${NAMESPACE}" ]]; then
  usage
fi
if [[ -z "${TEST_FORMAT}" ]]; then
  usage
fi

case "${TEST_FORMAT}" in
  osm)
    test_slot=1
    prep_script="read_osm_tape.sh"
    reader_test="cta-osmReaderTest"
    ;;
  enstore)
    test_slot=2
    prep_script="read_enstore_tape.sh"
    reader_test="cta-enstoreReaderTest"
    ;;
  enstore-large|enstore_large)
    test_slot=3
    prep_script="read_enstore_large_tape.sh"
    reader_test="cta-enstoreLargeReaderTest"
    ;;
  *)
    echo "Unsupported external tape format: ${TEST_FORMAT}" >&2
    usage
    ;;
esac

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

select_taped_drive
echo "Selected taped pod: ${CTA_TAPED_POD}; device ${device}; changer drive ${drive_index}"

unload_all_loaded_drives
verify_slot_full "${test_slot}"

echo "Installing cta systest rpms in ${CTA_TAPED_POD}..."
taped_exec bash -c "dnf -y install cta-integrationtests"

echo "Obtaining drive name"
device_name=$(taped_exec printenv DRIVE_NAME | tr -d '\r' | xargs)

if [[ -z "${device_name}" || -z "${device}" ]]; then
  echo "Missing DRIVE_NAME or resolved device path" >&2
  exit 1
fi

echo "Using device: ${device}; name ${device_name}"

echo "Running external tape format test: ${TEST_FORMAT}"
run_tape_format_test "${prep_script}" "${reader_test}"

exit 0
