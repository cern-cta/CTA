#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

usage() { cat <<EOF 1>&2
Usage: $0 -n <namespace>
EOF
exit 1
}

declare -a TAPED_STATEFULSETS=()
declare -A TAPED_REPLICA_COUNTS=()

get_pods_by_type() {
  local type="$1"
  local namespace="$2"
  kubectl get pod -l app.kubernetes.io/component=$type -n $namespace --no-headers -o custom-columns=":metadata.name"
}

get_drive_index_from_device() {
  local device_path="$1"
  local base
  base=$(basename "$device_path")
  if [[ "$base" =~ ([0-9]+)$ ]]; then
    echo "${BASH_REMATCH[1]}"
    return 0
  fi
  echo "WARNING: Could not derive drive index from device ${device_path}, defaulting to 0" >&2
  echo "0"
}

unload_loaded_tape() {
  local namespace="$1"
  local pod="$2"
  local changer="${3:-/dev/smc}"
  local drive_slot="${4:-0}"
  local status
  status=$(kubectl -n "$namespace" exec "$pod" -c cta-rmcd -- mtx -f "$changer" status 2>/dev/null || true)
  echo "$status"
  local loaded_slot
  loaded_slot=$(echo "$status" | awk -v drive="$drive_slot" '
    match($0, /Data Transfer Element ([0-9]+):Full/, m) {
      if (m[1] == drive && match($0, /Storage Element ([0-9]+)/, s)) {
        print s[1]
      }
    }')
  if [[ -n "$loaded_slot" ]]; then
    kubectl -n "$namespace" exec "$pod" -c cta-rmcd -- mtx -f "$changer" unload "$loaded_slot" "$drive_slot" || true
  fi
}

get_taped_app_name() {
  local namespace="$1"
  local pod="$2"
  kubectl -n "$namespace" get pod "$pod" -o jsonpath='{.metadata.labels.app\.kubernetes\.io/name}'
}

get_taped_pod_by_app_name() {
  local namespace="$1"
  local app_name="$2"
  kubectl -n "$namespace" get pod -l app.kubernetes.io/name="$app_name" --no-headers -o custom-columns=":metadata.name" | head -1
}

pause_all_taped() {
  local namespace="$1"
  local line

  TAPED_STATEFULSETS=()
  TAPED_REPLICA_COUNTS=()

  while read -r line; do
    local name
    local replicas
    name=$(echo "$line" | awk '{print $1}')
    replicas=$(echo "$line" | awk '{print $2}')
    if [[ -n "$name" ]]; then
      TAPED_STATEFULSETS+=("$name")
      TAPED_REPLICA_COUNTS["$name"]="${replicas:-1}"
    fi
  done < <(kubectl -n "$namespace" get statefulset -l app.kubernetes.io/component=taped --no-headers -o custom-columns=":metadata.name,:spec.replicas")

  if [[ ${#TAPED_STATEFULSETS[@]} -eq 0 ]]; then
    echo "WARNING: No taped statefulsets found to pause"
    return 0
  fi

  echo "Pausing taped statefulsets: ${TAPED_STATEFULSETS[*]}"
  for ss in "${TAPED_STATEFULSETS[@]}"; do
    kubectl -n "$namespace" scale statefulset "$ss" --replicas=0
  done
  kubectl -n "$namespace" wait --for=delete pod -l app.kubernetes.io/component=taped --timeout=300s || true
  sleep 2
  TAPED_PAUSED=true
}

resume_all_taped() {
  local namespace="$1"

  if [[ ${#TAPED_STATEFULSETS[@]} -eq 0 ]]; then
    return 0
  fi

  for ss in "${TAPED_STATEFULSETS[@]}"; do
    local replicas
    replicas="${TAPED_REPLICA_COUNTS[$ss]:-1}"
    echo "Resuming taped statefulset ${ss} to ${replicas} replicas"
    kubectl -n "$namespace" scale statefulset "$ss" --replicas="${replicas}"
  done
  kubectl -n "$namespace" wait --for=condition=Ready pod -l app.kubernetes.io/component=taped --timeout=300s
}

resume_taped_if_paused() {
  if [[ "${TAPED_PAUSED}" == "true" ]]; then
    resume_all_taped "${NAMESPACE}"
  fi
}

install_integrationtests() {
  local namespace="$1"
  local pod="$2"
  local container="$3"
  echo "Installing cta systest rpms in ${pod} - ${container} container... "
  kubectl -n ${namespace} exec ${pod} -c ${container} -- bash -c "dnf -y install cta-integrationtests"
}

wait_for_device_free() {
  local namespace="$1"
  local pod="$2"
  local container="$3"
  local device_path="$4"
  local max_attempts="${5:-30}"
  local kill_busy="${6:-false}"
  local attempt=1

  while true; do
    if kubectl -n "${namespace}" exec "${pod}" -c "${container}" -- \
      bash -c "dd if=${device_path} of=/dev/null bs=1 count=0 >/dev/null 2>&1"; then
      return 0
    fi
    if (( attempt >= max_attempts )); then
      echo "Timed out waiting for tape device ${device_path} to become free" >&2
      return 1
    fi
    echo "Tape device busy, waiting before running reader test (${attempt}/${max_attempts})..."
    kubectl -n "${namespace}" exec "${pod}" -c "${container}" -- bash -c "\
      if command -v fuser >/dev/null 2>&1; then fuser -v ${device_path} || true; fi; \
      if command -v lsof >/dev/null 2>&1; then lsof ${device_path} || true; fi" || true
    if [[ "${kill_busy}" == "true" ]]; then
      kubectl -n "${namespace}" exec "${pod}" -c "${container}" -- bash -c "\
        if command -v fuser >/dev/null 2>&1; then fuser -k ${device_path} || true; fi" || true
    fi
    sleep 2
    attempt=$((attempt + 1))
  done
}

refresh_taped_context() {
  local namespace="$1"
  local app_name="$2"
  CTA_TAPED_POD=$(get_taped_pod_by_app_name "$namespace" "$app_name")
  if [[ -z "$CTA_TAPED_POD" ]]; then
    echo "ERROR: Unable to find taped pod for ${app_name}" >&2
    exit 1
  fi

  install_integrationtests "${namespace}" "${CTA_TAPED_POD}" "cta-taped"
  echo "Obtaining drive device and name"
  device_name=$(kubectl -n ${namespace} exec ${CTA_TAPED_POD} -c cta-taped -- printenv DRIVE_NAME)
  device=$(kubectl -n ${namespace} exec ${CTA_TAPED_POD} -c cta-taped -- printenv DRIVE_DEVICE)
  resolved_device=$(kubectl -n ${namespace} exec ${CTA_TAPED_POD} -c cta-taped -- \
    bash -c "readlink -f \"${device}\" 2>/dev/null || echo \"${device}\"")
  DRIVE_INDEX=$(get_drive_index_from_device "${resolved_device}")
  echo "Using device: ${device}; name ${device_name}; drive index ${DRIVE_INDEX}"
}

NAMESPACE=""

while getopts "n:" o; do
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

if [[ -z "${NAMESPACE}" ]]; then
  usage
fi

TAPED_PAUSED=false
trap resume_taped_if_paused EXIT

CTA_RMCD_POD=$(get_pods_by_type rmcd $NAMESPACE | head -1)
CTA_TAPED_POD=$(get_pods_by_type taped $NAMESPACE | head -1)
CTA_TAPED_APP_NAME=$(get_taped_app_name "$NAMESPACE" "$CTA_TAPED_POD")
if [[ -z "$CTA_TAPED_APP_NAME" ]]; then
  echo "ERROR: Unable to determine taped app name from pod ${CTA_TAPED_POD}" >&2
  exit 1
fi

refresh_taped_context "$NAMESPACE" "$CTA_TAPED_APP_NAME"
install_integrationtests "${NAMESPACE}" "${CTA_RMCD_POD}" "cta-rmcd"

# Prepare Enstore tape sample
unload_loaded_tape "${NAMESPACE}" "${CTA_RMCD_POD}" "/dev/smc" "${DRIVE_INDEX}"
pause_all_taped "${NAMESPACE}"
wait_for_device_free "${NAMESPACE}" "${CTA_RMCD_POD}" "cta-rmcd" "${device}" 60 true || exit 1
kubectl -n ${NAMESPACE} cp read_enstore_tape.sh ${CTA_RMCD_POD}:/root/read_enstore_tape.sh -c cta-rmcd
kubectl -n ${NAMESPACE} exec ${CTA_RMCD_POD} -c cta-rmcd -- bash -c "FUSER_KILL_BUSY=1 /root/read_enstore_tape.sh ${device}" || exit 1
wait_for_device_free "${NAMESPACE}" "${CTA_RMCD_POD}" "cta-rmcd" "${device}" || exit 1
kubectl -n ${NAMESPACE} exec ${CTA_RMCD_POD} -c cta-rmcd -- cta-enstoreReaderTest ${device_name} ${device} || exit 1

# Prepare EnstoreLarge tape sample
unload_loaded_tape "${NAMESPACE}" "${CTA_RMCD_POD}" "/dev/smc" "${DRIVE_INDEX}"
kubectl -n ${NAMESPACE} cp read_enstore_large_tape.sh ${CTA_RMCD_POD}:/root/read_enstore_large_tape.sh -c cta-rmcd
wait_for_device_free "${NAMESPACE}" "${CTA_RMCD_POD}" "cta-rmcd" "${device}" 60 true || exit 1
kubectl -n ${NAMESPACE} exec ${CTA_RMCD_POD} -c cta-rmcd -- bash -c "FUSER_KILL_BUSY=1 /root/read_enstore_large_tape.sh ${device}" || exit 1
wait_for_device_free "${NAMESPACE}" "${CTA_RMCD_POD}" "cta-rmcd" "${device}" || exit 1
kubectl -n ${NAMESPACE} exec ${CTA_RMCD_POD} -c cta-rmcd -- cta-enstoreLargeReaderTest ${device_name} ${device} || exit 1

resume_taped_if_paused

# Copy and run the above a script to the rmcd pod to load osm tape
refresh_taped_context "$NAMESPACE" "$CTA_TAPED_APP_NAME"
kubectl -n ${NAMESPACE} cp read_osm_tape.sh ${CTA_RMCD_POD}:/root/read_osm_tape.sh -c cta-rmcd
kubectl -n ${NAMESPACE} exec ${CTA_RMCD_POD} -c cta-rmcd -- bash /root/read_osm_tape.sh ${device}

# Run the test
kubectl -n ${NAMESPACE} exec ${CTA_TAPED_POD} -c cta-taped -- cta-osmReaderTest ${device_name} ${device} || exit 1

exit 0
