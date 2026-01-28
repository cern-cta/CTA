#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

usage() { cat <<EOF 1>&2
Usage: $0 -n <namespace>
EOF
exit 1
}

get_pods_by_type() {
  local type="$1"
  local namespace="$2"
  kubectl get pod -l app.kubernetes.io/component=$type -n $namespace --no-headers -o custom-columns=":metadata.name"
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

pause_taped() {
  local namespace="$1"
  local app_name="$2"
  local pod
  pod=$(get_taped_pod_by_app_name "$namespace" "$app_name")
  if [[ -z "$pod" ]]; then
    echo "WARNING: Could not find taped pod for ${app_name} to pause"
    return 0
  fi
  echo "Pausing taped pod ${pod} (statefulset ${app_name}) to free tape device"
  kubectl -n "$namespace" scale statefulset "$app_name" --replicas=0
  kubectl -n "$namespace" wait --for=delete pod "$pod" --timeout=300s || true
  sleep 2
}

resume_taped() {
  local namespace="$1"
  local app_name="$2"
  echo "Resuming taped statefulset ${app_name}"
  kubectl -n "$namespace" scale statefulset "$app_name" --replicas=1
  kubectl -n "$namespace" wait --for=condition=Ready pod -l app.kubernetes.io/name="$app_name" --timeout=300s
}

refresh_taped_context() {
  local namespace="$1"
  local app_name="$2"
  CTA_TAPED_POD=$(get_taped_pod_by_app_name "$namespace" "$app_name")
  if [[ -z "$CTA_TAPED_POD" ]]; then
    echo "ERROR: Unable to find taped pod for ${app_name}" >&2
    exit 1
  fi

  echo "Installing cta systest rpms in ${CTA_TAPED_POD} - taped container... "
  kubectl -n ${namespace} exec ${CTA_TAPED_POD} -c cta-taped -- bash -c "dnf -y install cta-integrationtests"

  echo "Obtaining drive device and name"
  device_name=$(kubectl -n ${namespace} exec ${CTA_TAPED_POD} -c cta-taped -- printenv DRIVE_NAME)
  device=$(kubectl -n ${namespace} exec ${CTA_TAPED_POD} -c cta-taped -- printenv DRIVE_DEVICE)
  echo "Using device: ${device}; name ${device_name}"
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

CTA_RMCD_POD=$(get_pods_by_type rmcd $NAMESPACE | head -1)
CTA_TAPED_POD=$(get_pods_by_type taped $NAMESPACE | head -1)
CTA_TAPED_APP_NAME=$(get_taped_app_name "$NAMESPACE" "$CTA_TAPED_POD")
if [[ -z "$CTA_TAPED_APP_NAME" ]]; then
  echo "ERROR: Unable to determine taped app name from pod ${CTA_TAPED_POD}" >&2
  exit 1
fi

refresh_taped_context "$NAMESPACE" "$CTA_TAPED_APP_NAME"

# Copy and run the above a script to the rmcd pod to load osm tape
kubectl -n ${NAMESPACE} cp read_osm_tape.sh ${CTA_RMCD_POD}:/root/read_osm_tape.sh -c cta-rmcd
kubectl -n ${NAMESPACE} exec ${CTA_RMCD_POD} -c cta-rmcd -- bash /root/read_osm_tape.sh ${device}

# Run the test
kubectl -n ${NAMESPACE} exec ${CTA_TAPED_POD} -c cta-taped -- cta-osmReaderTest ${device_name} ${device} || exit 1

# Prepare Enstore tape sample
unload_loaded_tape "${NAMESPACE}" "${CTA_RMCD_POD}"
pause_taped "${NAMESPACE}" "${CTA_TAPED_APP_NAME}"
kubectl -n ${NAMESPACE} cp read_enstore_tape.sh ${CTA_RMCD_POD}:/root/read_enstore_tape.sh -c cta-rmcd
kubectl -n ${NAMESPACE} exec ${CTA_RMCD_POD} -c cta-rmcd -- bash /root/read_enstore_tape.sh ${device}
resume_taped "${NAMESPACE}" "${CTA_TAPED_APP_NAME}"
refresh_taped_context "$NAMESPACE" "$CTA_TAPED_APP_NAME"
kubectl -n ${NAMESPACE} exec ${CTA_TAPED_POD} -c cta-taped -- cta-enstoreReaderTest ${device_name} ${device} || exit 1

# Prepare EnstoreLarge tape sample
unload_loaded_tape "${NAMESPACE}" "${CTA_RMCD_POD}"
pause_taped "${NAMESPACE}" "${CTA_TAPED_APP_NAME}"
kubectl -n ${NAMESPACE} cp read_enstore_large_tape.sh ${CTA_RMCD_POD}:/root/read_enstore_large_tape.sh -c cta-rmcd
kubectl -n ${NAMESPACE} exec ${CTA_RMCD_POD} -c cta-rmcd -- bash /root/read_enstore_large_tape.sh ${device}
resume_taped "${NAMESPACE}" "${CTA_TAPED_APP_NAME}"
refresh_taped_context "$NAMESPACE" "$CTA_TAPED_APP_NAME"
kubectl -n ${NAMESPACE} exec ${CTA_TAPED_POD} -c cta-taped -- cta-enstoreLargeReaderTest ${device_name} ${device} || exit 1

exit 0
