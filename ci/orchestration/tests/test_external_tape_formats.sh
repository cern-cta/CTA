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
  kubectl get pod -l app.kubernetes.io/component=$type -n "$namespace" --no-headers -o custom-columns=":metadata.name"
}

wait_for_drive_ready() {
  echo "Waiting for tape drive to become ready..."
  kubectl -n "${NAMESPACE}" exec "${CTA_TAPED_POD}" -c cta-taped -- bash -c "
  for i in {1..30}; do
    mt -f ${device} status >/dev/null 2>&1 && exit 0
    sleep 1
  done
  echo 'Tape device still busy after 30s'
  exit 1
  "
}

reload_loaded_tape() {

  echo "Querying changer state..."

  local status
  status=$(kubectl -n "${NAMESPACE}" exec "${CTA_RMCD_POD}" -c cta-rmcd -- mtx -f /dev/smc status)

  local drive slot

  drive=$(echo "$status" | awk '
    /Data Transfer Element/ && /Full/ {
      match($0,/Data Transfer Element ([0-9]+)/,a)
      print a[1]
      exit
    }')

  slot=$(echo "$status" | awk '
    /Data Transfer Element/ && /Full/ {
      match($0,/Storage Element ([0-9]+)/,a)
      print a[1]
      exit
    }')

  if [[ -z "${drive}" || -z "${slot}" ]]; then
    echo "Failed to determine drive/slot from changer state"
    echo "$status"
    exit 1
  fi

  echo "Loaded tape located in drive ${drive} from slot ${slot}"
  echo "Reloading tape to reset device state"

  kubectl -n "${NAMESPACE}" exec "${CTA_RMCD_POD}" -c cta-rmcd -- mtx -f /dev/smc unload "${drive}" "${slot}"
  kubectl -n "${NAMESPACE}" exec "${CTA_RMCD_POD}" -c cta-rmcd -- mtx -f /dev/smc load "${slot}" "${drive}"

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
CTA_TAPED_POD=$(get_pods_by_type taped "$NAMESPACE" | head -1)

echo "Installing cta systest rpms in ${CTA_TAPED_POD}..."
kubectl -n "${NAMESPACE}" exec "${CTA_TAPED_POD}" -c cta-taped -- bash -c "dnf -y install cta-integrationtests"

echo "Obtaining drive device and name"

device_name=$(kubectl -n "${NAMESPACE}" exec "${CTA_TAPED_POD}" -c cta-taped -- printenv DRIVE_NAME)
device=$(kubectl -n "${NAMESPACE}" exec "${CTA_TAPED_POD}" -c cta-taped -- printenv DRIVE_DEVICE)

echo "Using device: ${device}; name ${device_name}"

############################################################
# Enstore test
############################################################

kubectl -n "${NAMESPACE}" cp read_enstore_tape.sh "${CTA_RMCD_POD}:/root/read_enstore_tape.sh" -c cta-rmcd
kubectl -n "${NAMESPACE}" exec "${CTA_RMCD_POD}" -c cta-rmcd -- bash /root/read_enstore_tape.sh "${device}"

wait_for_drive_ready
reload_loaded_tape

kubectl -n "${NAMESPACE}" exec "${CTA_TAPED_POD}" -c cta-taped -- cta-enstoreReaderTest "${device_name}" "${device}"

############################################################
# EnstoreLarge test
############################################################

kubectl -n "${NAMESPACE}" cp read_enstore_large_tape.sh "${CTA_RMCD_POD}:/root/read_enstore_large_tape.sh" -c cta-rmcd
kubectl -n "${NAMESPACE}" exec "${CTA_RMCD_POD}" -c cta-rmcd -- bash /root/read_enstore_large_tape.sh "${device}"

wait_for_drive_ready
reload_loaded_tape

kubectl -n "${NAMESPACE}" exec "${CTA_TAPED_POD}" -c cta-taped -- cta-enstoreLargeReaderTest "${device_name}" "${device}"

############################################################
# OSM test
############################################################

kubectl -n "${NAMESPACE}" cp read_osm_tape.sh "${CTA_RMCD_POD}:/root/read_osm_tape.sh" -c cta-rmcd
kubectl -n "${NAMESPACE}" exec "${CTA_RMCD_POD}" -c cta-rmcd -- bash /root/read_osm_tape.sh "${device}"

wait_for_drive_ready
reload_loaded_tape

kubectl -n "${NAMESPACE}" exec "${CTA_TAPED_POD}" -c cta-taped -- cta-osmReaderTest "${device_name}" "${device}"

exit 0