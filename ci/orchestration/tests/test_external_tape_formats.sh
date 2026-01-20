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

# Install systest rpm that contains the osm reader test.
echo "Installing cta systest rpms in ${CTA_TAPED_POD} - taped container... "
kubectl -n ${NAMESPACE} exec ${CTA_TAPED_POD} -c cta-taped -- bash -c "dnf -y install cta-integrationtests"

# Get the device to be used.
echo "Obtaining drive device and name"
device_name=$(kubectl -n ${NAMESPACE} exec ${CTA_TAPED_POD} -c cta-taped -- printenv DRIVE_NAME)
device=$(kubectl -n ${NAMESPACE} exec ${CTA_TAPED_POD} -c cta-taped -- printenv DRIVE_DEVICE)
echo "Using device: ${device}; name ${device_name}"

# Copy and run the above a script to the rmcd pod to load osm tape
kubectl -n ${NAMESPACE} cp read_osm_tape.sh ${CTA_RMCD_POD}:/root/read_osm_tape.sh -c cta-rmcd
kubectl -n ${NAMESPACE} exec ${CTA_RMCD_POD} -c cta-rmcd -- bash /root/read_osm_tape.sh ${device}

# Run the test
kubectl -n ${NAMESPACE} exec ${CTA_TAPED_POD} -c cta-taped -- cta-osmReaderTest ${device_name} ${device} || exit 1

# Prepare and validate Enstore tape sample
kubectl -n ${NAMESPACE} cp read_enstore_tape.sh ${CTA_RMCD_POD}:/root/read_enstore_tape.sh -c cta-rmcd
enstore_env=(env)
if [[ -n "${ENSTORE_SAMPLE_DIR:-}" ]]; then
  enstore_env+=("ENSTORE_SAMPLE_DIR=${ENSTORE_SAMPLE_DIR}")
fi
if [[ -n "${ENSTORE_PAYLOAD_BLOCK_SIZE:-}" ]]; then
  enstore_env+=("ENSTORE_PAYLOAD_BLOCK_SIZE=${ENSTORE_PAYLOAD_BLOCK_SIZE}")
fi
enstore_env+=("/bin/bash" "/root/read_enstore_tape.sh" "${device}")
kubectl -n ${NAMESPACE} exec ${CTA_RMCD_POD} -c cta-rmcd -- "${enstore_env[@]}"
kubectl -n ${NAMESPACE} exec ${CTA_TAPED_POD} -c cta-taped -- cta-enstoreReaderTest ${device_name} ${device} || exit 1

# Prepare and validate EnstoreLarge tape sample
kubectl -n ${NAMESPACE} cp read_enstore_large_tape.sh ${CTA_RMCD_POD}:/root/read_enstore_large_tape.sh -c cta-rmcd
enstore_large_env=(env)
if [[ -n "${ENSTORE_LARGE_SAMPLE_DIR:-}" ]]; then
  enstore_large_env+=("ENSTORE_LARGE_SAMPLE_DIR=${ENSTORE_LARGE_SAMPLE_DIR}")
fi
if [[ -n "${ENSTORE_LARGE_BLOCK_SIZE:-}" ]]; then
  enstore_large_env+=("ENSTORE_LARGE_BLOCK_SIZE=${ENSTORE_LARGE_BLOCK_SIZE}")
fi
enstore_large_env+=("/bin/bash" "/root/read_enstore_large_tape.sh" "${device}")
kubectl -n ${NAMESPACE} exec ${CTA_RMCD_POD} -c cta-rmcd -- "${enstore_large_env[@]}"
kubectl -n ${NAMESPACE} exec ${CTA_TAPED_POD} -c cta-taped -- cta-enstoreLargeReaderTest ${device_name} ${device} || exit 1

exit 0
