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

exit 0
