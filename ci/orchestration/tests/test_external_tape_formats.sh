#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

usage() { cat <<EOF 1>&2
Usage: $0 -n <namespace>
EOF
exit 1
}

NAMESPACE=""
CTA_TPSRV_POD="cta-tpsrv01-0"

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

# Install systest rpm that contains the osm reader test.
echo "Installing cta systest rpms in ${CTA_TPSRV_POD} - taped-0 container... "
kubectl -n ${NAMESPACE} exec ${CTA_TPSRV_POD} -c cta-taped-0 -- bash -c "dnf -y install cta-integrationtests"

# Get the device to be used.
echo "Obtaining drive device and name"
device_name=$(kubectl -n ${NAMESPACE} exec ${CTA_TPSRV_POD} -c cta-taped-0 -- ls /etc/cta/ | grep 'cta-taped.conf' | awk 'NR==1')
device_name="${device_name:10:-5}"
device=$(kubectl -n ${NAMESPACE} exec ${CTA_TPSRV_POD} -c cta-taped-0 -- cat /etc/cta/cta-taped.conf | grep DriveDevice | awk '{ print $3 }')
echo "Using device: ${device}; name ${device_name}"

# Copy and run the above a script to the rmcd pod to load osm tape
kubectl -n ${NAMESPACE} cp read_osm_tape.sh ${CTA_TPSRV_POD}:/root/read_osm_tape.sh -c cta-rmcd
kubectl -n ${NAMESPACE} exec ${CTA_TPSRV_POD} -c cta-rmcd -- bash -c "/bin/bash /root/read_osm_tape.sh ${device}"

# Run the test
kubectl -n ${NAMESPACE} exec ${CTA_TPSRV_POD} -c cta-taped-0 -- cta-osmReaderTest ${device_name} ${device} || exit 1

exit 0
