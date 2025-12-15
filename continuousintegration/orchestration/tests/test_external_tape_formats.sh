#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2022 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.

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
