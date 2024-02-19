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
tempdir=$(mktemp -d)
poddir=""
READ_OSM_SCRIPT="${tempdir}/read_osm_tape.sh"

while getopts "n:P:" o; do
  case "${o}" in
    n)
      NAMESPACE=${OPTARG}
      ;;
    P)
      poddir=${OPTARG}
      ;;
    *)
      usage
      ;;
  esac
done
shift $((OPTIND-1))

if [ -z "${NAMESPACE}" ]; then
  usage
fi

# Does kubernetes version supports `get pod --show-all=true`?
# use it for older versions and this is not needed for new versions of kubernetes
KUBECTL_DEPRECATED_SHOWALL=$(kubectl get pod --show-all=true >/dev/null 2>&1 && echo "--show-all=true")
test -z ${KUBECTL_DEPRECATED_SHOWALL} || echo "WARNING: you are running a old version of kubernetes and should think about updating it."

echo "
device=\"DEVICE\"
echo \"Device is \$device\"

# Load tape in a tapedrive
mtx -f /dev/smc status
mtx -f /dev/smc load 1 0
mtx -f /dev/smc status

# Get the device status where the tape is loaded and rewind it.
mt -f \$device status
mt -f \$device rewind

# The header files have 4 more bytes in the git file
truncate -s -4 /osm-mhvtl/L08033/L1
touch /osm-tape.img
dd if=/osm-mhvtl/L08033/L1 of=/osm-tape.img bs=32768
dd if=/osm-mhvtl/L08033/L2 of=/osm-tape.img bs=32768 seek=1
dd if=/osm-tape.img of=\$device bs=32768 count=2
dd if=/osm-mhvtl/L08033/file1 of=\$device bs=262144 count=202
dd if=/osm-mhvtl/L08033/file2 of=\$device bs=262144 count=202

mt -f \$device rewind
" &> ${READ_OSM_SCRIPT}

# Download OSM Tape from git
echo "${tempdir}/osm-mhvtl"
if [ ! -d "${tempdir}/osm-mhvtl" ] ; then
  yum install -y git git-lfs
  git lfs install || git lfs update --force
  git lfs clone https://gitlab.desy.de/mwai.karimi/osm-mhvtl.git ${tempdir}/osm-mhvtl
fi

# Get the device to be used.

device_name=$(kubectl -n ${NAMESPACE} exec tpsrv01 -c taped -- ls | grep -r 'cta-taped-.*\.conf' | awk 'NR==1')
device_name="${device_name:10:-5}"
device=$(kubectl -n ${NAMESPACE} exec tpsrv01 -c taped -- cat /etc/cta/cta-taped-${device_name}.conf | grep DriveDevice | awk '{ print $3 }')
sed -i "s#DEVICE#${device}#g" ${READ_OSM_SCRIPT}
sed -i "s#DEVICE_NAME_VALUE#${device_name}#g" ${poddir}/pod-externaltapetests.yaml
sed -i "s#DEVICE_PATH#${device}#g" ${poddir}/pod-externaltapetests.yaml

# Copy the above script to the pod rmcd to be run
kubectl -n ${NAMESPACE} cp ${tempdir}/osm-mhvtl tpsrv01:/osm-mhvtl -c rmcd
kubectl -n ${NAMESPACE} cp ${READ_OSM_SCRIPT} tpsrv01:/root/read_osm_tape.sh -c rmcd
kubectl -n ${NAMESPACE} exec tpsrv01 -c rmcd -- /bin/bash /root/read_osm_tape.sh

# Creation of the pod to run the tests
kubectl create -f "${poddir}/pod-externaltapetests.yaml" --namespace=${NAMESPACE}

echo -n "Waiting for externaltapetests"
for ((i=0; i<400; i++)); do
  echo -n "."
  kubectl -n ${NAMESPACE} get pod externaltapetests ${KUBECTL_DEPRECATED_SHOWALL} | egrep -q 'Completed|Error' && break
  sleep 1
done
echo "\n"

# Copy the logs outside
kubectl -n ${NAMESPACE} logs externaltapetests &> "../../pod_logs/${NAMESPACE}/externaltapetests.log"

# Cleanning
rm -rf ${tempdir}/osm-mhvtl
rm ${READ_OSM_SCRIPT}

exit 0
