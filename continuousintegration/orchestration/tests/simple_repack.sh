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

if [ -z "${NAMESPACE}" ]; then
    usage
fi

if [ ! -z "${error}" ]; then
    echo -e "ERROR:\n${error}"
    exit 1
fi

echo
echo "Launching a round trip repack request"
kubectl -n ${NAMESPACE} cp repack_systemtest.sh client:/root/repack_systemtest.sh
source ./repack_helper.sh

REPACK_BUFFER_URL=/eos/ctaeos/repack
echo "Creating the repack buffer URL directory (${REPACK_BUFFER_URL})"
kubectl -n ${NAMESPACE} exec ctaeos -- eos mkdir ${REPACK_BUFFER_URL}
kubectl -n ${NAMESPACE} exec ctaeos -- eos chmod 1777 ${REPACK_BUFFER_URL}

VID_TO_REPACK=$(getFirstVidContainingFiles)
if [ "$VID_TO_REPACK" != "null" ]
then
echo
  echo "Marking the tape ${VID_TO_REPACK} as full"
  kubectl -n ${NAMESPACE} exec ctacli -- cta-admin tape ch --vid ${VID_TO_REPACK} -f true
  echo "Launching the repack test on VID ${VID_TO_REPACK}"
  kubectl -n ${NAMESPACE} exec client -- bash /root/repack_systemtest.sh -v ${VID_TO_REPACK} -b ${REPACK_BUFFER_URL} -n ctasystest
else
  echo "No vid found to repack"
  exit 1
fi

echo "Reclaiming tape ${VID_TO_REPACK}"
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin tape reclaim --vid ${VID_TO_REPACK}

VID_TO_REPACK=$(getFirstVidContainingFiles)
if [ "$VID_TO_REPACK" != "null" ]
then
echo
  echo "Marking the tape ${VID_TO_REPACK} as full"
  kubectl -n ${NAMESPACE} exec ctacli -- cta-admin tape ch --vid ${VID_TO_REPACK} -f true
  echo "Launching the repack test on VID ${VID_TO_REPACK}"
  kubectl -n ${NAMESPACE} exec client -- bash /root/repack_systemtest.sh -v ${VID_TO_REPACK} -b ${REPACK_BUFFER_URL} -n ctasystest
else
  echo "No vid found to repack"
  exit 1
fi

echo "Reclaiming tape ${VID_TO_REPACK}"
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin tape reclaim --vid ${VID_TO_REPACK}
