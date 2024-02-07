#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2023 CERN
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

usage() {
  cat <<EOF 1>&2
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

if [ -n "${error}" ]; then
    echo -e "ERROR:\n${error}"
    exit 1
fi

echo "Preparing namespace for the tests"
  . prepare_tests.sh -n "${NAMESPACE}"
if [ $? -ne 0 ]; then
  echo "ERROR: failed to prepare namespace for the tests"
  exit 1
fi

echo
echo "Copying test scripts to pods..."
kubectl -n "${NAMESPACE}" cp . client:/root/ || exit 1

echo
echo "Launching cta-wfe-test on client pod..."
#kubectl -n "${NAMESPACE}" exec client -- bash +x /root/cta_admin.sh  || exit 1
kubectl -n "${NAMESPACE}" exec client -- bash +x /root/cta_wfe.sh  || exit 1



# Add log file to Ci artifcats.
#kubectl -n "${NAMESPACE}" cp client:/root/log ../../../pod_logs/"${NAMESPACE}"/cta-admin_xrd.log


echo "retrieve-job-completion-report test completed successfully"
