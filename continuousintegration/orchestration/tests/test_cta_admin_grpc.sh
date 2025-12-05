#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2024 CERN
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

CLIENT_POD="cta-client-0"
CTA_CLI_POD="cta-cli-0"
echo
echo "Copying gRPC test scripts to client pod..."
kubectl -n "${NAMESPACE}" cp test_cta_admin_grpc_auth.sh ${CLIENT_POD}:/root/ -c client || exit 1
kubectl -n "${NAMESPACE}" cp grpc_obtain_jwt.sh ${CLIENT_POD}:/root/ -c client || exit 1
kubectl -n "${NAMESPACE}" cp grpc_obtain_jwt.sh ${CTA_CLI_POD}:/root/ -c cta-cli || exit 1

echo "Preparing namespace for the gRPC authentication tests"
. prepare_tests.sh -n "${NAMESPACE}"
if [ $? -ne 0 ]; then
  echo "ERROR: failed to prepare namespace for the tests"
  exit 1
fi

echo
echo "Launching test_cta_admin_grpc_auth.sh on client pod..."
kubectl -n "${NAMESPACE}" exec ${CLIENT_POD} -c client -- bash /root/test_cta_admin_grpc_auth.sh || exit 1

echo
echo "Collecting test results and logs..."
kubectl -n "${NAMESPACE}" cp ${CLIENT_POD}:/tmp/cta-admin-grpc-auth-test.log -c client ../../../pod_logs/"${NAMESPACE}"/cta-admin-grpc-auth.log || echo "Warning: Could not copy log file"

echo
echo "cta-admin-grpc authentication test completed successfully"
