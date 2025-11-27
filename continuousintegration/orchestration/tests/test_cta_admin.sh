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

if [[ -z "${NAMESPACE}" ]]; then
    usage
fi

if [[ -n "${error}" ]]; then
    echo -e "ERROR:\n${error}"
    exit 1
fi

echo "Preparing namespace for the tests"
  . prepare_tests.sh -n "${NAMESPACE}"
if [[ $? -ne 0 ]]; then
  echo "ERROR: failed to prepare namespace for the tests"
  exit 1
fi

CLIENT_POD="cta-client-0"
echo
echo "Copying test scripts to pods..."
kubectl -n "${NAMESPACE}" cp . ${CLIENT_POD}:/root/ -c client || exit 1

# Wait for script to rm the drive and restore it.
# TODO: This is not working correctly on CI but it works for buildtree. Not needed at the moment but when grpc frontend is ready we will need to restart restart tpsrv01-0 pod.
# (
# TIMEOUT=360
# SECS=0
# while [[ -z "${MESSAGE}" ]]; do
#   if test ${SECS} -eq ${TIMEOUT}; then
#     echo "ERROR. Timedout waiting for client to remove drive."
#     exit 1
#   fi
#
#   MESSAGE=$(kubectl -n ${NAMESPACE} exec ${CLIENT_POD} ls /root | grep deleted.txt)
#   SECS=$(( ${SECS} + 1 ))
# done
#
# kubectl -n ${NAMESPACE} delete pod tpsrv01-0
#
# SECS=0
# while test 1 == "$(kubectl -n ${NAMESPACE} get pods | grep tpsrv01-0 | wc -l)"; do
#   if test ${SECS} -eq ${TIMEOUT}; then
#     echo "HOST: ERROR. Timed out while waiting for tpsrv01-0 to be deleted."
#   fi
#   echo "HOST: Waiting for tpsrv01-0 to be deleted..."
#   sleep 1
#   SECS=$(( ${SECS} + 1 ))
# done
#
# # Find the yaml file used to generate the pod.
# tpsrv-1_yaml=$(find /tmp | grep pod-tpsrv01-0.yaml)
# kubectl create -f ${tpsrv-1_yaml} --namespace=${NAMESPACE}
#
# exit 0
# ) &

# TODO: Find another way to generate a Failed Request, at the moment the system cannot recover from doing this (it could) and breaks the CI.
# Wait for script to generate message file to corrupt the data on disk.
#(
#TIMEOUT=360
#SECONDS_PASSED=0
#while [[ -z "${MESSAGE}" ]]; do
#  if test ${SECONDS_PASSED} -eq ${TIMEOUT}; then
#   echo "ERROR. Timedout waiting for ctafrontendpod to generate file."
#   exit 1
#  fi

#  sleep 1
#  MESSAGE=$(kubectl -n ${NAMESPACE} exec ${CLIENT_POD} ls /root/ | grep message.txt)
#  SECONDS_PASSED=$(( ${SECONDS_PASSED} + 1 ))
#done

#vid=$(kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -- cat /root/message.txt)
# Backup tape.
#echo "HOST: Creating tape ${vid}TA backup."
#echo "HOST: $(ls /opt/mhvtl/)"
#cp /opt/mhvtl/${vid}TA/data /opt/mhvtl/${vid}TA/data.cp

# Corrupt the virtual tape.
#echo "HOST: Corrupting ${vid}TA"
#echo "1234" | dd of=/opt/mhvtl/${vid}TA/data seek=1 obs=50

# Inform the pod that the tape has been corrupted.
#kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -- bash -c 'echo "1" > /root/response.txt'

# Restore tape
#SECONDS_PASSED=0
#while [[ -z "${TEST_DONE}" ]]; do
#  if test ${SECONDS_PASSED} -eq ${TIMEOUT}; then
#    echo "ERROR. Timed out while waiting for client pod to get fail request."
#    exit 1
#  fi

#  sleep 1
#  TEST_DONE=$(kubectl -n ${NAMESPACE} exec ${CLIENT_POD} ls /root/ | grep done.txt)
#  SECONDS_PASSED=$(( ${SECONDS_PASSED} + 1 ))
#done


#cp /opt/mhvtl/${vid}TA/data.cp /opt/mhvtl/${vid}TA/data
#exit 0
#) &

echo
echo "Launching cta_admin.sh on client pod..."
kubectl -n "${NAMESPACE}" exec ${CLIENT_POD} -- bash +x /root/cta_admin.sh  || exit 1


# Add log file to Ci artifcats.
kubectl -n "${NAMESPACE}" cp ${CLIENT_POD}:/root/log -c client ../../../pod_logs/"${NAMESPACE}"/cta-admin_xrd.log


echo "cta-admin test completed successfully"
