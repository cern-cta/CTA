#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2025 CERN
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
PREPARE=1 # run prepare by default

usage() { cat <<EOF 1>&2
Usage: $0 -n <namespace> -q
   -q: do not run prepare
EOF
exit 1
}

while getopts "n:q" o; do
    case "${o}" in
        n)
            NAMESPACE=${OPTARG}
            ;;
        q)
            PREPARE=0
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

if [[ ${PREPARE} -eq 1 ]]; then
  echo "Preparing namespace for the tests"
    . prepare_tests.sh -n ${NAMESPACE}
  if [ $? -ne 0 ]; then
    echo "ERROR: failed to prepare namespace for the tests"
    exit 1
  fi
fi

CLIENT_POD="cta-client-0"
EOS_MGM_POD="eos-mgm-0"

NB_FILES=500
FILE_SIZE_KB=1
NB_PROCS=10

kubectl -n ${NAMESPACE} cp . ${CLIENT_POD}:/root/ -c client

# Need CTAADMIN_USER krb5
admin_kdestroy &>/dev/null
admin_kinit &>/dev/null

echo " Copying CA certificates to client pod from ${EOS_MGM_POD} pod."
kubectl -n ${NAMESPACE} cp "${EOS_MGM_POD}:etc/grid-security/certificates/" /tmp/certificates/ -c eos-mgm
kubectl -n ${NAMESPACE} cp /tmp/certificates ${CLIENT_POD}:/etc/grid-security/ -c client
rm -rf /tmp/certificates

echo
echo "Setting up environment for tests."
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "/root/client_setup.sh -n ${NB_FILES} -s ${FILE_SIZE_KB} -p ${NB_PROCS} -d /eos/ctaeos/preprod -r -c xrd" || exit 1

echo "Putting all tape drives up"
kubectl -n ${NAMESPACE} exec cta-cli-0 -- cta-admin dr up '.*'

echo
echo "Launching client_archive.sh on client pod"
echo " Archiving ${NB_FILES} files of ${FILE_SIZE_KB}kB each"
echo " Archiving files: xrdcp as user1"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c ". /root/client_env && /root/client_archive.sh" || exit 1

echo
echo "Launching client_retrieve.sh on client pod"
echo " Retrieving files: xrdfs as poweruser1"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c ". /root/client_env && /root/client_retrieve.sh" || exit 1

echo "Sleeping 20 seconds to give Prometheus time to scrape the metrics"
sleep 20

# Provide some telemetry metrics
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "/root/client_telemetry_summary.sh $NB_FILES" || exit 1
