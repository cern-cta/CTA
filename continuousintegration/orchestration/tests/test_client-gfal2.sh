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

echo "Preparing namespace for the tests"
  . prepare_tests.sh -n ${NAMESPACE}
if [ $? -ne 0 ]; then
  echo "ERROR: failed to prepare namespace for the tests"
  exit 1
fi

echo "Installing parallel"
kubectl -n ${NAMESPACE} exec client -- bash -c "yum -y install parallel"
kubectl -n ${NAMESPACE} exec client -- bash -c "echo 'will cite' | parallel --bibtex"

echo "Installing gfal2 utility"
kubectl -n ${NAMESPACE} exec client -- bash -c "sudo yum -y install gfal2-util"

echo
echo "Copying test scripts to client pod"
kubectl -n ${NAMESPACE} cp . client:/root/
kubectl -n ${NAMESPACE} cp grep_xrdlog_mgm_for_error.sh ctaeos:/root/

NB_FILES=10000
FILE_SIZE_KB=15
NB_PROCS=100

TEST_PRERUN=". /root/client_env "
TEST_POSTRUN=""

VERBOSE=1
if [[ $VERBOSE == 1 ]]; then
  TEST_PRERUN="tail -v -f /mnt/logs/tpsrv0*/rmcd/cta/cta-rmcd.log & export TAILPID=\$! && ${TEST_PRERUN}"
  TEST_POSTRUN=" && kill \${TAILPID} &> /dev/null"
fi

clientgfal2_options='-n ${NB_FILES} -s ${FILE_SIZE_KB} -p ${NB_PROCS} -d /eos/ctaeos/preprod -v -r -Z ${TEST_PROTOCOL}'

# Tests
# Check for xrd vesion as xrd gfal plugin only runs under xrd version 5.
V5=$(yum list installed | grep xrootd | grep -e ':5.')
if [[ ! -z ${V5} ]]; then
    TEST_PROTOCOL='root'
    echo "Installing gfal2-plugin-xrootd for xrd gfal tests."
    kubectl -n ${NAMESPACE} exec client -- bash -c "sudo yum -y install gfal2-plugin-xrootd"

    echo
    echo "Launching client-gfal2_ar.sh on client pod using ${TEST_PROTOCOL} protocol"
    echo " Archiving files: xrdcp as user1"
    echo " Retrieving files with gfal xrootd"

    kubectl -n ${NAMESPACE} exec client -- bash -c "${TEST_PRERUN} && /root/client-gfal2_ar.sh ${clientgfal2_options} && ${TEST_POSTRUN}" || exit 1
    kubectl -n ${NAMESPACE} exec ctaeos -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1
fi


# Test gfal http plugin.
#TEST_PROTOCOL='https'
# echo "Installing gfal2-plugin-http for http gfal test."
# kubectl -n ${NAMESPACE} exec client -- bash -c "sudo yum -y install gfal2-plugin-http"
#echo
#echo "Launching client-gfal2_ar.sh on client pod using ${TEST_PROTOCOL} protocol"
#echo " Archiving files: xrdcp as user1"
#echo " Retrieving files with gfal https"
#kubectl -n ${NAMESPACE} exec client -- bash -c "${TEST_PRERUN} &&  /root/client-gfal2_ar.sh ${clientgfal2_options} && ${TEST_POSTRUN}" || exit 1
#kubectl -n ${NAMESPACE} exec ctaeos -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1
