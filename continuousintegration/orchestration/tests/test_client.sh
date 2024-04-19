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

echo
echo "Copying test scripts to client pod."
kubectl -n ${NAMESPACE} cp . client:/root/
kubectl -n ${NAMESPACE} cp grep_xrdlog_mgm_for_error.sh ctaeos:/root/

NB_FILES=10000
FILE_SIZE_KB=15
NB_PROCS=100

echo
echo "Setting up environment for tests."
kubectl -n ${NAMESPACE} exec client -- bash -c "/root/client_setup.sh -n ${NB_FILES} -s ${FILE_SIZE_KB} -p ${NB_PROCS} -d /eos/ctaeos/preprod -v -r -c xrd" || exit 1

# Test are run under the cta user account which doesn't have a login
# option so to be able to export the test setup we need to source the file
# client_env (file generated in client_setup with all env varss and fucntions)
#
# Also, to show the output of tpsrv0X rmcd to the logs we need to tail the files
# before every related script and kill it a the end. Another way to do this would
# require to change the stdin/out/err of the tail process and set//reset it
# at the beginning and end of each kubectl exec command.
TEST_PRERUN=". /root/client_env "
TEST_POSTRUN=""

VERBOSE=1
if [[ $VERBOSE == 1 ]]; then
  TEST_PRERUN="tail -v -f /mnt/logs/tpsrv0*/rmcd/cta/cta-rmcd.log & export TAILPID=\$! && ${TEST_PRERUN}"
  TEST_POSTRUN=" && kill \${TAILPID} &> /dev/null"
fi

echo
echo "Launching client_archive.sh on client pod"
echo " Archiving ${NB_FILES} files of ${FILE_SIZE_KB}kB each"
echo " Archiving files: xrdcp as user1"
kubectl -n ${NAMESPACE} exec client -- bash -c "${TEST_PRERUN} && /root/client_archive.sh ${TEST_POSTRUN}" || exit 1

kubectl -n ${NAMESPACE} exec ctaeos -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo "###"
echo "Sleeping 10 seconds to allow MGM-FST communication to settle after disk copy deletion."
sleep 10
echo "###"

echo
echo "Launching client_retrieve.sh on client pod"
echo " Retrieving files: xrdfs as poweruser1"
kubectl -n ${NAMESPACE} exec client -- bash -c "${TEST_PRERUN} && /root/client_retrieve.sh ${TEST_POSTRUN}" || exit 1

kubectl -n ${NAMESPACE} exec ctaeos -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo
echo "Launching client_evict.sh on client pod"
echo " Evicting files: xrdfs as poweruser1"
kubectl -n ${NAMESPACE} exec ctaeos -- bash -c 'eos debug debug "*"'
kubectl -n ${NAMESPACE} exec client -- bash -c "${TEST_PRERUN} && /root/client_evict.sh ${TEST_POSTRUN}" || exit 1
kubectl -n ${NAMESPACE} exec ctaeos -- bash -c 'eos debug info "*"'

kubectl -n ${NAMESPACE} exec ctaeos -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo
echo "Launching client_delete.sh on client pod"
echo " Deleting files:"
kubectl -n ${NAMESPACE} exec client -- bash -c "${TEST_PRERUN} && /root/client_delete.sh ${TEST_POSTRUN}" || exit 1
kubectl -n ${NAMESPACE} exec ctaeos -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1


echo "Launching check_for_errno.py"
kubectl -n ${NAMESPACE} cp check_for_errno.py ctaeos:/root/check_for_errno.py
kubectl -n ${NAMESPACE} exec ctaeos -- python3 /root/check_for_errno.py || exit 1

exit 0
