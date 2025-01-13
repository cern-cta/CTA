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

CLIENT_POD="client-0"
EOS_MGM_POD="ctaeos"
EOS_MGM_HOST="ctaeos"
CTA_TPSRV_POD="cta-tpsrv01-0"

echo
echo "Copying test scripts to ${CLIENT_POD}, ${EOS_MGM_POD} and ${CTA_TPSRV_POD} pods."
kubectl -n ${NAMESPACE} cp . ${CLIENT_POD}:/root/ -c client
kubectl -n ${NAMESPACE} cp grep_xrdlog_mgm_for_error.sh "${EOS_MGM_POD}:/root/"
kubectl -n ${NAMESPACE} cp grep_eosreport_for_archive_metadata.sh "${EOS_MGM_POD}:/root/"
kubectl -n ${NAMESPACE} cp log_rotate_test.sh "${CTA_TPSRV_POD}:/root/"

NB_FILES=10000
FILE_SIZE_KB=15
NB_PROCS=100

echo
echo "Setting up environment for tests."
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "/root/client_setup.sh -n ${NB_FILES} -s ${FILE_SIZE_KB} -p ${NB_PROCS} -d /eos/ctaeos/preprod -v -r -c xrd" || exit 1

# Test are run under the cta user account which doesn't have a login
# option so to be able to export the test setup we need to source the file
# client_env (file generated in client_setup with all env varss and fucntions)
#
# Also, to show the output of tpsr-X rmcd to the logs we need to tail the files
# before every related script and kill it a the end. Another way to do this would
# require to change the stdin/out/err of the tail process and set//reset it
# at the beginning and end of each kubectl exec command.
TEST_PRERUN=". /root/client_env "
TEST_POSTRUN=""

VERBOSE=1
if [[ $VERBOSE == 1 ]]; then
  TEST_PRERUN="tail -v -f /mnt/logs/cta-tpsrv*/rmcd/cta/cta-rmcd.log & export TAILPID=\$! && ${TEST_PRERUN}"
  TEST_POSTRUN=" && kill \${TAILPID} &> /dev/null"
fi


echo "Setting up client pod for HTTPs REST API test"
echo " Copying CA certificates to client pod from ${EOS_MGM_POD} pod."
kubectl -n ${NAMESPACE} cp "${EOS_MGM_POD}:etc/grid-security/certificates/" /tmp/certificates/
kubectl -n ${NAMESPACE} cp /tmp/certificates ${CLIENT_POD}:/etc/grid-security/ -c client
rm -rf /tmp/certificates

# We don'y care about the tapesrv logs so we don't need the TEST_[PRERUN|POSTRUN].
# We just test the .well-known/wlcg-tape-rest-api endpoint and REST API compliance
# with the specification.
echo " Launching client_rest_api.sh on client pod"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/client_rest_api.sh || exit 1

# Note that this test simply tests whether the base64 encoded string ends up in the eos report logs verbatim
TEST_METADATA=$(echo "{\"scheduling_hints\": \"test 4\"}" | base64)
echo " Launching client_archive_metadata.sh on client pod"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/client_archive_metadata.sh ${TEST_METADATA} || exit 1
echo " Launching grep_eosreport_for_archive_metadata.sh on ${EOS_MGM_POD} pod"
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -- bash /root/grep_eosreport_for_archive_metadata.sh ${TEST_METADATA} || exit 1

echo
echo "Launching immutable file test on client pod"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} && echo yes | cta-immutable-file-test root://${EOS_MGM_HOST}/\${EOS_DIR}/immutable_file ${TEST_POSTRUN} || die 'The cta-immutable-file-test failed.'" || exit 1

echo
echo "Launching client_simple_ar.sh on client pod"
echo " Archiving file: xrdcp as user1"
echo " Retrieving it as poweruser1"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} && /root/client_simple_ar.sh ${TEST_POSTRUN}" || exit 1
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo
echo " Launching client_timestamp.sh on client pod"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} && /root/client_timestamp.sh ${TEST_POSTRUN}" || exit 1

echo
echo "Track progress of test"
(kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c ". /root/client_env && /root/progress_tracker.sh 'archive retrieve evict abort delete'"
)&
TRACKER_PID=$!

echo
echo "Launching client_archive.sh on client pod"
echo " Archiving ${NB_FILES} files of ${FILE_SIZE_KB}kB each"
echo " Archiving files: xrdcp as user1"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} && /root/client_archive.sh ${TEST_POSTRUN}" || exit 1

kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo "###"
echo "Sleeping 10 seconds to allow MGM-FST communication to settle after disk copy deletion."
sleep 10
echo "###"

echo
echo "Launching client_retrieve.sh on client pod"
echo " Retrieving files: xrdfs as poweruser1"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} && /root/client_retrieve.sh ${TEST_POSTRUN}" || exit 1

kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo
echo "Launching client_evict.sh on client pod"
echo " Evicting files: xrdfs as poweruser1"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} && /root/client_evict.sh ${TEST_POSTRUN}" || exit 1

kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo
echo "Launching client_abortPrepare.sh on client pod"
echo "  Retrieving files: xrdfs as poweruser1"
echo "  Aborting prepare: xrdfs as poweruser1"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} && /root/client_abortPrepare.sh ${TEST_POSTRUN}" || exit 1

kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo
echo "Launching client_delete.sh on client pod"
echo " Deleting files:"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} && /root/client_delete.sh ${TEST_POSTRUN}" || exit 1
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo "$(date +%s): Waiting for tracker process to finish. "
wait "${TRACKER_PID}"
if [[ $? == 1 ]]; then
  echo "Some files were lost during tape workflow."
 exit 1
fi

echo
echo "Launching client_multiple_retrieve.sh on client pod"
echo " Archiving file: xrdcp as user1"
echo " Retrieving it as poweruser1"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/client_multiple_retrieve.sh || exit 1
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo
echo "Launching idempotent_prepare.sh on client pod"
echo " Archiving file: xrdcp as user1"
echo " Retrieving it as poweruser1"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/idempotent_prepare.sh || exit 1
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo
echo "Launching delete_on_closew_error.sh on client pod"
echo " Archiving file: xrdcp as user1"
echo " Retrieving it as poweruser1"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/delete_on_closew_error.sh || exit 1
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo
echo "Launching archive_zero_length_file.sh on client pod"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/archive_zero_length_file.sh || exit 1
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo
echo "Launching try_evict_before_archive_completed.sh on client pod"
echo " Archiving file: xrdcp as user1"
echo " Retrieving it as poweruser1"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/try_evict_before_archive_completed.sh || exit 1
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

# TODO: Remove the stagerrm tests once the command is removed from EOS
echo
echo "Launching stagerrm_tests.sh on client pod"
echo " Archiving file: xrdcp as user1"
echo " Retrieving it as poweruser1"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/stagerrm_tests.sh || exit 1
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo
echo "Launching evict_tests.sh on client pod"
echo " Archiving file: xrdcp as user1"
echo " Retrieving it as poweruser1"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/evict_tests.sh || exit 1
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

setup_tapes_for_multicopy_test

echo
echo "Launching retrieve_queue_cleanup.sh on client pod"
echo " Archiving file: xrdcp as user1"
echo " Retrieving it as poweruser1"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/retrieve_queue_cleanup.sh || exit 1
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo
echo "Launching log_rotate_test.sh on ${CTA_TPSRV_POD} pod"
kubectl -n ${NAMESPACE} exec ${CTA_TPSRV_POD} -- bash /root/log_rotate_test.sh || exit 1

exit 0
