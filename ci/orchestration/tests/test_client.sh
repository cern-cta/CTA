#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
PREPARE=1 # run prepare by default

usage() { cat <<EOF 1>&2
Usage: $0 -n <namespace> -q
   -q: do not run prepare
EOF
exit 1
}

get_pods_by_type() {
  local type="$1"
  local namespace="$2"
  kubectl get pod -l app.kubernetes.io/component=$type -n $namespace --no-headers -o custom-columns=":metadata.name"
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

if [[ -z "${NAMESPACE}" ]]; then
    usage
fi

if [[ ! -z "${error}" ]]; then
    echo -e "ERROR:\n${error}"
    exit 1
fi

CTA_RMCD_POD=$(get_pods_by_type rmcd $NAMESPACE | head -1)
CTA_TAPED_POD=$(get_pods_by_type taped $NAMESPACE | head -1)
CTA_FRONTEND_POD=$(get_pods_by_type frontend $NAMESPACE | head -1)
CLIENT_POD=$(get_pods_by_type client $NAMESPACE | head -1)
CTA_CLI_POD=$(get_pods_by_type cli $NAMESPACE | head -1)
CTA_MAINTD_POD=$(get_pods_by_type maintd $NAMESPACE | head -1)
EOS_MGM_POD="eos-mgm-0"
EOS_MGM_HOST="ctaeos"

echo
echo "Copying test scripts to ${CLIENT_POD}, ${EOS_MGM_POD} and ${CTA_TAPED_POD} pods."
kubectl -n ${NAMESPACE} cp . "${CLIENT_POD}:/root/" -c client
kubectl -n ${NAMESPACE} cp grep_xrdlog_mgm_for_error.sh "${EOS_MGM_POD}:/root/" -c eos-mgm
kubectl -n ${NAMESPACE} cp grep_eosreport_for_archive_metadata.sh "${EOS_MGM_POD}:/root/" -c eos-mgm
kubectl -n ${NAMESPACE} cp taped_refresh_log_fd.sh "${CTA_TAPED_POD}:/root/" -c cta-taped
kubectl -n ${NAMESPACE} cp maintd_refresh_log_fd.sh "${CTA_MAINTD_POD}:/root/" -c cta-maintd
kubectl -n ${NAMESPACE} cp maintd_refresh_config.sh "${CTA_MAINTD_POD}:/root/" -c cta-maintd

if [[ ${PREPARE} -eq 1 ]]; then
  echo "Preparing namespace for the tests"
    . prepare_tests.sh -n ${NAMESPACE}
  if [[ $? -ne 0 ]]; then
    echo "ERROR: failed to prepare namespace for the tests"
    exit 1
  fi
fi

NB_FILES=2000
FILE_SIZE_KB=15
NB_PROCS=20

echo
echo "Setting up environment for tests."
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "/root/client_setup.sh -n ${NB_FILES} -s ${FILE_SIZE_KB} -p ${NB_PROCS} -d /eos/ctaeos/preprod -r -c xrd" || exit 1

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

# We launch this in test_client for now as this is the one with opentelemetry enabled (meaning the config file has more options to check)
echo "Checking drive config correctness..."
python3 compare_taped_config_to_dr_ls.py --namespace ${NAMESPACE}

echo "Setting up client pod for HTTPs REST API test"
echo " Copying CA certificates to client pod from ${EOS_MGM_POD} pod."
kubectl -n ${NAMESPACE} cp "${EOS_MGM_POD}:etc/grid-security/certificates/" /tmp/certificates/ -c eos-mgm
kubectl -n ${NAMESPACE} cp /tmp/certificates ${CLIENT_POD}:/etc/grid-security/ -c client
rm -rf /tmp/certificates

echo "Launching brief Keycloak test"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/client_testKeycloak.sh || exit 1

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
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- bash /root/grep_eosreport_for_archive_metadata.sh ${TEST_METADATA} || exit 1

echo
echo "Launching immutable file test on client pod"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} && echo yes | cta-immutable-file-test root://${EOS_MGM_HOST}/\${EOS_DIR}/immutable_file ${TEST_POSTRUN} || die 'The cta-immutable-file-test failed.'" || exit 1

echo
echo "Launching client_simple_ar.sh on client pod"
echo " Archiving file: xrdcp as user1"
echo " Retrieving it as poweruser1"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} && /root/client_simple_ar.sh ${TEST_POSTRUN}" || exit 1
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

EOSDF_BUFFER_BASEDIR=/eos/ctaeos/eosdf
EOSDF_BUFFER_URL=${EOSDF_BUFFER_BASEDIR}
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos mkdir ${EOSDF_BUFFER_URL}
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos chmod 1777 ${EOSDF_BUFFER_URL}
# Test correct script execution
echo "Launching eosdf_systemtest.sh, expecting script to run properly"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} && /root/eosdf_systemtest.sh ${TEST_POSTRUN}" || exit 1
## Verify proper execution of script by grepping for the debug log line
# Set the TAPES and DRIVE_NAME based on the config in CTA_TAPED_POD
echo "Reading library configuration from ${CTA_TAPED_POD}"
DRIVE_NAME=$(kubectl exec -n ${NAMESPACE} ${CTA_TAPED_POD} -c cta-taped -- printenv DRIVE_NAME)
CTA_TAPED_LOG=/var/log/cta/cta-taped.log

if kubectl -n ${NAMESPACE} exec ${CTA_TAPED_POD} -c cta-taped -- bash -c "grep -q 'unable to get the free disk space with the script' ${CTA_TAPED_LOG}"; then
  echo "Script unexpectedly failed to get free disk space"
  exit 1
fi

## The idea is that we run it once without script, and once without executable permission on the script
## Both times we should get a success, because when the script is the problem, we allow staging to continue
echo "Launching eosdf_systemtest.sh with a nonexistent script"
# rename the script on taped so that it cannot be found
# error to grep for in the logs is 'No such file or directory'
kubectl -n ${NAMESPACE} exec ${CTA_TAPED_POD} -c cta-taped -- bash -c "mv /usr/bin/cta-eosdf.sh /usr/bin/eosdf_newname.sh" || exit 1
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} && /root/eosdf_systemtest.sh ${TEST_POSTRUN}" || exit 1
if kubectl -n ${NAMESPACE} exec ${CTA_TAPED_POD} -c cta-taped -- bash -c "grep -q 'No such file or directory' ${CTA_TAPED_LOG}"; then
  echo "Subprocess threw 'No such file or directory', as expected"
else
  exit 1
fi
# now give it back its original name but remove the executable permission, should still succeed
# now the error to grep for is 'Permission denied'
echo "Launching eosdf_systemtest.sh with correct script without executable permissions"
kubectl -n ${NAMESPACE} exec ${CTA_TAPED_POD} -c cta-taped -- bash -c "mv /usr/bin/eosdf_newname.sh /usr/bin/cta-eosdf.sh" || exit 1
kubectl -n ${NAMESPACE} exec ${CTA_TAPED_POD} -c cta-taped -- bash -c "chmod -x /usr/bin/cta-eosdf.sh" || exit 1
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} && /root/eosdf_systemtest.sh ${TEST_POSTRUN}" || exit 1
if kubectl -n ${NAMESPACE} exec ${CTA_TAPED_POD} -c cta-taped -- bash -c "grep -q 'Permission denied' ${CTA_TAPED_LOG}"; then
  echo "Subprocess threw 'Permission denied', as expected"
else
  exit 1
fi
# Test what happens when we get an error from the eos client (fake instance not reachable by specifying a nonexistent instance name in the script)
# grep for 'could not be used to get the FreeSpace'
echo "Launching eosdf_systemtest.sh with script that throws an eos-client error"
# fake instance not reachable
kubectl -n ${NAMESPACE} exec ${CTA_TAPED_POD} -c cta-taped -- bash -c "sed -i 's|root://\$diskInstance|root://nonexistentinstance|g' /usr/bin/cta-eosdf.sh" || exit 1
kubectl -n ${NAMESPACE} exec ${CTA_TAPED_POD} -c cta-taped -- bash -c "chmod +x /usr/bin/cta-eosdf.sh" || exit 1
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} && /root/eosdf_systemtest.sh ${TEST_POSTRUN}" || exit 1
kubectl -n ${NAMESPACE} exec ${CTA_TAPED_POD} -c cta-taped -- bash -c "sed -i 's|root://nonexistentinstance|root://\$diskInstance|g' /usr/bin/cta-eosdf.sh" || exit 1

echo
echo " Launching client_timestamp.sh on client pod"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} && /root/client_timestamp.sh ${TEST_POSTRUN}" || exit 1

echo
echo "Launching client_archive.sh on client pod"
echo " Archiving ${NB_FILES} files of ${FILE_SIZE_KB}kB each"
echo " Archiving files: xrdcp as user1"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} && /root/client_archive.sh ${TEST_POSTRUN}" || exit 1

kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo "###"
echo "Sleeping 10 seconds to allow MGM-FST communication to settle after disk copy deletion."
sleep 10
echo "###"

echo
echo "Launching client_retrieve.sh on client pod"
echo " Retrieving files: xrdfs as poweruser1"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} && /root/client_retrieve.sh ${TEST_POSTRUN}" || exit 1

kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo
echo "Launching client_evict.sh on client pod"
echo " Evicting files: xrdfs as poweruser1"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} && /root/client_evict.sh ${TEST_POSTRUN}" || exit 1

kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo
echo "Launching client_abortPrepare.sh on client pod"
echo "  Retrieving files: xrdfs as poweruser1"
echo "  Aborting prepare: xrdfs as poweruser1"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} && /root/client_abortPrepare.sh ${TEST_POSTRUN}" || exit 1

kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo
echo "Launching client_delete.sh on client pod"
echo " Deleting files:"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} && /root/client_delete.sh ${TEST_POSTRUN}" || exit 1
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo
echo "Launching client_multiple_retrieve.sh on client pod"
echo " Archiving file: xrdcp as user1"
echo " Retrieving it as poweruser1"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/client_multiple_retrieve.sh || exit 1
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo
echo "Launching idempotent_prepare.sh on client pod"
echo " Archiving file: xrdcp as user1"
echo " Retrieving it as poweruser1"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/idempotent_prepare.sh || exit 1
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo
echo "Launching delete_on_closew_error.sh on client pod"
echo " Archiving file: xrdcp as user1"
echo " Retrieving it as poweruser1"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/delete_on_closew_error.sh || exit 1
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo
echo "Launching archive_zero_length_file.sh on client pod"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/archive_zero_length_file.sh || exit 1
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo
echo "Launching evict_tests.sh on client pod"
echo " Archiving file: xrdcp as user1"
echo " Retrieving it as poweruser1"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/evict_tests.sh || exit 1
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

setup_tapes_for_multicopy_test

echo
echo "Launching client_retrieve_queue_cleanup.sh on client pod"
echo " Archiving file: xrdcp as user1"
echo " Retrieving it as poweruser1"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/client_retrieve_queue_cleanup.sh || exit 1
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo
echo "Launching taped_refresh_log_fd.sh on ${CTA_TAPED_POD} pod"
kubectl -n ${NAMESPACE} exec ${CTA_TAPED_POD} -c cta-taped -- bash /root/taped_refresh_log_fd.sh || exit 1

# Once we get the Python system tests, we should have a set of standardised tests for each process:
# - Correct log rotation
# - Correct config reloading
echo
echo "Launching maintd_refresh_log_fd.sh on ${CTA_MAINTD_POD} pod"
kubectl -n ${NAMESPACE} exec ${CTA_MAINTD_POD} -c cta-maintd -- bash /root/maintd_refresh_log_fd.sh || exit 1

echo
echo "Launching maintd_refresh_config.sh on ${CTA_MAINTD_POD} pod"
kubectl -n ${NAMESPACE} exec ${CTA_MAINTD_POD} -c cta-maintd -- bash /root/maintd_refresh_config.sh || exit 1

exit 0
