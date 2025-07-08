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

CLIENT_POD="cta-client-0"
EOS_MGM_POD="eos-mgm-0"

echo "Installing gfal2 utility"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "dnf install -y python3-gfal2-util" || exit 1

echo
echo "Copying test scripts to client pod"
kubectl -n ${NAMESPACE} cp . ${CLIENT_POD}:/root/ -c client
kubectl -n ${NAMESPACE} cp grep_xrdlog_mgm_for_error.sh ${EOS_MGM_POD}:/root/ -c eos-mgm

kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c ". /root/client_helper.sh && admin_kinit"

NB_FILES=1000
FILE_SIZE_KB=15
NB_PROCS=20

TEST_PRERUN=". /root/client_env "
TEST_POSTRUN=""

clientgfal2_options="-n ${NB_FILES} -s ${FILE_SIZE_KB} -p ${NB_PROCS} -d /eos/ctaeos/preprod -r -c gfal2"

# Tests

GFAL2_PROTOCOL='root'
echo "Installing gfal2-plugin-xrootd for gfal-${GFAL2_PROTOCOL} tests."
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "dnf install -y gfal2-plugin-xrootd"

echo "Setting up environment for gfal-${GFAL2_PROTOCOL} test."
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "/root/client_setup.sh ${clientgfal2_options} -Z ${GFAL2_PROTOCOL}"

echo
echo "Track progress of test"
(kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c ". /root/client_env && /root/progress_tracker.sh 'archive retrieve evict delete'"
)&
TRACKER_PID=$!

echo
echo "Launching client_archive.sh on client pod using ${GFAL2_PROTOCOL} protocol"
echo "  Archiving files: xrdcp as user1"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} &&  /root/client_archive.sh ${TEST_POSTRUN}" || exit 1
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo "###"
echo "Sleeping 10 seconds to allow MGM-FST communication to settle after disk copy deletion."
sleep 10
echo "###"

echo
echo "Launching client_retrieve.sh on client pod using ${GFAL2_PROTOCOL} protocol"
echo "  Retrieving files with gfal-bringonline via root protocol"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} &&  /root/client_retrieve.sh ${TEST_POSTRUN}" || exit 1
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo
echo "Launching client_evict.sh on client pod using ${GFAL2_PROTOCOL} protocol"
echo "  Evicting files with gfal-evict via root protocol"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} &&  /root/client_evict.sh ${TEST_POSTRUN}" || exit 1
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo
echo "Launching client_delete.sh on client pod using ${GFAL2_PROTOCOL} protocol"
echo "  Deleting files with gfal-rm via root protocol"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} &&  /root/client_delete.sh ${TEST_POSTRUN}" || exit 1
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1


echo "$(date +%s): Waiting for tracker process to finish. "
wait "${TRACKER_PID}"
if [[ $? == 1 ]]; then
  echo "Some files were lost during tape workflow."
  kubectl -n ${NAMESPACE} cp ${CLIENT_POD}:/root/trackerdb.db -c client ../../../pod_logs/${NAMESPACE}/trackerdb.db 2>/dev/null
exit 1
fi

# Test gfal2 https plugin

echo "Uninstall gfal2-plugin-xrootd before continuing with http tests"
# The presence of the xrootd package seems to be causing double free/corruption errors in the http plugin
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "dnf -y remove gfal2-plugin-xrootd"
echo "Installing gfal2-plugin-http for http gfal test."
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "dnf install -y gfal2-plugin-http" || exit 1
echo "Enable insecure certs for gfal2"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "sed -i 's/INSECURE=false/INSECURE=true/g' /etc/gfal2.d/http_plugin.conf" || exit 1
echo "Setting up environment for gfal-https tests"
GFAL2_PROTOCOL='https'
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "/root/client_setup.sh ${clientgfal2_options} -Z ${GFAL2_PROTOCOL}"

echo
echo "Track progress of test"
(kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c ". /root/client_env && /root/progress_tracker.sh 'archive retrieve evict delete'"
)&
TRACKER_PID=$!

echo
echo "Launching client_archive.sh on client pod using ${GFAL2_PROTOCOL} protocol"
echo " Archiving files: gfal-copy as user1 via https"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} &&  /root/client_archive.sh ${TEST_POSTRUN}" || exit 1
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo "###"
echo "Sleeping 10 seconds to allow MGM-FST communication to settle after disk copy deletion."
sleep 10
echo "###"

echo
echo "Launching client_retrieve.sh on client pod using ${GFAL2_PROTOCOL} protocol"
echo "  Retrieving files with gfal-bringonline via https protocol"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} &&  /root/client_retrieve.sh ${TEST_POSTRUN}" || exit 1
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo
echo "Launching client_evict.sh on client pod using ${GFAL2_PROTOCOL} protocol"
echo "  Evicting files with gfal-evict as poweruser via https protocol"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} &&  /root/client_evict.sh ${TEST_POSTRUN}" || exit 1
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo
echo "Launching client_delete.sh on client pod using ${GFAL2_PROTOCOL} protocol"
echo "  Deleting files with gfal-rm as user1 via https protocol"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} &&  /root/client_delete.sh ${TEST_POSTRUN}" || exit 1
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo "$(date +%s): Waiting for tracker process to finish. "
wait "${TRACKER_PID}"
if [[ $? == 1 ]]; then
    echo "Some files were lost during tape workflow."
    exit 1
fi

echo "Sleeping 10 seconds to give Prometheus time to scrape the metrics"
sleep 10

# Test that telemetry agrees with the amount of files archived and retrieved
TOTAL_FILES_ARCHIVED_RETRIEVED=$((NB_FILES * 2))
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "/root/client_telemetry_summary.sh $TOTAL_FILES_ARCHIVED_RETRIEVED" || exit 1

# Test activity
TEST_PRERUN=". /root/client_env "

echo
echo "Launching gfal_activity_check.sh on client pod"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} && /root/gfal_activity_check.sh"  || exit 1

echo
echo "Launching xrootd_activity_check.sh on client pod"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} && /root/xrootd_activity_check.sh" || exit 1

echo "Checking activity was set..."
kubectl -n ${NAMESPACE} cp grep_eosreport_for_activity.sh ${EOS_MGM_POD}:/root/ -c eos-mgm
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- bash /root/grep_eosreport_for_activity.sh || exit 1

exit 0
