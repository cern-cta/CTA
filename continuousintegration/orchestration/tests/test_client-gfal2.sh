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

echo "Installing gfal2 utility"
kubectl -n ${NAMESPACE} exec client -- bash -c "yum -y install gfal2-util" || exit 1

echo
echo "Copying test scripts to client pod"
kubectl -n ${NAMESPACE} cp . client:/root/
kubectl -n ${NAMESPACE} cp grep_xrdlog_mgm_for_error.sh ctaeos:/root/

kubectl -n ${NAMESPACE} exec client -- bash -c ". /root/client_helper.sh && admin_kinit"

NB_FILES=5000
FILE_SIZE_KB=15
NB_PROCS=100

TEST_PRERUN=". /root/client_env "
TEST_POSTRUN=""

VERBOSE=1
if [[ $VERBOSE == 1 ]]; then
  TEST_PRERUN="tail -v -f /mnt/logs/tpsrv0*/rmcd/cta/cta-rmcd.log & export TAILPID=\$! && ${TEST_PRERUN}"
  TEST_POSTRUN=" && kill \${TAILPID} &> /dev/null"
fi

clientgfal2_options="-n ${NB_FILES} -s ${FILE_SIZE_KB} -p ${NB_PROCS} -d /eos/ctaeos/preprod -v -r -c gfal2"

# Tests
# Check for xrd vesion as xrd gfal plugin only runs under xrd version 5.
if [[ ${XROOTD_VERSION} == 5 ]]; then
    GFAL2_PROTOCOL='root'
    echo "Installing gfal2-plugin-xrootd for gfal-${GFAL2_PROTOCOL} tests."
    kubectl -n ${NAMESPACE} exec client -- bash -c "yum -y install gfal2-plugin-xrootd"

    echo "Setting up environment for gfal-${GFAL2_PROTOCOL} test."
    kubectl -n ${NAMESPACE} exec client -- bash -c "/root/client_setup.sh ${clientgfal2_options} -Z ${GFAL2_PROTOCOL}"

    echo
    echo "Track progress of test"
    (kubectl -n ${NAMESPACE} exec client -- bash -c ". /root/client_env && /root/progress_tracker.sh 'archive retrieve evict delete'"
    )&
    TRACKER_PID=$!

    echo
    echo "Launching client_archive.sh on client pod using ${GFAL2_PROTOCOL} protocol"
    echo "  Archiving files: xrdcp as user1"
    kubectl -n ${NAMESPACE} exec client -- bash -c "${TEST_PRERUN} &&  /root/client_archive.sh ${TEST_POSTRUN}" || exit 1
   kubectl -n ${NAMESPACE} exec ctaeos -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

    echo
    echo "Launching client_retrieve.sh on client pod using ${GFAL2_PROTOCOL} protocol"
    echo "  Retrieving files with gfal-bringonline via root protocol"
    kubectl -n ${NAMESPACE} exec client -- bash -c "${TEST_PRERUN} &&  /root/client_retrieve.sh ${TEST_POSTRUN}" || exit 1
    kubectl -n ${NAMESPACE} exec ctaeos -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

    echo
    echo "Launching client_evict.sh on client pod using ${GFAL2_PROTOCOL} protocol"
    echo "  Evicting files with gfal-evict via root protocol"
    kubectl -n ${NAMESPACE} exec client -- bash -c "${TEST_PRERUN} &&  /root/client_evict.sh ${TEST_POSTRUN}" || exit 1
    kubectl -n ${NAMESPACE} exec ctaeos -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

    echo
    echo "Launching client_delete.sh on client pod using ${GFAL2_PROTOCOL} protocol"
    echo "  Deleting files with gfal-rm via root protocol"
    kubectl -n ${NAMESPACE} exec client -- bash -c "${TEST_PRERUN} &&  /root/client_delete.sh ${TEST_POSTRUN}" || exit 1
    kubectl -n ${NAMESPACE} exec ctaeos -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1


    echo "$(date +%s): Waiting for tracker process to finish. "
    wait "${TRACKER_PID}"
    if [[ $? == 1 ]]; then
      echo "Some files were lost during tape workflow."
      kubectl -n ${NAMESPACE} cp client:/root/trackerdb.db ../../../pod_logs/${NAMESPACE}/trackerdb.db 2>/dev/null
    exit 1
  fi
fi

# Test gfal2 https plugin
echo "Installing gfal2-plugin-http for http gfal test."
kubectl -n ${NAMESPACE} exec client -- bash -c "yum -y install gfal2-plugin-http" || exit 1
echo "Enable insecure certs for gfal2"
kubectl -n ${NAMESPACE} exec client -- bash -c "sed -i 's/INSECURE=false/INSECURE=true/g' /etc/gfal2.d/http_plugin.conf" || exit 1
echo "Setting up environment for gfal-https tests"
GFAL2_PROTOCOL='https'
kubectl -n ${NAMESPACE} exec client -- bash -c "/root/client_setup.sh ${clientgfal2_options} -Z ${GFAL2_PROTOCOL}"

echo
echo "Track progress of test"
(kubectl -n ${NAMESPACE} exec client -- bash -c ". /root/client_env && /root/progress_tracker.sh 'archive retrieve evict delete'"
)&
TRACKER_PID=$!

echo
echo "Launching client_archive.sh on client pod using ${TEST_PROTOCOL} protocol"
echo " Archiving files: gfal-copy as user1 via https"
kubectl -n ${NAMESPACE} exec client -- bash -c "${TEST_PRERUN} &&  /root/client_archive.sh ${TEST_POSTRUN}" || exit 1
kubectl -n ${NAMESPACE} exec ctaeos -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

 echo
echo "Launching client_retrieve.sh on client pod using ${GFAL2_PROTOCOL} protocol"
echo "  Retrieving files with gfal-bringonline via https protocol"
    kubectl -n ${NAMESPACE} exec client -- bash -c "${TEST_PRERUN} &&  /root/client_retrieve.sh ${TEST_POSTRUN}" || exit 1
    kubectl -n ${NAMESPACE} exec ctaeos -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

    echo
    echo "Launching client_evict.sh on client pod using ${GFAL2_PROTOCOL} protocol"
    echo "  Evicting files with gfal-evict as poweruser via https protocol"
    kubectl -n ${NAMESPACE} exec client -- bash -c "${TEST_PRERUN} &&  /root/client_evict.sh ${TEST_POSTRUN}" || exit 1
    kubectl -n ${NAMESPACE} exec ctaeos -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

    echo
    echo "Launching client_delete.sh on client pod using ${GFAL2_PROTOCOL} protocol"
    echo "  Deleting files with gfal-rm as user1 via https protocol"
    kubectl -n ${NAMESPACE} exec client -- bash -c "${TEST_PRERUN} &&  /root/client_delete.sh ${TEST_POSTRUN}" || exit 1
    kubectl -n ${NAMESPACE} exec ctaeos -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

    echo "$(date +%s): Waiting for tracker process to finish. "
    wait "${TRACKER_PID}"
    if [[ $? == 1 ]]; then
      echo "Some files were lost during tape workflow."
      exit 1
    fi

exit 0
