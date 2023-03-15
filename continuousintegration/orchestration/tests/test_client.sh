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

echo
echo "Copying test scripts to client pod."
kubectl -n ${NAMESPACE} cp . client:/root/
kubectl -n ${NAMESPACE} cp grep_xrdlog_mgm_for_error.sh ctaeos:/root/


echo
echo "Launching immutable file test on client pod"
kubectl -n ${NAMESPACE} exec client -- bash -c "${TEST_PRERUN} && echo yes | cta-immutable-file-test root://\${EOSINSTANCE}/\${EOS_DIR}/immutable_file ${TEST_POSTRUN} || die 'The cta-immutable-file-test failed.'" || exit 1

echo
echo "Launching client_simple_ar.sh on client pod"
echo " Archiving file: xrdcp as user1"
echo " Retrieving it as poweruser1"
kubectl -n ${NAMESPACE} exec client -- bash /root/client_simple_ar.sh || exit 1
kubectl -n ${NAMESPACE} exec ctaeos -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

NB_FILES=10000
FILE_SIZE_KB=15

echo
echo "Launching client_ar.sh on client pod"
echo " Archiving ${NB_FILES} files of ${FILE_SIZE_KB}kB each"
echo " Archiving files: xrdcp as user1"
echo " Retrieving them as poweruser1"
kubectl -n ${NAMESPACE} exec client -- bash /root/client_ar.sh -n ${NB_FILES} -s ${FILE_SIZE_KB} -p 100 -d /eos/ctaeos/preprod -v -r || exit 1
kubectl -n ${NAMESPACE} exec ctaeos -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

#echo
#echo "Launching client_archive.sh on client pod"
#echo " Archiving file: xrdcp as user1"
#kubectl -n ${NAMESPACE} exec client -- bash /root/client_archive.sh || exit 1

#kubectl -n ${NAMESPACE} exec client -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

#echo
#echo "Launching client_retireve.sh on client pod"
#echo " Retrieving them as poweruser1"
#kubectl -n ${NAMESPACE} exec client -- bash /root/client_retrieve.sh || exit 1
#kubectl -n ${NAMESPACE} exec client -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1



# Only run gfal with xrd when xrd version == 5
#echo
#echo "Launching client_retrieve_gfal.sh on client pod"
#echo " Retrieving as , using xrootd"
#kubectl -n ${NAMESPACE} exec client -- bash /root/client_retrieve_gfal2.sh || exit 1

#kubectl -n ${NAMESPACE} exec client -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

#echo
#echo "Launching client_retrieve_gfal.sh on client pod"
#echo " Retrieving as , using HTPP"


#echo "Launching client_abortPrepare.sh on client pod"
#echo " Retrieving it as poweruser1"
#kubectl -n ${NAMESPACE} exec client -- bash /root/client_abortPrepare.sh || exit 1

#kubectl -n ${NAMESPACE} exec client -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1




echo
echo "Launching client_multiple_retrieve.sh on client pod"
echo " Archiving file: xrdcp as user1"
echo " Retrieving it as poweruser1"
kubectl -n ${NAMESPACE} exec client -- bash /root/client_multiple_retrieve.sh || exit 1

kubectl -n ${NAMESPACE} exec ctaeos -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo
echo "Launching idempotent_prepare.sh on client pod"
echo " Archiving file: xrdcp as user1"
echo " Retrieving it as poweruser1"
kubectl -n ${NAMESPACE} exec client -- bash /root/idempotent_prepare.sh || exit 1

kubectl -n ${NAMESPACE} exec ctaeos -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo
echo "Launching delete_on_closew_error.sh on client pod"
echo " Archiving file: xrdcp as user1"
echo " Retrieving it as poweruser1"
kubectl -n ${NAMESPACE} exec client -- bash /root/delete_on_closew_error.sh || exit 1

kubectl -n ${NAMESPACE} exec ctaeos -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

echo
echo "Launching try_evict_before_archive_completed.sh on client pod"
echo " Archiving file: xrdcp as user1"
echo " Retrieving it as poweruser1"
kubectl -n ${NAMESPACE} exec client -- bash /root/try_evict_before_archive_completed.sh || exit 1

kubectl -n ${NAMESPACE} exec ctaeos -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

setup_tapes_for_multicopy_test

echo
echo "Launching retrieve_queue_cleanup.sh on client pod"
echo " Archiving file: xrdcp as user1"
echo " Retrieving it as poweruser1"
kubectl -n ${NAMESPACE} exec client -- bash /root/retrieve_queue_cleanup.sh || exit 1

kubectl -n ${NAMESPACE} exec ctaeos -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

exit 0
