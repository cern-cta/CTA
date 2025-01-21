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

CLIENT_POD="client-0"
CTA_CLI_POD="cta-cli-0"
CTA_FRONTEND_POD="cta-frontend-0"
EOS_MGM_POD="eos-mgm-0"

kubectl -n ${NAMESPACE} cp client_helper.sh ${CLIENT_POD}:/root/client_helper.sh -c client

NB_FILES=100000
NB_DIRS=25
FILE_SIZE_KB=1
NB_PROCS=200
NB_DRIVES=10

# Need CTAADMIN_USER krb5
admin_kdestroy &>/dev/null
admin_kinit &>/dev/null

echo "Putting all tape drives up"
kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin dr up '.*'

#kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin mp ch --name ctasystest --minarchiverequestage 100 --minretrieverequestage 100 --maxdrivesallowed ${NB_DRIVES} --comment "ctasystest for stress tests"

kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin mp ch --name ctasystest --minarchiverequestage 100 --minretrieverequestage 100 --comment "ctasystest for stress tests"

kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin vo ch --vo vo --writemaxdrives ${NB_DRIVES} --readmaxdrives ${NB_DRIVES}

kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin mp ls

kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin dr ls

kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -- eos fs config 1 scaninterval=0

# install eos-debuginfo (600MB -> only for stress tests)
# NEEDED because eos does not leave the coredump after a crash
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -- dnf install -y eos-debuginfo

kubectl -n ${NAMESPACE} exec ${CTA_FRONTEND_POD} -c cta-frontend -- dnf install -y xrootd-debuginfo


ls | xargs -I{} kubectl -n ${NAMESPACE} cp {} ${CLIENT_POD}:/root/{} -c client
kubectl -n ${NAMESPACE} cp grep_xrdlog_mgm_for_error.sh ${EOS_MGM_POD}:/root/

echo
echo "Setting up environment for tests."
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "/root/client_setup.sh -n ${NB_FILES} -N ${NB_DIRS} -s ${FILE_SIZE_KB} -p ${NB_PROCS} -d /eos/ctaeos/preprod -v -r" || exit 1

TEST_PRERUN=". /root/client_env "
TEST_POSTRUN=""

VERBOSE=1
if [[ $VERBOSE == 1 ]]; then
  TEST_PRERUN="tail -v -f /mnt/logs/cta-tpsrv*/rmcd/cta/cta-rmcd.log & export TAILPID=\$! && ${TEST_PRERUN}"
  TEST_POSTRUN=" && kill \${TAILPID} &> /dev/null"
fi

echo
echo "Launching immutable file test on client pod"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} && echo yes | cta-immutable-file-test root://\${EOS_MGM_HOST}/\${EOS_DIR}/immutable_file ${TEST_POSTRUN} || die 'The cta-immutable-file-test failed.'" || exit 1

echo
echo "Launching client_ar.sh on client pod"
echo " Archiving ${NB_FILES} files of ${FILE_SIZE_KB}kB each"
echo " Archiving files: xrdcp as user1"
echo " Retrieving them as poweruser1"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} && /root/client_archive.sh ${TEST_POSTRUN}" || exit 1
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} && /root/client_retrieve.sh ${TEST_POSTRUN}" || exit 1

echo
echo "Launching client_evict.sh on client pod"
echo " Evicting files: xrdfs as poweruser1"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} && /root/client_evict.sh ${TEST_POSTRUN}" || exit 1

echo
echo "Launching client_abortPrepare.sh on client pod"
echo "  Retrieving files: xrdfs as poweruser1"
echo "  Aborting prepare: xrdfs as poweruser1"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} && /root/client_abortPrepare.sh ${TEST_POSTRUN}" || exit 1

echo
echo "Launching client_delete.sh on client pod"
echo " Deleting files:"
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "${TEST_PRERUN} && /root/client_delete.sh ${TEST_POSTRUN}" || exit 1

kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

exit 0
