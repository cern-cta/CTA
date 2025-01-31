#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2024-2025 CERN
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


# simple stress test: archive some files and then retrieve these

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

CLIENT_POD="cta-client-0"
CTA_CLI_POD="cta-cli-0"
CTA_FRONTEND_POD="cta-frontend-0"
EOS_MGM_POD="eos-mgm-0"

NB_FILES=100
NB_DIRS=20
FILE_SIZE_KB=1
NB_PROCS=40
NB_DRIVES=2


kubectl -n ${NAMESPACE} cp client_helper.sh ${CLIENT_POD}:/root/client_helper.sh -c client

# Need CTAADMIN_USER krb5
admin_kdestroy &>/dev/null
admin_kinit &>/dev/null

echo "Putting all tape drives down - to queue the files first since the processing is faster than the queueing capabilities of EOS, we hold it half-way and only then put drives up in the client_stress_ar.sh script"
kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin dr down '.*' --reason "prequeue"

#echo "Putting all tape drives up"
#kubectl -n ${NAMESPACE} exec ctacli -- cta-admin dr up '.*'

#kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin mp ch --name ctasystest --minarchiverequestage 100 --minretrieverequestage 100 --maxdrivesallowed ${NB_DRIVES} --comment "ctasystest for stress tests"

kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin mp ch --name ctasystest --minarchiverequestage 100 --minretrieverequestage 100 --comment "ctasystest for stress tests"

kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin vo ch --vo vo --writemaxdrives ${NB_DRIVES} --readmaxdrives ${NB_DRIVES}

kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin mp ls

kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin dr ls

kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos fs config 1 scaninterval=0

# install eos-debuginfo (600MB -> only for stress tests)
# NEEDED because eos does not leave the coredump after a crash
# Commented out for now as the EOS images do not provide debuginfo
# kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- yum install -y eos-debuginfo

kubectl -n ${NAMESPACE} exec ${CTA_FRONTEND_POD} -c cta-frontend -- yum install -y xrootd-debuginfo

echo
echo "Launching client_stress_ar.sh on ${CLIENT_POD} pod"
echo " Archiving ${NB_FILES} files of ${FILE_SIZE_KB}kB each"
echo " Archiving files: xrdcp as user1"
echo " Retrieving them as poweruser1"
kubectl -n ${NAMESPACE} cp client_stress_ar.sh ${CLIENT_POD}:/root/client_stress_ar.sh -c client
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/client_stress_ar.sh -N ${NB_DIRS} -n ${NB_FILES} -s ${FILE_SIZE_KB} -p ${NB_PROCS} -e ctaeos -d /eos/ctaeos/preprod || exit 1
## Do not remove as listing af is not coming back???
#kubectl -n ${NAMESPACE} exec client -- bash /root/client_stress_ar.sh -A -N ${NB_DIRS} -n ${NB_FILES} -s ${FILE_SIZE_KB} -p ${NB_PROCS} -e ctaeos -d /eos/ctaeos/preprod -v || exit 1

#kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- bash /root/grep_xrdlog_mgm_for_error.sh || exit 1

exit 0
