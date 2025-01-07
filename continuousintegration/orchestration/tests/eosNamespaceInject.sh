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

###################################################
################# HOW TO RUN TEST #################
###################################################
## To run the test in CI:                        ##
## 1. Create the pods                            ##
## 2. Run ./prepare_tests.sh -n <namespace>      ##
## 3. Run ./eosNamespaceInject.sh -n <namespace> ##
###################################################
###################################################
###################################################

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

CLIENT_POD="client"
CTA_CLI_POD="cta-cli"
CTA_FRONTEND_POD="cta-frontend"
EOS_MGM_POD="ctaeos"
# This should be the service name; not the pod name
EOS_INSTANCE="ctaeos"
TMP_DIR=$(mktemp -d)
FILE_1=$(uuidgen)
FILE_2=$(uuidgen)
echo
echo "Creating files: ${FILE_1} ${FILE_2}"

kubectl -n ${NAMESPACE} cp common/archive_file.sh ${CLIENT_POD}:/usr/bin/ -c client
kubectl -n ${NAMESPACE} cp client_helper.sh ${CLIENT_POD}:/root/ -c client
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /usr/bin/archive_file.sh -f ${FILE_1} || exit 1
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /usr/bin/archive_file.sh -f ${FILE_2} || exit 1

EOS_METADATA_PATH_1=$(mktemp -d).json
echo "SEND EOS METADATA TO JSON FILE: ${EOS_METADATA_PATH_1}"
touch ${EOS_METADATA_PATH_1}
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- eos -j root://${EOS_INSTANCE} file info /eos/ctaeos/cta/${FILE_1} | jq . | tee ${EOS_METADATA_PATH_1}
EOS_ARCHIVE_ID_1=$(jq -r '.xattr | .["sys.archive.file_id"]' ${EOS_METADATA_PATH_1})
EOS_CHECKSUM_1=$(jq -r '.checksumvalue' ${EOS_METADATA_PATH_1})
EOS_SIZE_1=$(jq -r '.size' ${EOS_METADATA_PATH_1})

EOS_METADATA_PATH_2=$(mktemp -d).json
echo "SEND EOS METADATA TO JSON FILE: ${EOS_METADATA_PATH_2}"
touch ${EOS_METADATA_PATH_2}
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- eos -j root://${EOS_INSTANCE} file info /eos/ctaeos/cta/${FILE_2} | jq . | tee ${EOS_METADATA_PATH_2}
EOS_ARCHIVE_ID_2=$(jq -r '.xattr | .["sys.archive.file_id"]' ${EOS_METADATA_PATH_2})
EOS_CHECKSUM_2=$(jq -r '.checksumvalue' ${EOS_METADATA_PATH_2})
EOS_SIZE_2=$(jq -r '.size' ${EOS_METADATA_PATH_2})

echo "Create json meta data input file"
touch ${TMP_DIR}/metaData
FILE_PATH_1=$(uuidgen)
FILE_PATH_2=$(uuidgen)
echo '{"eosPath": "/eos/ctaeos/'${FILE_PATH_1}'", "diskInstance": "ctaeos", "archiveId": '${EOS_ARCHIVE_ID_1}', "size": "'${EOS_SIZE_1}'", "checksumType": "ADLER32", "checksumValue": "'${EOS_CHECKSUM_1}'"}' >> ${TMP_DIR}/metaData
echo '{"eosPath": "/eos/ctaeos/'${FILE_PATH_2}'", "diskInstance": "ctaeos", "archiveId": '${EOS_ARCHIVE_ID_2}', "size": "'${EOS_SIZE_2}'", "checksumType": "ADLER32", "checksumValue": "'${EOS_CHECKSUM_2}'"}' >> ${TMP_DIR}/metaData
kubectl -n ${NAMESPACE} exec ${CTA_FRONTEND_POD} -c cta-frontend -- bash -c "mkdir -p ${TMP_DIR}"
kubectl cp ${TMP_DIR}/metaData ${NAMESPACE}/cta-frontend:${TMP_DIR}/

echo
echo "ENABLE cta-frontend TO EXECUTE CTA ADMIN COMMANDS"
kubectl --namespace ${NAMESPACE} exec auth-kdc -- cat /root/ctaadmin2.keytab | kubectl --namespace ${NAMESPACE} exec -i cta-frontend --  bash -c "cat > /root/ctaadmin2.keytab; mkdir -p /tmp/ctaadmin2"
kubectl -n ${NAMESPACE} cp client_helper.sh cta-frontend:${TMP_DIR}/client_helper.sh
touch ${TMP_DIR}/init_kerb.sh
echo '. '${TMP_DIR}'/client_helper.sh; admin_kinit' >> ${TMP_DIR}/init_kerb.sh
kubectl -n ${NAMESPACE} cp ${TMP_DIR}/init_kerb.sh cta-frontend:${TMP_DIR}/init_kerb.sh
kubectl -n ${NAMESPACE} exec ${CTA_FRONTEND_POD} -c cta-frontend -- bash ${TMP_DIR}/init_kerb.sh

echo
echo "ADD FRONTEND GATEWAY TO EOS"
FRONTEND_IP=$(kubectl -n ${NAMESPACE} get pods cta-frontend -o json | jq .status.podIP | tr -d '"')
echo "kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -- eos root://${EOS_INSTANCE} -r 0 0 vid add gateway ${FRONTEND_IP} grpc"
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -- eos -r 0 0 vid add gateway ${FRONTEND_IP} grpc

echo
echo "COPY REQUIRED FILES TO FRONTEND POD"
echo "kubectl cp ${NAMESPACE}/${CTA_CLI_POD}:/etc/cta/cta-cli.conf ${TMP_DIR}/cta-cli.conf"
echo "kubectl cp ${TMP_DIR}/cta-cli.conf ${NAMESPACE}/cta-frontend:/etc/cta/cta-cli.conf"
kubectl cp ${NAMESPACE}/${CTA_CLI_POD}:/etc/cta/cta-cli.conf ${TMP_DIR}/cta-cli.conf
kubectl cp ${TMP_DIR}/cta-cli.conf ${NAMESPACE}/cta-frontend:/etc/cta/cta-cli.conf

echo
echo "kubectl -n ${NAMESPACE} exec ${CTA_FRONTEND_POD} -c cta-frontend -- bash -c cta-eos-namespace-inject --json ${TMP_DIR}/metaData"
kubectl -n ${NAMESPACE} exec ${CTA_FRONTEND_POD} -c cta-frontend -- bash -c "XrdSecPROTOCOL=krb5 KRB5CCNAME=/tmp/ctaadmin2/krb5cc_0 cta-eos-namespace-inject --json ${TMP_DIR}/metaData"
