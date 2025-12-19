#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

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

if [[ -z "${NAMESPACE}" ]]; then
    usage
fi

if [[ -n "${error}" ]]; then
    echo -e "ERROR:\n${error}"
    exit 1
fi

CLIENT_POD="cta-client-0"
CTA_CLI_POD="cta-cli-0"
# This should be the service name; not the pod name
EOS_INSTANCE_NAME="ctaeos"
EOS_MGM_HOST="ctaeos"
NEW_STORAGE_CLASS_NAME=newStorageClassName

FILE_1=$(uuidgen)
FILE_2=$(uuidgen)
echo
echo "Creating files: ${FILE_1} ${FILE_2}"

############## CREAT TEMPORARY DIRECTORY ##############
TMP_DIR=$(mktemp -d)
kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- bash -c "mkdir -p ${TMP_DIR}"

############## ADD STORAGE CLASS WITH TWO COPIES ##############
kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin sc add --name ${NEW_STORAGE_CLASS_NAME} --numberofcopies 2 --virtualorganisation vo --comment "comment"
kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin sc ls

############## COPY REQUIRED FILES TO FRONTEND POD ##############
echo "kubectl cp ${NAMESPACE}/${CTA_CLI_POD}:/etc/cta/cta-cli.conf ${TMP_DIR}/cta-cli.conf"
echo "kubectl cp ${TMP_DIR}/cta-cli.conf ${NAMESPACE}/${CTA_CLI_POD}:/etc/cta/cta-cli.conf"
kubectl cp ${NAMESPACE}/${CTA_CLI_POD}:/etc/cta/cta-cli.conf ${TMP_DIR}/cta-cli.conf
kubectl cp ${TMP_DIR}/cta-cli.conf ${NAMESPACE}/${CTA_CLI_POD}:/etc/cta/cta-cli.conf

############## ARCHIVE FILES ##############
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "mkdir -p ${TMP_DIR}"
kubectl -n ${NAMESPACE} cp common/archive_file.sh ${CLIENT_POD}:${TMP_DIR}/
kubectl -n ${NAMESPACE} cp client_helper.sh ${CLIENT_POD}:/root/ -c client
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash ${TMP_DIR}/archive_file.sh -f ${FILE_1} || exit 1
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash ${TMP_DIR}/archive_file.sh -f ${FILE_2} || exit 1

############## GET EOS AND ARCHVIVE FILE IDS FROM ARCHIVED FILES ##############
EOS_METADATA_PATH_1=$(mktemp -d).json
echo "SEND EOS METADATA TO JSON FILE: ${EOS_METADATA_PATH}"
touch ${EOS_METADATA_PATH_1}
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- eos -j root://${EOS_MGM_HOST} file info /eos/ctaeos/cta/${FILE_1} | jq . | tee ${EOS_METADATA_PATH_1}
EOS_ARCHIVE_ID_1=$(jq -r '.xattr | .["sys.archive.file_id"]' ${EOS_METADATA_PATH_1})
EOS_FILE_ID_1=$(jq -r '.id' ${EOS_METADATA_PATH_1})

EOS_METADATA_PATH_2=$(mktemp -d).json
echo "SEND EOS METADATA TO JSON FILE: ${EOS_METADATA_PATH_2}"
touch ${EOS_METADATA_PATH_2}
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- eos -j root://${EOS_MGM_HOST} file info /eos/ctaeos/cta/${FILE_2} | jq . | tee ${EOS_METADATA_PATH_2}
EOS_ARCHIVE_ID_2=$(jq -r '.xattr | .["sys.archive.file_id"]' ${EOS_METADATA_PATH_2})
EOS_FILE_ID_2=$(jq -r '.id' ${EOS_METADATA_PATH_2})

############## CREATE INPUT FILE TO USE FOR TOOL ##############
IDS_FILEPATH=${TMP_DIR}/archiveFileIds.txt
touch ${IDS_FILEPATH}
echo '{"archiveFileId": '${EOS_ARCHIVE_ID_1}', "fid": '${EOS_FILE_ID_1}', "instance": "'${EOS_INSTANCE_NAME}'", "storageclass": "'${NEW_STORAGE_CLASS_NAME}'"}' >> ${IDS_FILEPATH}
echo '{"archiveFileId": '${EOS_ARCHIVE_ID_2}', "fid": '${EOS_FILE_ID_2}', "instance": "'${EOS_INSTANCE_NAME}'", "storageclass": "'${NEW_STORAGE_CLASS_NAME}'"}' >> ${IDS_FILEPATH}

############## RUN TOOL ##############
kubectl cp ${IDS_FILEPATH} ${NAMESPACE}/${CTA_CLI_POD}:${IDS_FILEPATH}
kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- bash -c "cta-change-storage-class-in-catalogue --storageclassname ${NEW_STORAGE_CLASS_NAME} --json ${IDS_FILEPATH} -t 1"

############## CHECK THAT VALUES WERE CHANGED IN THE CATALOGUE ##############
CATALOGUE_METADATA_PATH_AFTER_CHANGE_1=$(mktemp -d).json
echo "SEND CATALOGUE METADATA TO JSON FILE: ${CATALOGUE_METADATA_PATH_AFTER_CHANGE_1}"
touch ${CATALOGUE_METADATA_PATH_AFTER_CHANGE_1}
kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json tf ls --id ${EOS_ARCHIVE_ID_1} | jq . | tee ${CATALOGUE_METADATA_PATH_AFTER_CHANGE_1}
CATALOGUE_STORAGE_CLASS_1=$(jq . ${CATALOGUE_METADATA_PATH_AFTER_CHANGE_1} | jq '.[0]' | jq -r '.af | .["storageClass"]')
rm -r ${CATALOGUE_METADATA_PATH_AFTER_CHANGE_1}

CATALOGUE_METADATA_PATH_AFTER_CHANGE_2=$(mktemp -d).json
echo "SEND CATALOGUE METADATA TO JSON FILE: ${CATALOGUE_METADATA_PATH_AFTER_CHANGE_2}"
touch ${CATALOGUE_METADATA_PATH_AFTER_CHANGE_2}
kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json tf ls --id ${EOS_ARCHIVE_ID_2} | jq . | tee ${CATALOGUE_METADATA_PATH_AFTER_CHANGE_2}
CATALOGUE_STORAGE_CLASS_2=$(jq . ${CATALOGUE_METADATA_PATH_AFTER_CHANGE_2} | jq '.[0]' | jq -r '.af | .["storageClass"]')
rm -r ${CATALOGUE_METADATA_PATH_AFTER_CHANGE_2}

if test ${CATALOGUE_STORAGE_CLASS_1} != ${NEW_STORAGE_CLASS_NAME}; then
  echo "ERROR: File ${FILE_1} did not change the storage class in the Catalogue"
  exit 1
fi

if test ${CATALOGUE_STORAGE_CLASS_2} != ${NEW_STORAGE_CLASS_NAME}; then
  echo "ERROR: File ${FILE_2} did not change the storage class in the Catalogue"
  exit 1
fi

echo
echo "All tests passed"
