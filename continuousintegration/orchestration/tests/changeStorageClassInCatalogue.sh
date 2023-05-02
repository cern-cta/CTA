#bin/bash

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

EOSINSTANCE=ctaeos
NEW_STORAGE_CLASS_NAME=newStorageClassName

FILE_1=`uuidgen`
FILE_2=`uuidgen`
echo
echo "Creating files: ${FILE_1} ${FILE_2}"

FRONTEND_IP=$(kubectl -n ${NAMESPACE} get pods ctafrontend -o json | jq .status.podIP | tr -d '"')


############## CREAT TEMPORARY DIRECTORY ##############
TMP_DIR=$(mktemp -d)
kubectl -n ${NAMESPACE} exec ctafrontend -- bash -c "mkdir -p ${TMP_DIR}"

############## ENABLE CTAFRONTEND TO EXECUTE CTA ADMIN COMMANDS ##############
kubectl --namespace=${NAMESPACE} exec kdc -- cat /root/ctaadmin2.keytab | kubectl --namespace=${NAMESPACE} exec -i ctafrontend --  bash -c "cat > /root/ctaadmin2.keytab; mkdir -p /tmp/ctaadmin2"
kubectl -n ${NAMESPACE} cp client_helper.sh ctafrontend:/root/client_helper.sh
touch ${TMP_DIR}/init_kerb.sh
echo '. /root/client_helper.sh; admin_kinit' >> ${TMP_DIR}/init_kerb.sh
kubectl -n ${NAMESPACE} cp ${TMP_DIR}/init_kerb.sh ctafrontend:${TMP_DIR}/init_kerb.sh
kubectl -n ${NAMESPACE} exec ctafrontend -- bash ${TMP_DIR}/init_kerb.sh

############## ADD STORAGE CLASS WITH TWO COPIES ##############
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin sc add --name ${NEW_STORAGE_CLASS_NAME} --numberofcopies 2 --virtualorganisation vo --comment "comment"
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin sc ls

############## COPY REQUIRED FILES TO FRONTEND POD ##############
echo "kubectl cp ${NAMESPACE}/ctacli:/etc/cta/cta-cli.conf ${TMP_DIR}/cta-cli.conf"
echo "kubectl cp ${TMP_DIR}/cta-cli.conf ${NAMESPACE}/ctafrontend:/etc/cta/cta-cli.conf"
kubectl cp ${NAMESPACE}/ctacli:/etc/cta/cta-cli.conf ${TMP_DIR}/cta-cli.conf
kubectl cp ${TMP_DIR}/cta-cli.conf ${NAMESPACE}/ctafrontend:/etc/cta/cta-cli.conf

############## ARCHIVE FILES ##############
kubectl -n ${NAMESPACE} exec client -- bash -c "mkdir -p ${TMP_DIR}"
kubectl -n ${NAMESPACE} cp common/archive_file.sh client:${TMP_DIR}/
kubectl -n ${NAMESPACE} cp client_helper.sh client:/root/
kubectl -n ${NAMESPACE} exec client -- bash ${TMP_DIR}/archive_file.sh -f ${FILE_1} || exit 1
kubectl -n ${NAMESPACE} exec client -- bash ${TMP_DIR}/archive_file.sh -f ${FILE_2} || exit 1

############## GET EOS AND ARCHVIVE FILE IDS FROM ARCHIVED FILES ##############
EOS_METADATA_PATH_1=$(mktemp -d).json
echo "SEND EOS METADATA TO JSON FILE: ${EOS_METADATA_PATH}"
touch ${EOS_METADATA_PATH_1}
kubectl -n ${NAMESPACE} exec client -- eos -j root://${EOSINSTANCE} file info /eos/ctaeos/cta/${FILE_1} | jq . | tee ${EOS_METADATA_PATH_1}
EOS_ARCHIVE_ID_1=$(jq -r '.xattr | .["sys.archive.file_id"]' ${EOS_METADATA_PATH_1})
EOS_FILE_ID_1=$(jq -r '.id' ${EOS_METADATA_PATH_1})

EOS_METADATA_PATH_2=$(mktemp -d).json
echo "SEND EOS METADATA TO JSON FILE: ${EOS_METADATA_PATH_2}"
touch ${EOS_METADATA_PATH_2}
kubectl -n ${NAMESPACE} exec client -- eos -j root://${EOSINSTANCE} file info /eos/ctaeos/cta/${FILE_2} | jq . | tee ${EOS_METADATA_PATH_2}
EOS_ARCHIVE_ID_2=$(jq -r '.xattr | .["sys.archive.file_id"]' ${EOS_METADATA_PATH_2})
EOS_FILE_ID_2=$(jq -r '.id' ${EOS_METADATA_PATH_2})

############## CREATE INPUT FILE TO USE FOR TOOL ##############
IDS_FILEPATH=${TMP_DIR}/archiveFileIds.txt
touch ${IDS_FILEPATH}
echo '{"archiveFileId": '${EOS_ARCHIVE_ID_1}', "fid": '${EOS_FILE_ID_1}', "instance": "'${EOSINSTANCE}'", "storageclass": "'${NEW_STORAGE_CLASS_NAME}'"}' >> ${IDS_FILEPATH}
echo '{"archiveFileId": '${EOS_ARCHIVE_ID_2}', "fid": '${EOS_FILE_ID_2}', "instance": "'${EOSINSTANCE}'", "storageclass": "'${NEW_STORAGE_CLASS_NAME}'"}' >> ${IDS_FILEPATH}

############## RUN TOOL ##############
kubectl cp ${IDS_FILEPATH} ${NAMESPACE}/ctafrontend:${IDS_FILEPATH}
kubectl -n ${NAMESPACE} exec ctafrontend -- bash -c "XrdSecPROTOCOL=krb5 KRB5CCNAME=/tmp/ctaadmin2/krb5cc_0 cta-change-storage-class-in-catalogue --storageclassname ${NEW_STORAGE_CLASS_NAME} --json ${IDS_FILEPATH} -t 1"

############## CHECK THAT VALUES WERE CHANGED IN THE CATALOGUE ##############
CATALOGUE_METADATA_PATH_AFTER_CHANGE_1=$(mktemp -d).json
echo "SEND CATALOGUE METADATA TO JSON FILE: ${CATALOGUE_METADATA_PATH_AFTER_CHANGE_1}"
touch ${CATALOGUE_METADATA_PATH_AFTER_CHANGE_1}
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin --json tf ls --id ${EOS_ARCHIVE_ID_1} | jq . | tee ${CATALOGUE_METADATA_PATH_AFTER_CHANGE_1}
CATALOGUE_STORAGE_CLASS_1=$(jq . ${CATALOGUE_METADATA_PATH_AFTER_CHANGE_1} | jq '.[0]' | jq -r '.af | .["storageClass"]')
rm -r ${CATALOGUE_METADATA_PATH_AFTER_CHANGE_1}

CATALOGUE_METADATA_PATH_AFTER_CHANGE_2=$(mktemp -d).json
echo "SEND CATALOGUE METADATA TO JSON FILE: ${CATALOGUE_METADATA_PATH_AFTER_CHANGE_2}"
touch ${CATALOGUE_METADATA_PATH_AFTER_CHANGE_2}
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin --json tf ls --id ${EOS_ARCHIVE_ID_2} | jq . | tee ${CATALOGUE_METADATA_PATH_AFTER_CHANGE_2}
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
