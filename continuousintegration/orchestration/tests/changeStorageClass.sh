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

echo
echo "ENABLE CTAFRONTEND TO EXECUTE CTA ADMIN COMMANDS"
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin admin add --username ctafrontend --comment "for restore files test"
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin admin add --username ctaeos --comment "for restore files test"

echo
echo "ADD FRONTEND GATEWAY TO EOS"
echo "kubectl -n ${NAMESPACE} exec ctaeos -- bash eos root://${EOSINSTANCE} -r 0 0 vid add gateway ${FRONTEND_IP} grpc"
kubectl -n ${NAMESPACE} exec ctaeos -- eos -r 0 0 vid add gateway ${FRONTEND_IP} grpc

echo
echo "ADD STORAGE CLASS WITH ONE COPIES ${NEW_STORAGE_CLASS_NAME}"
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin sc add --name ${NEW_STORAGE_CLASS_NAME} --numberofcopies 2 --virtualorganisation vo --comment "comment"
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin sc ls

echo
echo "COPY REQUIRED FILES TO FRONTEND POD"
echo "sudo kubectl cp ${NAMESPACE}/ctacli:/etc/cta/cta-cli.conf /etc/cta/cta-cli.conf"
echo "sudo kubectl cp /etc/cta/cta-cli.conf ${NAMESPACE}/ctafrontend:/etc/cta/cta-cli.conf"
sudo kubectl cp ${NAMESPACE}/ctacli:/etc/cta/cta-cli.conf /etc/cta/cta-cli.conf
sudo kubectl cp /etc/cta/cta-cli.conf ${NAMESPACE}/ctafrontend:/etc/cta/cta-cli.conf

kubectl -n ${NAMESPACE} cp common/archive_file.sh client:/usr/bin/
kubectl -n ${NAMESPACE} cp client_helper.sh client:/root/
kubectl -n ${NAMESPACE} exec client -- bash /usr/bin/archive_file.sh -f ${FILE_1} || exit 1
kubectl -n ${NAMESPACE} exec client -- bash /usr/bin/archive_file.sh -f ${FILE_2} || exit 1

EOS_METADATA_PATH_1=$(mktemp -d).json
echo "SEND EOS METADATA TO JSON FILE: ${EOS_METADATA_PATH}"
touch ${EOS_METADATA_PATH_1}
kubectl -n ${NAMESPACE} exec client -- eos -j root://${EOSINSTANCE} file info /eos/ctaeos/cta/${FILE_1} | jq . | tee ${EOS_METADATA_PATH_1}
EOS_ARCHIVE_ID_1=$(jq -r '.xattr | .["sys.archive.file_id"]' ${EOS_METADATA_PATH_1})

EOS_METADATA_PATH_2=$(mktemp -d).json
echo "SEND EOS METADATA TO JSON FILE: ${EOS_METADATA_PATH_2}"
touch ${EOS_METADATA_PATH_2}
kubectl -n ${NAMESPACE} exec client -- eos -j root://${EOSINSTANCE} file info /eos/ctaeos/cta/${FILE_2} | jq . | tee ${EOS_METADATA_PATH_2}
EOS_ARCHIVE_ID_2=$(jq -r '.xattr | .["sys.archive.file_id"]' ${EOS_METADATA_PATH_2})

echo
echo "CHANGE FILES WITH IDS"
cd ~
IDS_FILEPATH=~/archiveFileIds.txt
rm ${IDS_FILEPATH}
touch ${IDS_FILEPATH}
echo ${EOS_ARCHIVE_ID_1} | tee -a ${IDS_FILEPATH}
echo ${EOS_ARCHIVE_ID_2} | tee -a ${IDS_FILEPATH}

echo
kubectl cp ~/CTA-build/cmdline/standalone_cli_tools/change_storage_class/cta-change-storage-class ${NAMESPACE}/ctafrontend:/usr/bin/
echo "kubectl cp ${IDS_FILEPATH} ${NAMESPACE}/ctafrontend:~/"
kubectl cp ${IDS_FILEPATH} ${NAMESPACE}/ctafrontend:/root/
echo "kubectl -n ${NAMESPACE} exec ctafrontend -- bash -c XrdSecPROTOCOL=sss XrdSecSSSKT=/etc/cta/eos.sss.keytab cta-change-storage-class --storage.class.name ${NEW_STORAGE_CLASS_NAME} --filename ${IDS_FILEPATH}"
kubectl -n ${NAMESPACE} exec ctafrontend -- bash -c "XrdSecPROTOCOL=sss XrdSecSSSKT=/etc/cta/eos.sss.keytab cta-change-storage-class --storage.class.name ${NEW_STORAGE_CLASS_NAME} --filename ${IDS_FILEPATH} -t 1"

EOS_METADATA_PATH_AFTER_CHANGE_1=$(mktemp -d).json
echo "SEND EOS METADATA TO JSON FILE: ${EOS_METADATA_PATH_AFTER_CHANGE_1}"
touch ${EOS_METADATA_PATH_AFTER_CHANGE_1}
kubectl -n ${NAMESPACE} exec client -- eos -j root://${EOSINSTANCE} file info /eos/ctaeos/cta/${FILE_1} | jq . | tee ${EOS_METADATA_PATH_AFTER_CHANGE_1}
EOS_STORAGE_CLASS_1=$(jq -r '.xattr | .["sys.archive.storage_class"]' ${EOS_METADATA_PATH_AFTER_CHANGE_1})
rm -r ${EOS_METADATA_PATH_AFTER_CHANGE_1}

EOS_METADATA_PATH_AFTER_CHANGE_2=$(mktemp -d).json
echo "SEND EOS METADATA TO JSON FILE: ${EOS_METADATA_PATH_AFTER_CHANGE_2}"
touch ${EOS_METADATA_PATH_AFTER_CHANGE_2}
kubectl -n ${NAMESPACE} exec client -- eos -j root://${EOSINSTANCE} file info /eos/ctaeos/cta/${FILE_2} | jq . | tee ${EOS_METADATA_PATH_AFTER_CHANGE_2}
EOS_STORAGE_CLASS_2=$(jq -r '.xattr | .["sys.archive.storage_class"]' ${EOS_METADATA_PATH_AFTER_CHANGE_2})
rm -r ${EOS_METADATA_PATH_AFTER_CHANGE_2}

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

if test ${EOS_STORAGE_CLASS_1} != ${NEW_STORAGE_CLASS_NAME}; then
  echo "ERROR: File ${FILE_1} did not change the storage class in EOS"
  exit 1
fi

if test ${CATALOGUE_STORAGE_CLASS_1} != ${NEW_STORAGE_CLASS_NAME}; then
  echo "ERROR: File ${FILE_1} did not change the storage class in the Catalogue"
  exit 1
fi

if test ${EOS_STORAGE_CLASS_2} != ${NEW_STORAGE_CLASS_NAME}; then
  echo "ERROR: File ${FILE_2} did not change the storage class in EOS"
  exit 1
fi

if test ${CATALOGUE_STORAGE_CLASS_2} != ${NEW_STORAGE_CLASS_NAME}; then
  echo "ERROR: File ${FILE_2} did not change the storage class in the Catalogue"
  exit 1
fi

echo
echo "All tests passed"

# Remove authorization
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin admin rm --username ctafrontend
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin admin rm --username ctaeos

