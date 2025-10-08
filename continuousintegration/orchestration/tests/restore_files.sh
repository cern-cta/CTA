#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


EOS_INSTANCE_NAME="ctaeos"
EOS_MGM_HOST="ctaeos"
LOGFILE_PATH=$(mktemp -d)/restore_files.log
TEST_FILE_NAME=$(uuidgen)
WAIT_FOR_RETRIEVED_FILE_TIMEOUT=10

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

CLIENT_POD="cta-client-0"
CTA_CLI_POD="cta-cli-0"
CTA_FRONTEND_POD="cta-frontend-0"
EOS_MGM_POD="eos-mgm-0"

FRONTEND_IP=$(kubectl -n ${NAMESPACE} get pods ${CTA_FRONTEND_POD} -o json | jq .status.podIP | tr -d '"')

echo
echo "ADD FRONTEND GATEWAY TO EOS"
echo "kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- bash eos root://${EOS_MGM_HOST} -r 0 0 vid add gateway ${FRONTEND_IP} grpc"
# Generate random key
grpc_key=$(openssl rand -hex 12)

kubectl -n ${NAMESPACE} exec ${CTA_FRONTEND_POD} -c cta-frontend -- sh -c "echo 'ctaeos ctaeos:50051 ${grpc_key}' > /etc/cta/eos.grpc.keytab"
# This will fail currently as /etc/cta/cta-frontend-xrootd.conf is a configmap and therefore readonly
# If this needs to be tested, the dev should manually update the configmap to include this
# kubectl -n ${NAMESPACE} exec ${CTA_FRONTEND_POD} -c cta-frontend -- sh -c "echo 'cta.ns.config /etc/cta/eos.grpc.keytab' >> /etc/cta/cta-frontend-xrootd.conf"
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos vid set map -grpc key:${grpc_key} vuid:2 vgid:2
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos -r 0 0 vid add gateway ${FRONTEND_IP} grpc

echo
echo "eos vid ls"
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos vid ls

echo
echo "Launching restore_files_client.sh on client pod"
echo " Archiving file: xrdcp as user1"
kubectl -n ${NAMESPACE} cp common/archive_file.sh ${CLIENT_POD}:/root/archive_file.sh -c client
kubectl -n ${NAMESPACE} cp client_helper.sh ${CLIENT_POD}:/root/client_helper.sh -c client
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/archive_file.sh -f ${TEST_FILE_NAME} || exit 1

echo
METADATA_FILE_PATH=$(mktemp -d).json
echo "SEND FILE METADATA TO JSON FILE: ${METADATA_FILE_PATH}"
touch ${METADATA_FILE_PATH}
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- eos -j root://${EOS_MGM_HOST} file info /eos/ctaeos/cta/${TEST_FILE_NAME} | jq . > ${METADATA_FILE_PATH}

# Extract values from the meta data used for restoring and testing
FXID=$(jq -r '.fxid' ${METADATA_FILE_PATH})
FILE_SIZE=$(jq -r '.size' ${METADATA_FILE_PATH})
ARCHIVE_FILE_ID=$(jq -r '.xattr | .["sys.archive.file_id"]' ${METADATA_FILE_PATH})
CHECKSUM=$(jq -r '.checksumvalue' ${METADATA_FILE_PATH})

echo
echo "DELETE ARCHIVED FILE"
kubectl -n ${NAMESPACE} cp common/delete_file.sh ${CLIENT_POD}:/root/delete_file.sh -c client
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/delete_file.sh -i ${EOS_MGM_HOST} -f ${TEST_FILE_NAME}

echo
echo "VALIDATE THAT THE FILE IS IN THE RECYCLE BIN"
echo "kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin rtf ls --fxid ${FXID} || exit 1"
kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin rtf ls --fxid ${FXID} || exit 1

echo
echo "COPY REQUIRED FILES TO FRONTEND POD"
TMP_DIR=$(mktemp -d)
kubectl --namespace ${NAMESPACE} exec ${CTA_FRONTEND_POD} -c cta-frontend -- bash -c "mkdir -p ${TMP_DIR}"
echo "kubectl cp ${NAMESPACE}/${CTA_CLI_POD}:/etc/cta/cta-cli.conf ${TMP_DIR}/cta-cli.conf"
echo "kubectl cp ${TMP_DIR}/cta/cta-cli.conf ${NAMESPACE}/cta-frontend:/etc/cta-cli.conf"

kubectl cp ${NAMESPACE}/${CTA_CLI_POD}:/etc/cta/cta-cli.conf ${TMP_DIR}/cta-cli.conf
kubectl cp ${TMP_DIR}/cta-cli.conf ${NAMESPACE}/${CTA_FRONTEND_POD}:/etc/cta/cta-cli.conf

##
# Maybe that this part should entirely be moved to cta-cli pod:
# there is no reason to install cta-cli rpm on the frontend pod as it is just meant to run the cta-frontend with minimal requirements
echo
echo "ENABLE ${CTA_FRONTEND_POD} TO EXECUTE CTA ADMIN COMMANDS"
kubectl --namespace ${NAMESPACE} exec ${CLIENT_POD} -- cat /root/ctaadmin2.keytab | kubectl --namespace ${NAMESPACE} exec -i ${CTA_FRONTEND_POD} --  bash -c "cat > /root/ctaadmin2.keytab; mkdir -p /tmp/ctaadmin2"
kubectl -n ${NAMESPACE} cp client_helper.sh ${CTA_FRONTEND_POD}:/root/client_helper.sh
touch ${TMP_DIR}/init_kerb.sh
echo '. /root/client_helper.sh; admin_kinit' >> ${TMP_DIR}/init_kerb.sh
kubectl -n ${NAMESPACE} cp ${TMP_DIR}/init_kerb.sh ${CTA_FRONTEND_POD}:${TMP_DIR}/init_kerb.sh
kubectl -n ${NAMESPACE} exec ${CTA_FRONTEND_POD} -c cta-frontend -- bash ${TMP_DIR}/init_kerb.sh
# install cta-cli that provides `cta-restore-deleted-files`
kubectl -n ${NAMESPACE} exec ${CTA_FRONTEND_POD} -c cta-frontend -- bash -c 'rpm -q cta-cli || dnf install -y cta-cli'

echo
echo "RESTORE FILES"
kubectl -n ${NAMESPACE} exec ${CTA_FRONTEND_POD} -c cta-frontend -- bash -c "XrdSecPROTOCOL=krb5 KRB5CCNAME=/tmp/ctaadmin2/krb5cc_0 cta-restore-deleted-files --id ${ARCHIVE_FILE_ID} --copynb 1 --debug"
# End of *movable* section
##

SECONDS_PASSED=0
WAIT_FOR_RETRIEVED_FILE_TIMEOUT=10
while kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- test $(false = xrdfs root://${EOS_MGM_HOST} query prepare 0 /eos/ctaeos/cta/${TEST_FILE_NAME} | jq . | jq '.responses[0] | .path_exists'); do
  echo "Waiting for file with name:${TEST_FILE_NAME} to be restored on EOS side: Seconds passed = ${SECONDS_PASSED}"
  sleep 1
  let SECONDS_PASSED=SECONDS_PASSED+1

  if test ${SECONDS_PASSED} == ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT}; then
    echo "Timed out after ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT} seconds waiting for file to be restored at EOS side"
    exit 1
  fi
done

echo
METADATA_FILE_AFTER_RESTORE_PATH=$(mktemp -d).json
echo "SEND FILE METADATA TO JSON FILE: ${METADATA_FILE_AFTER_RESTORE_PATH}"
touch ${METADATA_FILE_AFTER_RESTORE_PATH}
kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json tf ls --id ${ARCHIVE_FILE_ID} --instance ${EOS_INSTANCE_NAME} | jq '.[0]' |& tee ${METADATA_FILE_AFTER_RESTORE_PATH}
kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json tf ls --id ${ARCHIVE_FILE_ID} --instance ${EOS_INSTANCE_NAME} | jq '.[0]' | tee -a ${LOGFILE_PATH}

# Extract values from the meta data from the restored file
FILE_SIZE_AFTER_RESTORE=$(jq -r '.af | .["size"]' ${METADATA_FILE_AFTER_RESTORE_PATH})
ARCHIVE_FILE_ID_AFTER_RESTORE=$(jq -r '.af | .["archiveId"]' ${METADATA_FILE_AFTER_RESTORE_PATH})
CHECKSUM_AFTER_RESTORE=$(jq -r '.af | .checksum[0] | .["value"]' ${METADATA_FILE_AFTER_RESTORE_PATH})
FXID_AFTER_RESTORE=$(jq -r '.df | .["diskId"]' ${METADATA_FILE_AFTER_RESTORE_PATH})

EOS_METADATA_AFTER_RESTORE_PATH=$(mktemp -d).json
echo "SEND EOS METADATA TO JSON FILE: ${EOS_METADATA_AFTER_RESTORE_PATH}"
touch ${EOS_METADATA_AFTER_RESTORE_PATH}
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- eos -j root://${EOS_MGM_HOST} file info /eos/ctaeos/cta/${TEST_FILE_NAME} | jq . |& tee ${EOS_METADATA_AFTER_RESTORE_PATH}
kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- eos -j root://${EOS_MGM_HOST} file info /eos/ctaeos/cta/${TEST_FILE_NAME} | jq . | tee -a ${LOGFILE_PATH}

EOS_NS_FXID_AFTER_RESTORE=$(jq -r '.fxid' ${EOS_METADATA_AFTER_RESTORE_PATH})
EOS_NS_FXID_AFTER_RESTORE_DEC=$(( 16#$EOS_NS_FXID_AFTER_RESTORE ))

if test ${CHECKSUM} != ${CHECKSUM_AFTER_RESTORE}; then
  echo "ERROR: Checksum for original file and restored file does match"
  exit 1
fi

if test ${ARCHIVE_FILE_ID} != ${ARCHIVE_FILE_ID_AFTER_RESTORE}; then
  echo "ERROR: archive file id for original file and restored file does match"
  exit 1
fi

if test ${FILE_SIZE} != ${FILE_SIZE_AFTER_RESTORE}; then
  echo "ERROR: file size for original file and restored file does match"
  exit 1
fi

echo "CTA fxid: ${FXID_AFTER_RESTORE}"
echo "EOS fxid: ${EOS_NS_FXID_AFTER_RESTORE_DEC}"
if test ${FXID_AFTER_RESTORE} != ${EOS_NS_FXID_AFTER_RESTORE_DEC}; then
  echo "ERROR: fxid does not match in EOS and CTA"
  echo "${FXID_AFTER_RESTORE} does not match ${EOS_NS_FXID_AFTER_RESTORE_DEC}"
  exit 1
fi

echo
echo "ALL TESTS PASSED FOR cta-restore-deleted-files"

echo
echo "CLEAN UP TEMPORARY FILES AND REMOVE TEMPORARY CTA ADMIN ACCESS"
rm ${METADATA_FILE_AFTER_RESTORE_PATH}
rm ${METADATA_FILE_PATH}
rm ${EOS_METADATA_AFTER_RESTORE_PATH}
