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

EOSINSTANCE=ctaeos
LOGFILE_PATH=/var/log/restore_files.log
TEST_FILE_NAME=`uuidgen`
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

FRONTEND_IP=$(kubectl -n ${NAMESPACE} get pods ctafrontend -o json | jq .status.podIP | tr -d '"')

echo
echo "ADD FRONTEND GATEWAY TO EOS"
echo "kubectl -n ${NAMESPACE} exec ctaeos -- bash eos root://${EOSINSTANCE} -r 0 0 vid add gateway ${FRONTEND_IP} grpc"
kubectl -n ${NAMESPACE} exec ctaeos -- eos -r 0 0 vid add gateway ${FRONTEND_IP} grpc

echo
echo "eos vid ls"
kubectl -n ${NAMESPACE} exec ctaeos -- eos root://${EOSINSTANCE} vid ls

echo
echo "Launching restore_files_client.sh on client pod"
echo " Archiving file: xrdcp as user1"
kubectl -n ${NAMESPACE} cp common/archive_file.sh client:/root/archive_file.sh
kubectl -n ${NAMESPACE} cp client_helper.sh client:/root/client_helper.sh
kubectl -n ${NAMESPACE} exec client -- bash /root/archive_file.sh -f ${TEST_FILE_NAME} || exit 1

echo
METADATA_FILE_PATH=$(mktemp -d).json
echo "SEND FILE METADATA TO JSON FILE: ${METADATA_FILE_PATH}"
touch ${METADATA_FILE_PATH}
kubectl -n ${NAMESPACE} exec client -- eos -j root://${EOSINSTANCE} file info /eos/ctaeos/cta/${TEST_FILE_NAME} | jq . > ${METADATA_FILE_PATH}

# Extract values from the meta data used for restoring and testing
FXID=$(jq -r '.fxid' ${METADATA_FILE_PATH})
FILE_SIZE=$(jq -r '.size' ${METADATA_FILE_PATH})
ARCHIVE_FILE_ID=$(jq -r '.xattr | .["sys.archive.file_id"]' ${METADATA_FILE_PATH})
CHECKSUM=$(jq -r '.checksumvalue' ${METADATA_FILE_PATH})

echo
echo "DELETE ARCHIVED FILE"
kubectl -n ${NAMESPACE} cp common/delete_file.sh client:/root/delete_file.sh
kubectl -n ${NAMESPACE} exec client -- bash /root/delete_file.sh -i ${EOSINSTANCE} -f ${TEST_FILE_NAME}

echo
echo "VALIDATE THAT THE FILE IS IN THE RECYCLE BIN"
echo "kubectl -n ${NAMESPACE} exec ctacli -- cta-admin rtf ls --fxid ${FXID} || exit 1"
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin rtf ls --fxid ${FXID} || exit 1

echo 
echo "COPY REQUIRED FILES TO FRONTEND POD"
echo "sudo kubectl cp ${NAMESPACE}/ctacli:/etc/cta/cta-cli.conf /etc/cta/cta-cli.conf"
echo "sudo kubectl cp /etc/cta/cta-cli.conf ${NAMESPACE}/ctafrontend:/etc/cta/cta-cli.conf"
sudo kubectl cp ${NAMESPACE}/ctacli:/etc/cta/cta-cli.conf /etc/cta/cta-cli.conf
sudo kubectl cp /etc/cta/cta-cli.conf ${NAMESPACE}/ctafrontend:/etc/cta/cta-cli.conf

echo
echo "ENABLE CTAFRONTEND TO EXECUTE CTA ADMIN COMMANDS"
kubectl --namespace=${NAMESPACE} exec kdc -- cat /root/ctaadmin2.keytab | kubectl --namespace=${NAMESPACE} exec -i ctafrontend --  bash -c "cat > /root/ctaadmin2.keytab; mkdir -p /tmp/ctaadmin2"
kubectl -n ${NAMESPACE} cp client_helper.sh ctafrontend:/root/client_helper.sh
rm /tmp/init_kerb.sh
touch /tmp/init_kerb.sh
echo '. /root/client_helper.sh; admin_kinit' >> /tmp/init_kerb.sh
kubectl -n ${NAMESPACE} cp /tmp/init_kerb.sh ctafrontend:/tmp/init_kerb.sh
kubectl -n ${NAMESPACE} exec ctafrontend -- bash /tmp/init_kerb.sh

echo
echo "RESTORE FILES"
kubectl -n ${NAMESPACE} cp client_helper.sh ctafrontend:/root/client_helper.sh
kubectl cp ~/CTA-build/cmdline/standalone_cli_tools/restore_files/cta-restore-deleted-files ${NAMESPACE}/ctafrontend:/usr/bin/cta-restore-deleted-files
kubectl cp restore_files_ctafrontend.sh ${NAMESPACE}/ctafrontend:/root/restore_files_ctafrontend.sh
kubectl -n ${NAMESPACE} exec ctafrontend -- chmod +x /root/restore_files_ctafrontend.sh
kubectl -n ${NAMESPACE} exec ctafrontend -- bash -c "XrdSecPROTOCOL=krb5 KRB5CCNAME=/tmp/ctaadmin2/krb5cc_0 /root/restore_files_ctafrontend.sh -I ${ARCHIVE_FILE_ID} -f ${TEST_FILE_NAME} -i ${EOSINSTANCE}"

SECONDS_PASSED=0
WAIT_FOR_RETRIEVED_FILE_TIMEOUT=10
while kubectl -n ${NAMESPACE} exec client -- test `false = xrdfs root://${EOSINSTANCE} query prepare 0 /eos/ctaeos/cta/${TEST_FILE_NAME} | jq . | jq '.responses[0] | .path_exists'`; do
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
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin --json tf ls --id ${ARCHIVE_FILE_ID} --instance ${EOSINSTANCE} | jq '.[0]' |& tee ${METADATA_FILE_AFTER_RESTORE_PATH}
kubectl -n ${NAMESPACE} exec ctacli -- cta-admin --json tf ls --id ${ARCHIVE_FILE_ID} --instance ${EOSINSTANCE} | jq '.[0]' | sudo tee -a ${LOGFILE_PATH}

# Extract values from the meta data from the restored file
FILE_SIZE_AFTER_RESTORE=$(jq -r '.af | .["size"]' ${METADATA_FILE_AFTER_RESTORE_PATH})
ARCHIVE_FILE_ID_AFTER_RESTORE=$(jq -r '.af | .["archiveId"]' ${METADATA_FILE_AFTER_RESTORE_PATH})
CHECKSUM_AFTER_RESTORE=$(jq -r '.af | .checksum[0] | .["value"]' ${METADATA_FILE_AFTER_RESTORE_PATH})
FXID_AFTER_RESTORE=$(jq -r '.df | .["diskId"]' ${METADATA_FILE_AFTER_RESTORE_PATH})

EOS_METADATA_AFTER_RESTORE_PATH=$(mktemp -d).json
echo "SEND EOS METADATA TO JSON FILE: ${EOS_METADATA_AFTER_RESTORE_PATH}"
touch ${EOS_METADATA_AFTER_RESTORE_PATH}
kubectl -n ${NAMESPACE} exec client -- eos -j root://${EOSINSTANCE} file info /eos/ctaeos/cta/${TEST_FILE_NAME} | jq . |& tee ${EOS_METADATA_AFTER_RESTORE_PATH}
kubectl -n ${NAMESPACE} exec client -- eos -j root://${EOSINSTANCE} file info /eos/ctaeos/cta/${TEST_FILE_NAME} | jq . | sudo tee -a ${LOGFILE_PATH}

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
echo "kubectl -n ${NAMESPACE} exec ctacli -- cta-admin admin rm --username ctafrontend"
echo "kubectl -n ${NAMESPACE} exec ctacli -- cta-admin admin rm --username ctaeos"
sudo rm ${METADATA_FILE_AFTER_RESTORE_PATH}
sudo rm ${METADATA_FILE_PATH}
sudo rm ${EOS_METADATA_AFTER_RESTORE_PATH}
