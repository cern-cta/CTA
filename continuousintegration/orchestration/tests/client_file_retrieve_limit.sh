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

EOSINSTANCE=ctaeos
TEST_FILE_NAME=$(uuidgen | sed 's/-//g')
TEST_DIR=/eos/ctaeos/cta/

# get some common useful helpers for krb5
eospower_kdestroy
eospower_kinit

echo "xrdcp /etc/group root://${EOSINSTANCE}/${TEST_DIR}${TEST_FILE_NAME}"
xrdcp /etc/group root://${EOSINSTANCE}/${TEST_DIR}${TEST_FILE_NAME}

wait_for_archive ${EOSINSTANCE} "${TEST_DIR}${TEST_FILE_NAME}"

echo
echo "FILE ARCHIVED TO TAPE"
echo
eos root://${EOSINSTANCE} info ${TEST_DIR}${TEST_FILE_NAME}

echo
echo "Information about the testing file:"
echo "********"
  eos root://${EOSINSTANCE} attr ls ${TEST_DIR}${TEST_FILE_NAME}
  eos root://${EOSINSTANCE} ls -l ${TEST_DIR}${TEST_FILE_NAME}
  eos root://${EOSINSTANCE} info ${TEST_DIR}${TEST_FILE_NAME}

echo
echo "Removing disk replica as poweruser1:powerusers (12001:1200)"
XrdSecPROTOCOL=sss eos -r 0 0 root://${EOSINSTANCE} file drop "${TEST_DIR}${TEST_FILE_NAME}" 1


echo
echo "Information about the testing file without disk replica"
  eos root://${EOSINSTANCE} ls -l ${TEST_DIR}${TEST_FILE_NAME}
  eos root://${EOSINSTANCE} info ${TEST_DIR}${TEST_FILE_NAME}

echo
echo "Putting drives down..."
IFS=' ' read -r -a dr_names <<< "$(admin_cta --json dr ls | jq -r '.[] | .driveName')"
for drive in "${dr_names[@]}"; do
  log_message "Putting drive $drive down."
  admin_cta dr down "$drive" -r "File retrieve limit test"
done
sleep 3

echo
echo "Trigerring EOS retrieve workflow as poweruser1:powerusers (12001:1200), 64 times, to fill max request ID quota"
# We need the -s as we are staging the files from tape (see xrootd prepare definition)
for i in {1..64}
do
  REQUEST_ID=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOSINSTANCE} prepare -s ${TEST_DIR}${TEST_FILE_NAME})
  QUERY_RSP=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOSINSTANCE} query prepare ${REQUEST_ID} ${TEST_DIR}${TEST_FILE_NAME})
  PATH_EXISTS=$(echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE_OK}\").path_exists")
  REQUESTED=$(echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE_OK}\").requested")
  HAS_REQID=$(echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE_OK}\").has_reqid")
  ERROR_TEXT=$(echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE_OK}\").error_text")

  if [[
    "true" != "${PATH_EXISTS}" ||
    "true" != "${REQUESTED}" ||
    "true" != "${HAS_REQID}" ||
    "\"\"" != "${ERROR_TEXT}" ]]
  then
    echo "ERROR: The ${i}th retrieve requests should have been received without failure"
    exit 1
  fi
done

echo
echo "Trigerring EOS retrieve workflow as poweruser1:powerusers (12001:1200) one more time, should fail to create request ID"
REQUEST_ID=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOSINSTANCE} prepare -s ${TEST_DIR}${TEST_FILE_NAME})

echo "Checking that the 65th retrieve request failed"
QUERY_RSP=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOSINSTANCE} query prepare ${REQUEST_ID} ${TEST_DIR}${TEST_FILE_NAME})
PATH_EXISTS=$(echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE_OK}\").path_exists")
REQUESTED=$(echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE_OK}\").requested")
HAS_REQID=$(echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE_OK}\").has_reqid")
ERROR_TEXT=$(echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE_OK}\").error_text")

if [[
  "true" != "${PATH_EXISTS}" ||
  "true" != "${REQUESTED}" ||
  "false" != "${HAS_REQID}" ||
  "\"\"" == "${ERROR_TEXT}" ]]
then
  echo "ERROR: New request was not rejected properly after 64 retrieve requests have been accumulated"
  exit 1
fi
echo "Test completed successfully"

# Delete the file so it doesn't interfere with tests in client_ar.sh
echo "eos root://${EOSINSTANCE} rm ${TEST_DIR}${TEST_FILE_NAME}"
eos root://${EOSINSTANCE} rm ${TEST_DIR}${TEST_FILE_NAME}
