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

compare_timestamps() {
  local ts1="$1"
  local ts2="$2"

  local modify_ts1=$(echo "$ts1" | grep "Modify:" | awk -F'Timestamp:' '{print $2}' | xargs)
  local birth_ts1=$(echo "$ts1" | grep "Birth:" | awk -F'Timestamp:' '{print $2}' | xargs)

  local modify_ts2=$(echo "$ts2" | grep "Modify:" | awk -F'Timestamp:' '{print $2}' | xargs)
  local birth_ts2=$(echo "$ts2" | grep "Birth:" | awk -F'Timestamp:' '{print $2}' | xargs)

  echo "Comparing Modify timestamps: $modify_ts1 to $modify_ts2"
  echo "Comparing Birth timestamps: $birth_ts1 to $birth_ts2"

  # Ensure modify and birth match. Change does not matter
  if [[ "$modify_ts1" == "$modify_ts2" && "$birth_ts1" == "$birth_ts2" ]]; then
    return 0
  else
    echo "Timestamp modification and birth values do not match for tape"
    return 1
  fi
}


EOSINSTANCE=ctaeos
TEST_FILE_NAME=$(uuidgen | sed 's/-//g')
TEST_DIR=/eos/ctaeos/cta/

# get some common useful helpers for krb5
eospower_kdestroy
eospower_kinit

# Archive a file
echo "xrdcp /etc/group root://${EOSINSTANCE}/${TEST_DIR}${TEST_FILE_NAME}"
xrdcp /etc/group root://${EOSINSTANCE}/${TEST_DIR}${TEST_FILE_NAME}

fileInfoBeforeArchive=$(eos root://${EOSINSTANCE} fileinfo ${TEST_DIR}${TEST_FILE_NAME})

wait_for_archive ${EOSINSTANCE} "${TEST_DIR}${TEST_FILE_NAME}"

fileInfoAfterArchive=$(eos root://${EOSINSTANCE} fileinfo ${TEST_DIR}${TEST_FILE_NAME})

echo "Comparing modify/birth timestampts before archival and after archival"
if ! compare_timestamps "$fileInfoBeforeArchive" "$fileInfoAfterArchive"; then
  echo "Modify/birth timestamps of the file before archiving and after archiving do not match"
  return 1
fi

# Wait a little bit to ensure the timestamps had the opportunity to change
sleep 0.1

# Evict file from disk buffer
echo "Trigerring EOS evict workflow as poweruser1:powerusers (12001:1200)"
KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOSINSTANCE} prepare -e ${TEST_DIR}${TEST_FILE_NAME}
wait_for_evict ${EOSINSTANCE} "${TEST_DIR}${TEST_FILE_NAME}"


# Retrieve file from tape
echo
echo "Trigerring EOS retrieve workflow as poweruser1:powerusers (12001:1200)"
# We need the -s as we are staging the files from tape (see xrootd prepare definition)
KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOSINSTANCE} prepare -s ${TEST_DIR}${TEST_FILE_NAME}
# Wait for the copy to appear on disk
wait_for_retrieve ${EOSINSTANCE} "${TEST_DIR}${TEST_FILE_NAME}"

fileInfoAfterRetrieve=$(eos root://${EOSINSTANCE} fileinfo ${TEST_DIR}${TEST_FILE_NAME})

echo "Comparing modify/birth timestampts before archival and after retrival"
if ! compare_timestamps "$fileInfoBeforeArchive" "$fileInfoAfterRetrieve"; then
  echo "Modify/birth timestamps of the file before archiving and after retrieving do not match"
  return 1
fi

# Wait a little bit to ensure the timestamps had the opportunity to change
sleep 0.1

# Evict again
echo "Trigerring EOS evict workflow as poweruser1:powerusers (12001:1200)"
KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOSINSTANCE} prepare -e ${TEST_DIR}${TEST_FILE_NAME}
wait_for_evict ${EOSINSTANCE} "${TEST_DIR}${TEST_FILE_NAME}"

fileInfoAfterEvict=$(eos root://${EOSINSTANCE} fileinfo ${TEST_DIR}${TEST_FILE_NAME})

echo "Comparing modify/birth timestampts before archival and after final evict"
if ! compare_timestamps "$fileInfoBeforeArchive" "$fileInfoAfterEvict"; then
  echo "Modify/birth timestamps of the file before archiving and after evicting do not match"
  return 1
fi

# Delete the file so it doesn't interfere with tests in client_ar.sh
echo "eos root://${EOSINSTANCE} rm ${TEST_DIR}${TEST_FILE_NAME}"
eos root://${EOSINSTANCE} rm ${TEST_DIR}${TEST_FILE_NAME}
