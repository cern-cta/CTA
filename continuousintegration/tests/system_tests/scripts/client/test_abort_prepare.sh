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

set -e
set -x

# Quick function to abort the prepare of files.
# $1: file containing even number of lines, odd lines == req id; even lines == file_name
abortFile() {
  while read -r REQ_ID; do
    read -r FILE_PATH
    FILE_NAME=$(echo ${FILE_PATH} | cut -d/ -f2)
    KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_MGM_HOST} prepare -a ${REQ_ID} ${EOS_DIR}/${FILE_PATH} 2>${ERROR_DIR}/RETRIEVE_${FILE_NAME} && rm ${ERROR_DIR}/RETRIEVE_${FILE_NAME} || echo ERROR with xrootd prepare stage for file ${FILE_NAME}, full logs in ${ERROR_DIR}/RETRIEVE_${FILE_NAME} | ( grep ^ERROR || true)
  done < $1
}
export -f abortFile

# Put drives down.
echo "$(date +%s): Sleeping 3 seconds to let previous sessions finish."
sleep 3
admin_kdestroy &>/dev/null || true
admin_kinit &>/dev/null

put_all_drives_down

# Stage.
retrieve='KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs root://${EOS_MGM_HOST} prepare -s ${EOS_DIR}/${subdir}/${subdir}TEST_FILE_NAME?activity=T0Reprocess'
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  echo -n "Retrieving files to ${EOS_DIR}/${subdir} using ${NB_PROCS} processes..."
  xrdfs_call=$(eval echo "${retrieve}")
  seq -w 0 $((${NB_FILES}-1)) | xargs --max-procs=${NB_PROCS} bash -c "$xrdfs_call"
done

# Wait for requests to be generated
sleep 1

# Ensure all requests files are queued
requestsTotal=$(admin_cta --json sq | jq 'map(select (.mountType == "RETRIEVE") | .queuedFiles | tonumber) | add')
echo "Retrieve requests count: ${requestsTotal}"
filesCount=${NB_FILES}
if [[ ${requestsTotal} -ne ${filesCount} ]]; then
    echo "ERROR: Retrieve queue(s) size mismatch: ${requestsTotal} requests queued for ${filesCount} files."
    exit 1
fi

# Cancel Stage
# Abort prepare -s requests
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  echo -n "Cancelling prepare for files in ${EOS_DIR}/${subdir} using ${NB_PROCS} processes (prepare_abort)..."
  ls reqid_* | xargs --max-procs=${NB_PROCS} -I{} bash -c "abortFile {}"
  echo Done.
done
rm -f reqid_*


REMAINING_REQUESTS=$(admin_cta --json sq | jq -r 'map(select (.mountType == "RETRIEVE") | .queuedFiles | tonumber) | add')
echo "${REMAINING_REQUESTS} requests remaining."

if [[ ${REMAINING_REQUESTS} -ne $((NB_FILES * NB_DIRS)) ]]; then
  echo "ERROR: Not all files were queued for abort prepare test."
  exit 1
fi

# Put drive(s) back up to clear the queue
put_all_drives_up

echo "$(date +%s): Waiting for retrieve queues to be cleared:"
sleep 5

SECONDS_PASSED=0
WAIT_FOR_RETRIEVE_QUEUES_CLEAR_TIMEOUT=$((60))
# Wait for Requests to be removed.
while [[ ${REMAINING_REQUESTS} -gt 0 ]]; do
  echo "$(date +%s): Waiting for retrieve queues to be cleared: Seconds passed = ${SECONDS_PASSED}"
  sleep 1
  let SECONDS_PASSED=SECONDS_PASSED+1

  if test ${SECONDS_PASSED} == ${WAIT_FOR_RETRIEVE_QUEUES_CLEAR_TIMEOUT}; then
    echo "$(date +%s): Timed out after ${WAIT_FOR_RETRIEVE_QUEUES_CLEAR_TIMEOUT} seconds waiting for retrieve queues to be cleared"
    break
  fi

  REMAINING_REQUESTS=$(admin_cta --json sq | jq -r 'map(select (.mountType == "RETRIEVE") | .queuedFiles | tonumber) | add');
  # Prevent the result from being empty
  if [ -z "$REMAINING_REQUEST" ]; then REMAINING_REQUESTS='0'; fi
  echo "${REMAINING_REQUESTS} requests remaining."
done

# Check that the files were not retrieved
echo "Checking restaged files..."
RESTAGEDFILES=0
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  RF=$(eos root://${EOS_MGM_HOST} ls -y ${EOS_DIR}/${subdir} | grep -E '^d[1-9][0-9]*::t1' | wc -l)
  echo "Restaged files in directory ${subdir}: ${RF}"
  (( RESTAGEDFILES += ${RF} ))
done
echo "Total restaged files found: ${RESTAGEDFILES}"

if [ "0" != "$(ls ${ERROR_DIR} 2> /dev/null | wc -l)" ]; then
  # there were some prepare errors
  echo "Several errors occured during prepare cancel test!"
  echo "Please check client pod logs in artifacts"
  mv ${ERROR_DIR}/* ${LOGDIR}/xrd_errors/
fi

if [ ${RESTAGEDFILES} -ne "0" ]; then
  echo "ERROR some files were retrieved in spite of retrieve cancellation."
  exit 1
fi
