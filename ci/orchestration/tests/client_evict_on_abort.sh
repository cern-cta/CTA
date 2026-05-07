#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Function to abort the prepare of files.
abortFile() {
  while read -r REQ_ID; do
    read -r FILE_PATH
    FILE_NAME=${FILE_PATH##*/}
    XRD_LOGLEVEL=Dump KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_MGM_HOST} prepare -a ${REQ_ID} ${EOS_DIR}/${FILE_PATH} 2>${ERROR_DIR}/RETRIEVE_${FILE_NAME} && rm ${ERROR_DIR}/RETRIEVE_${FILE_NAME} || echo ERROR with xrootd prepare stage for file ${FILE_NAME}, full logs in ${ERROR_DIR}/RETRIEVE_${FILE_NAME} | grep ^ERROR
  done < $1
}
export -f abortFile

# Retrieve files, to trigger the workflow
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  echo -n "Retrieving files to ${EOS_DIR}/${subdir} using ${NB_PROCS} processes..."

  xrdfs_call=$(eval echo "${retrieve}")
  xrdfs_call+=" 2>${ERROR_DIR}/RETRIEVE_${subdir}TEST_FILE_NAME"

  xrdfs_success="rm ${ERROR_DIR}/RETRIEVE_${subdir}TEST_FILE_NAME "

  xrdfs_error=" echo ERROR with xrootd prepare stage for file ${subdir}/${subdir}TEST_FILE_NAME, full logs in ${ERROR_DIR}/RETRIEVE_${subdir}TEST_FILE_NAME "

  command_str="${xrdfs_call} | tee -a reqid_\"\${index}\" && echo ${subdir}/${subdir}TEST_FILE_NAME >> reqid_\"\${index}\" && ${xrdfs_success} || ${xrdfs_error}"

  seq -w 0 $((${NB_FILES}-1)) | \
    xargs --process-slot-var=index --max-procs=${NB_PROCS} -iTEST_FILE_NAME bash -c "$command_str" | \
      tee ${LOGDIR}/prepare_${subdir}.log | \
      grep ^ERROR

  echo Done.

  # DANGER: compatibility matrix hell... See:
  # - xattr removed from Fsctl breaks CTA CI - EOS-6073
  # - Errors for EOS 5.2.8 - CTA#615
  # Broken if eos >= 5.2.8
  # default to xrootd 5 call tested with eos >= 5.2.17
  xrdfs4_call="XRD_LOGLEVEL=Dump KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 /opt/eos/xrootd/bin/xrdfs ${EOS_MGM_HOST} query opaquefile ${EOS_DIR}/${subdir}/${subdir}TEST_FILE_NAME?mgm.pcmd=xattr\&mgm.subcmd=get\&mgm.xattrname=sys.retrieve.req_id 2>${ERROR_DIR}/XATTRGET_${subdir}TEST_FILE_NAME && rm ${ERROR_DIR}/XATTRGET_${subdir}TEST_FILE_NAME"
  xrdfs_call="XRD_LOGLEVEL=Dump KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 /opt/eos/xrootd/bin/xrdfs ${EOS_MGM_HOST} xattr ${EOS_DIR}/${subdir}/${subdir}TEST_FILE_NAME get sys.retrieve.req_id 2>${ERROR_DIR}/XATTRGET_${subdir}TEST_FILE_NAME && rm ${ERROR_DIR}/XATTRGET_${subdir}TEST_FILE_NAME"

  (/opt/eos/xrootd/bin/xrdcp -V 2>&1 | grep -q -e '^v*4\.') && xrdfs_call=${xrdfs4_call}

  xrdfs_error=" echo ERROR with xrootd xattr get for file ${subdir}TEST_FILE_NAME, full logs in ${ERROR_DIR}/XATTRGET_${subdir}TEST_FILE_NAME"

  command_str="${xrdfs_call} || ${xrdfs_error}"

  # We should better deal with errors
  seq -w 0 $((${NB_FILES}-1)) | xargs --max-procs=${NB_PROCS} -iTEST_FILE_NAME bash -c "$command_str" | tee ${LOGDIR}/prepare_sys.retrieve.req_id_${subdir}.log | grep ^ERROR
done

if [[ "0" != "$(ls ${ERROR_DIR} 2> /dev/null | wc -l)" ]]; then
  # there were some prepare errors
  echo "Several prepare errors occurred during retrieval!"
  echo "Please check client pod logs in artifacts"
  mv ${ERROR_DIR}/* ${LOGDIR}/xrd_errors/
fi

# Check that all files are retrieved correctly
TO_BE_RETRIEVED=$(( ${NB_FILES} * ${NB_DIRS} - $(ls ${ERROR_DIR}/RETRIEVE_* 2>/dev/null | wc -l) ))
RETRIEVING=${TO_BE_RETRIEVED}
RETRIEVED=0
# Wait for the copy to appear on disk
echo "$(date +%s): Waiting for files to be back on disk:"
SECONDS_PASSED=0
WAIT_FOR_RETRIEVED_FILE_TIMEOUT=$((40+${NB_FILES}/5))

while [[ 0 -lt ${RETRIEVING} ]]; do
  echo "$(date +%s): Waiting for files to be retrieved from tape: (${SECONDS_PASSED}s)"
  let SECONDS_PASSED=SECONDS_PASSED+1

  if [[ ${SECONDS_PASSED} == ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT} ]]; then
    echo "$(date +%s): Timed out after ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT} seconds waiting for file to be retrieved tape"
    break
  fi

  RETRIEVED=0
  for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
    RETRIEVED=$(( ${RETRIEVED} + $( eos root://${EOS_MGM_HOST} ls -y ${EOS_DIR}/${subdir} | grep -E '^d[1-9][0-9]*::t1' | wc -l) ))
  done
  RETRIEVING=$((${TO_BE_RETRIEVED} - ${RETRIEVED}))

  echo "${RETRIEVED}/${TO_BE_RETRIEVED} retrieved"
done

if [[ ${RETRIEVED} -ne ${TO_BE_RETRIEVED} ]]; then
  echo "ERROR: Some files were not retrieved."
  exit 1
fi

# Execute prepare abort for all files and check that they are evicted from disk as expected
echo -n "$(date +%s): Cancelling prepare for files in ${EOS_DIR}/${subdir} using ${NB_PROCS} processes (prepare_abort)..."
ls reqid_* | xargs --max-procs=${NB_PROCS} -I{} bash -c "abortFile {}"
echo Done.
rm -f reqid_*

# Confirm that files are evicted from disk as desired
TO_EVICT=$((${NB_FILES}*${NB_DIRS}))
SECONDS_PASSED=0
LEFTOVER=${TO_EVICT}

while [[ 0 -lt ${LEFTOVER} ]]; do
  echo "$(date +%s): Waiting for files to be evicted from disk after prepare abort (${SECONDS_PASSED}s)"
  let SECONDS_PASSED=SECONDS_PASSED+1

  if [[ ${SECONDS_PASSED} == 20 ]]; then
    echo "$(date +%s): Timed out after 20 seconds waiting for files to be evicted from disk after prepare abort"
    exit 1
  fi

  LEFTOVER=0
  for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
    LEFTOVER=$(( ${LEFTOVER} + $(eos root://${EOS_MGM_HOST} ls -y ${EOS_DIR}/${subdir} | grep -E '^d[1-9][0-9]*::t1' | wc -l) ))
  done

  EVICTED=$((${TO_EVICT}-${LEFTOVER}))
  echo "${EVICTED}/${TO_EVICT} evicted from disk after prepare abort"

  sleep 1
done
