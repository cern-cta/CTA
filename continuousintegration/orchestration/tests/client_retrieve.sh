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


echo "$(date +%s): Trigerring EOS retrieve workflow as poweruser1:powerusers (12001:1200)"

# Build initial string without expanding subdir


# We need the -s as we are staging the files from tape (see xrootd prepare definition)
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  echo -n "Retrieving files to ${EOS_DIR}/${subdir} using ${NB_PROCS} processes..."

  xrdfs_call=$(eval echo "${retrieve}")
  xrdfs_call+=" 2>${ERROR_DIR}/${subdir}RETRIEVE_TEST_FILE_NAME"

  xrdfs_success="rm ${ERROR_DIR}/${subdir}RETRIEVE_TEST_FILE_NAME "

  xrdfs_error=" echo ERROR with xrootd prepare stage for file ${subdir}/TEST_FILE_NAME, full logs in ${ERROR_DIR}/${subdir}RETRIEVE_TEST_FILE_NAME "

  command_str="${xrdfs_call} && ${xrdfs_success} || ${xrdfs_error}"

  seq -w 0 $((${NB_FILES}-1)) | xargs --max-procs=${NB_PROCS} -iTEST_FILE_NAME bash -c "$command_str" | tee ${LOGDIR}/prepare_${subdir}.log | grep ^ERROR

  echo Done.

  #xrdfs_call="XRD_LOGLEVEL=Dump KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOSINSTANCE} query opaquefile ${EOS_DIR}/${subdir}/TEST_FILE_NAME?mgm.pcmd=xattr\&mgm.subcmd=get\&mgm.xattrname=sys.retrieve.req_id 2>${ERROR_DIR}/XATTRGET_TEST_FILE_NAME && rm ${ERROR_DIR}/XATTRGET_TEST_FILE_NAME"
  xrdfs_call="XRD_LOGLEVEL=Dump KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOSINSTANCE} xattr ${EOS_DIR}/${subdir}/TEST_FILE_NAME get sys.retrieve.req_id 2>${ERROR_DIR}/XATTRGET_TEST_FILE_NAME && rm ${ERROR_DIR}/XATTRGET_TEST_FILE_NAME"  

  xrdfs_error=" echo ERROR with xrootd xattr get for file TEST_FILE_NAME, full logs in ${ERROR_DIR}/XATTRGET_TEST_FILE_NAME"

  command_str="${xrdfs_call} || ${xrdfs_error}"

  seq -w 0 $((${NB_FILES}-1)) | xargs --max-procs=${NB_PROCS} -iTEST_FILE_NAME bash -c "$command_str" | tee ${LOGDIR}/prepare_sys.retrieve.req_id_${subdir}.log | grep ^ERROR
done

if [ "0" != "$(ls ${ERROR_DIR} 2> /dev/null | wc -l)" ]; then
  # there were some prepare errors
  echo "Several prepare errors occured during retrieval!"
  echo "Please check client pod logs in artifacts"
  mv ${ERROR_DIR}/* ${LOGDIR}/xrd_errors/
fi

TO_BE_RETRIEVED=$(( ${NB_FILES} * ${NB_DIRS} - $(ls ${ERROR_DIR}/RETRIEVE_* 2>/dev/null | wc -l) ))
RETRIEVING=${TO_BE_RETRIEVED}
RETRIEVED=0
# Wait for the copy to appear on disk
echo "$(date +%s): Waiting for files to be back on disk:"
SECONDS_PASSED=0
WAIT_FOR_RETRIEVED_FILE_TIMEOUT=$((40+${NB_FILES}/5))

while test 0 -lt ${RETRIEVING}; do
  echo "$(date +%s): Waiting for files to be retrieved from tape: Seconds passed = ${SECONDS_PASSED}"
  let SECONDS_PASSED=SECONDS_PASSED+1

  if test ${SECONDS_PASSED} == ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT}; then
    echo "$(date +%s): Timed out after ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT} seconds waiting for file to be retrieved tape"
    break
  fi

  RETRIEVED=0
  for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
    RETRIEVED=$(( ${RETRIEVED} + $( eos root://${EOSINSTANCE} ls -y ${EOS_DIR}/${subdir} | egrep '^d[1-9][0-9]*::t1' | wc -l) ))

    sleep 1 # do not hammer eos too hard
  done
  RETRIEVING=$((${TO_BE_RETRIEVED} - ${RETRIEVED}))

  echo "${RETRIEVED}/${TO_BE_RETRIEVED} retrieved"
done

echo "###"
echo "${RETRIEVED}/${TO_BE_RETRIEVED} retrieved files"
echo "###"

if [[ ${RETRIEVED} -ne ${TO_BE_RETRIEVED} ]]; then
  echo "ERROR: Some files were not retrieved."
  exit 1
fi
