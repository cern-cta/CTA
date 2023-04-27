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


# Quick function to abort the prepare of files.
# $1: file containing even number of lines, odd lines == req id; even lines == file_name
abortFile(){
  while read -r REQ_ID; do
    read -r FILE_PATH
    FILE_NAME=$(echo ${FILE_PATH} | cut -d/ -f2)
    XRD_LOGLEVEL=Dump KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOSINSTANCE} prepare -a ${REQ_ID} ${EOS_DIR}/${FILE_PATH} 2>${ERROR_DIR}/RETRIEVE_${FILE_NAME} && rm ${ERROR_DIR}/RETRIEVE_${FILE_NAME} || echo ERROR with xrootd prepare stage for file ${FILE_NAME}, full logs in ${ERROR_DIR}/RETRIEVE_${FILE_NAME} | grep ^ERROR
  done < $1
}

export -f abortFile

# Build the list of tape only files.
STATUS_FILE=$(mktemp)
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  eos root://${EOSINSTANCE} ls -y ${EOS_DIR}/${subdir} | egrep 'd0::t[^0]' | sed -e "s%\s\+% %g;s%.* \([^ ]\+\)$%\1%" >> ${STATUS_FILE}
done

if [[ $(cat ${STATUS_FILE} | wc -l )  -eq 0 ]]; then
  echo "ERROR: Can't run abort prepare test as there are no tape only files."
  exit 1
fi

# Put drives down.
echo "Sleeping 3 seconds to let previous sessions finish."
sleep 3
admin_kdestroy &>/dev/null
admin_kinit &>/dev/null
INITIAL_DRIVES_STATE=`admin_cta --json dr ls`
echo INITIAL_DRIVES_STATE:
echo ${INITIAL_DRIVES_STATE} | jq -r '.[] | [ .driveName, .driveStatus] | @tsv' | column -t
echo -n "Will put down those drives : "
drivesToSetDown=`echo ${INITIAL_DRIVES_STATE} | jq -r '.[] | select (.driveStatus == "UP") | .driveName'`
echo $drivesToSetDown
for d in `echo $drivesToSetDown`; do
  admin_cta drive down $d --reason "PUTTING DRIVE DOWN FOR TESTS"
done

# Wait for drives to be down.
echo "$(date +%s): Waiting for the drives to be down"
SECONDS_PASSED=0
WAIT_FOR_DRIVES_DOWN_TIMEOUT=$((10))
while [[ $SECONDS_PASSED < WAIT_FOR_DRIVES_DOWN_TIMEOUT ]]; do
  sleep 1
  oneStatusUpRemaining=0
  for d in `echo $drivesToSetDown`; do
    status=`admin_cta --json drive ls | jq -r ". [] | select(.driveName == \"$d\") | .driveStatus"`
    if [[ $status == "UP" ]]; then
      oneStatusUpRemaining=1
    fi;
  done
  if [[ $oneStatusUpRemaining -eq 0 ]]; then
    echo "Drives : $drivesToSetDown are down"
    break;
  fi
  echo -n "."
  SECONDS_PASSED=$SECONDS_PASSED+1
  if [[ $SECONDS_PASSED -gt $WAIT_FOR_DRIVES_DOWN_TIMEOUT ]]; then
    die "ERROR: Timeout reach for trying to put all drives down"
  fi
done

# Stage.
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
    echo -n "Retrieving files to ${EOS_DIR}/${subdir} using ?? process (prepare2)..."

    for((slot=1; slot <= ${NB_PROCS}; slot++)); do
      touch "slot${slot}"
    done

    # NOTE: Parallel max-procs options are in alpha testing according to
    # the docs. At the moment just run parallel without the flag making
    # parallel take the decission.
    cat ${STATUS_FILE} | parallel bash -c "true && XRD_LOGLEVEL=Dump KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOSINSTANCE} prepare -s ${EOS_DIR}/${subdir}/{} 2>${ERROR_DIR}/RETRIEVE_{} | tee -a slot\"{%}\" && echo ${subdir}/{} >> slot\"{%}\" && rm ${ERROR_DIR}/RETRIEVE_{} || echo ERROR with xrootd prepare stage for file {}, full logs in ${ERROR_DIR}/RETRIEVE_{}" | grep ^ERROR
    find . -size 0 | xargs rm -f
    echo Done.
  done
  if [ "0" != "$(ls ${ERROR_DIR} 2> /dev/null | wc -l)" ]; then
    # there were some prepare errors
    echo "Several prepare errors occured during retrieval!"
    echo "Please check client pod logs in artifacts"
    mv ${ERROR_DIR}/* ${LOGDIR}/xrd_errors/
  fi
  sleep 3
  # Ensure all requests files are queued
  requestsTotal=`admin_cta --json sq | jq 'map(select (.mountType == "RETRIEVE") | .queuedFiles | tonumber) | add'`
  echo "Retrieve requests count: ${requestsTotal}"
  filesCount=`cat ${STATUS_FILE} | wc -l`
  if [ ${requestsTotal} -ne ${filesCount} ]; then
    echo "ERROR: Retrieve queue(s) size mismatch: ${requestsTotal} requests queued for ${filesCount} files."
  fi
  sleep 3
  # Cancel Stage
  # Abort prepare -s requests
  for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
    echo -n "Cancelling prepare for files in ${EOS_DIR}/${subdir} using ??  process (prepare_abort)..."
    ls slot* | parallel abortFile {}
    echo Done.
  done
  rm -f slot*
  # Put drive(s) back up to clear the queue
  echo -n "Will put back up those drives : "
  echo ${INITIAL_DRIVES_STATE} | jq -r '.[] | select (.driveStatus == "UP") | .driveName'
  for d in `echo ${INITIAL_DRIVES_STATE} | jq -r '.[] | select (.driveStatus == "UP") | .driveName'`; do
  admin_cta dr up $d
done

sleep 10

# Check that queues are empty after a while and files did not get retrieved
echo "$(date +%s): Waiting for retrieve queues to be cleared:"
SECONDS_PASSED=0
WAIT_FOR_RETRIEVE_QUEUES_CLEAR_TIMEOUT=$((60))
REMAINING_REQUESTS=`admin_cta --json sq | jq -r 'map(select (.mountType == "RETRIEVE") | .queuedFiles | tonumber) | add'`
echo "${REMAINING_REQUESTS} requests remaining."
# Prevent the result from being empty
if [ -z "$REMAINING_REQUESTS" ]; then REMAINING_REQUESTS='0'; fi
while [[ ${REMAINING_REQUESTS} > 0 ]]; do
  echo "$(date +%s): Waiting for retrieve queues to be cleared: Seconds passed = ${SECONDS_PASSED}"
  sleep 1
  let SECONDS_PASSED=SECONDS_PASSED+1

  if test ${SECONDS_PASSED} == ${WAIT_FOR_RETRIEVE_QUEUES_CLEAR_TIMEOUT}; then
    echo "$(date +%s): Timed out after ${WAIT_FOR_RETRIEVE_QUEUES_CLEAR_TIMEOUT} seconds waiting for retrieve queues to be cleared"
    break
  fi

  REMAINING_REQUESTS=`admin_cta --json sq | jq -r 'map(select (.mountType == "RETRIEVE") | .queuedFiles | tonumber) | add'`;
  # Prevent the result from being empty
  if [ -z "$REMAINING_REQUEST" ]; then REMAINING_REQUESTS='0'; fi
  echo "${REMAINING_REQUESTS} requests remaining."
done

# Check that the files were not retrieved
echo "Checking restaged files..."
RESTAGEDFILES=0

db_begin_transaction
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  eos root://${EOSINSTANCE} ls -y ${EOS_DIR}/${subdir} | egrep '^d[1-9][0-9]*::t1' | awk -v sd="${subdir}/"  '{print sd$10}' > ${STATUS_FILE}

  RF=$(cat $STATUS_FILE | wc -l)
  echo "Restaged files in directory ${subdir}: ${RF}"
  (( RESTAGEDFILES += ${RF} ))
  rm -f ${STATUS_FILE}
  touch ${STATUS_FILE}
  eos root://${EOSINSTANCE} ls -y ${EOS_DIR}/${subdir} | egrep '^d0::t1' | awk -v sd="${subdir}/" '{print sd$10}' > ${STATUS_FILE}
  cat ${STATUS_FILE} | xargs -iFILE bash -c "db_update aborted FILE 1 '='"
done
echo "Total restaged files found: ${RESTAGEDFILES}"
db_commit_transaction

if [ "0" != "$(ls ${ERROR_DIR} 2> /dev/null | wc -l)" ]; then
  # there were some prepare errors
  echo "Several errors occured during prepare cancel test!"
  echo "Please check client pod logs in artifacts"
  mv ${ERROR_DIR}/* ${LOGDIR}/xrd_errors/
fi

if [ ${RESTAGEDFILES} -ne "0" ]; then
  #((RC++))
  echo 'ERROR some files were retrieved in spite of retirve cancellation.' >> /tmp/RC
  echo "ERROR some files were retrieved in spite of retrieve cancellation."
fi

rm -f ${STATUS_FILE}
