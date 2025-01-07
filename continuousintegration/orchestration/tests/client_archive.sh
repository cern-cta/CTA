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


echo "$(date +%s): Creating test dir in eos: ${EOS_DIR}"

eos root://${EOS_INSTANCE} mkdir -p ${EOS_DIR} || die "Cannot create directory ${EOS_DIR} in eos instance ${EOS_INSTANCE}."

echo
echo "Listing the EOS extended attributes of ${EOS_DIR}"
eos root://${EOS_INSTANCE} attr ls ${EOS_DIR}
echo

# As we are skipping n bytes per file we need a bit more than the file size to accomodate dd to read ${FILE_KB_SIZE} skipping the n first bytes
dd if=/dev/urandom of=/tmp/testfile bs=1k count=$((${FILE_KB_SIZE} + ${NB_FILES}*${NB_DIRS}/1024 + 1)) || exit 1


# not more than 100k files per directory so that we can rm and find as a standard user
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  eos root://${EOS_INSTANCE} mkdir -p ${EOS_DIR}/${subdir} || die "Cannot create directory ${EOS_DIR}/{subdir} in eos instance ${EOS_INSTANCE}."

  echo -n "Copying files to ${EOS_DIR}/${subdir} using ${NB_PROCS} processes..."

  file_creation="dd if=/tmp/testfile bs=1k 2>/dev/null | (dd bs=$((${subdir}*${NB_FILES})) count=1 of=/dev/null 2>/dev/null; dd bs=TEST_FILE_NUM count=1 of=/dev/null 2>/dev/null; dd bs=1k count=${FILE_KB_SIZE} 2>/dev/null) "

  # The `archive` variable is sourced from cli_calls.sh without parameter
  # expansion, to be able to expand the variable afterwards we need `eval echo`
  xrdcp_call=$(eval echo "${archive}")
  xrdcp_call+=" 2>${ERROR_DIR}/${subdir}TEST_FILE_NUM"
  xrdcp_succes=" rm ${ERROR_DIR}/${subdir}TEST_FILE_NUM"

  xrdcp_error="ERROR with xrootd transfer for file ${subdir}/TEST_FILE_NUM, full logs in ${ERROR_DIR}/${subdir}TEST_FILE_NUM"

  command_str="${file_creation} | ${xrdcp_call} && ${xrdcp_succes} || ${xrdcp_error}"
  start=$(date +%s)
  echo "Starting at ${start}"
  seq -w 0 $((${NB_FILES} - 1)) | xargs --max-procs=${NB_PROCS} -iTEST_FILE_NUM bash -c "$command_str"
  end=$(date +%s)
  duration=$((end - start))
  echo "All file copies to disk for subdir ${subdir} took ${duration} seconds."
  echo Done.
done
start=$(date +%s)
echo "Timestamp after all files copied ${start}"
if [ "0" != "$(ls ${ERROR_DIR} 2> /dev/null | wc -l)" ]; then
  # there were some xrdcp errors
  echo "Several xrdcp errors occured during archival!"
  echo "Please check client pod logs in artifacts"
  mv ${ERROR_DIR}/* ${LOGDIR}/xrd_errors/
fi

COPIED=0
COPIED_EMPTY=0
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  COPIED=$(( ${COPIED} + $(eos root://${EOS_INSTANCE} find -f ${EOS_DIR}/${subdir} | wc -l) ))
  COPIED_EMPTY=$(( ${COPIED_EMPTY} + $(eos root://${EOS_INSTANCE} find -0 ${EOS_DIR}/${subdir} | wc -l) ))
done

# Only not empty files are archived by CTA
TO_BE_ARCHIVED=$((${COPIED} - ${COPIED_EMPTY}))

ARCHIVED=0
echo "$(date +%s): Waiting for files to be on tape:"
SECONDS_PASSED=0
WAIT_FOR_ARCHIVED_FILE_TIMEOUT=$(((3+${NB_DIRS})*(40+${NB_FILES}/5)))
while test ${TO_BE_ARCHIVED} != ${ARCHIVED}; do
  start_check=$(date +%s)
  echo "$(date +%s): Waiting for files to be archived to tape: Seconds passed = ${SECONDS_PASSED}"
  sleep 1
  let SECONDS_PASSED=SECONDS_PASSED+1

  if test ${SECONDS_PASSED} == ${WAIT_FOR_ARCHIVED_FILE_TIMEOUT}; then
    echo "$(date +%s): Timed out after ${WAIT_FOR_ARCHIVED_FILE_TIMEOUT} seconds waiting for file to be archived to tape"
    break
  fi

  ARCHIVED=0
  for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
    ARCHIVED=$(( ${ARCHIVED} + $(eos root://${EOS_INSTANCE} ls -y ${EOS_DIR}/${subdir} | grep '^d0::t1' | wc -l) ))
    sleep 1 # do not hammer eos too hard
    let SECONDS_PASSED=SECONDS_PASSED+1
  done
  end_check=$(date +%s)
  duration=$((end_check - start_check))
  echo "${ARCHIVED}/${TO_BE_ARCHIVED} archived checked within ${duration} seconds, current timestamp ${end_check}"

  NB_TAPE_NOT_FULL=$(admin_cta --json ta ls --all | jq "[.[] | select(.full == false)] | length")
  if [[ ${NB_TAPE_NOT_FULL} == 0 ]]
  then
    echo "$(date +%s): All tapes are full, exiting archiving loop"
    break
  fi
done

echo "###"
echo "${ARCHIVED}/${TO_BE_ARCHIVED} archived"
echo "###"
if [[ ${ARCHIVED} -ne ${TO_BE_ARCHIVED} ]]; then
  echo "ERROR: Some files were lost during archival."
  exit 1
fi

echo "Archiving done."
