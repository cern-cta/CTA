#!/bin/bash

# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2021 CERN
# @license        This program is free software: you can redistribute it and/or modify
#                 it under the terms of the GNU General Public License as published by
#                 the Free Software Foundation, either version 3 of the License, or
#                 (at your option) any later version.
#
#                 This program is distributed in the hope that it will be useful,
#                 but WITHOUT ANY WARRANTY; without even the implied warranty of
#                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#                 GNU General Public License for more details.
#
#                 You should have received a copy of the GNU General Public License
#                 along with this program.  If not, see <http://www.gnu.org/licenses/>.

################################################################################
# DESCRIPTION
#
#   - This script tests the new behaviour of the PREPARE request, which treats
#   all files independently and idempotendly.
#   - If a file fails to prepare - for any reason - it should not
#   affect the PREPARE of the remaining files in the list.
#
# EXPECTED BEHAVIOUR
#
#   # PREPARE -s command
#
#   - Both these commands should treat <file_1> the same way, regardless of the
#   other files being staged:
#       > prepare -s <file_1> .. <file_N>
#       > prepare -s <file_1>
#   - <file_1> is no longer affected if another file <file_M> fails for any reason.
#   - [Edge case:] We return an error if ALL files fail to prepare.
#
#   # QUERY PREPARE
#
#   - If a file failed to stage, query prepare must be able to communicate back
#   that it failed and the reason.
#   - This error is signaled and communitated through the field "error_text".
#
#   # PREPARE -e/-a commands
#
#   - We should trigger prepare evict or abort for all files, even if some fail.
#   - If any file failed, the prepare -e/prepare -a should return an error
#   (different behaviour from 'prepare -s'). This is necessary because, for
#   these commands, this is the only way to directly know that they failed.
#
################################################################################

EOS_INSTANCE=ctaeos

NB_FILES_TAPE=4
NB_FILES_NO_P=4
NB_FILES_MISS=4

EOS_BASEDIR=/eos/ctaeos
EOS_TAPE_BASEDIR=$EOS_BASEDIR/cta               # Exists on tape: for testing files on tape
EOS_NO_P_BASEDIR=$EOS_TAPE_BASEDIR/no_prepare  # Exists on tape but without prepare permissions
EOS_NONE_BASEDIR=$EOS_BASEDIR/none              # Does not exist: for testing non-existing files

# get some common useful helpers for krb5
. /root/client_helper.sh

eospower_kdestroy &>/dev/null
eospower_kinit &>/dev/null

admin_kdestroy &>/dev/null
admin_kinit &>/dev/null

################################################################
# Helper functions
################################################################

# Pass list of files waiting for archival

wait_for_archive () {

  SECONDS_PASSED=0
  WAIT_FOR_ARCHIVED_FILE_TIMEOUT=90

  while test $# != $(echo "$@" | tr " " "\n" | xargs -iFILE eos root://${EOS_INSTANCE} info FILE | awk '{print $4;}' | grep tape | wc -l); do
    echo "Waiting for files to be archived to tape: seconds passed = ${SECONDS_PASSED}"
    sleep 1
    let SECONDS_PASSED=SECONDS_PASSED+1

    if test ${SECONDS_PASSED} == ${WAIT_FOR_ARCHIVED_FILE_TIMEOUT}; then
      echo "ERROR: Timed out after ${WAIT_FOR_ARCHIVED_FILE_TIMEOUT} seconds waiting for files to be archived to tape"
      exit 1
    fi
  done

}

# Pass list of files waiting for retrieval

wait_for_retrieve () {

  SECONDS_PASSED=0
  WAIT_FOR_RETRIEVED_FILE_TIMEOUT=90
  while test $# != $(echo "$@" | tr " " "\n" | xargs -iFILE eos root://${EOS_INSTANCE} info FILE | awk '{print $4;}' | grep -F "default.0" | wc -l); do
    echo "Waiting for files to be retrieved from tape: Seconds passed = ${SECONDS_PASSED}"
    sleep 1
    let SECONDS_PASSED=SECONDS_PASSED+1

    if test ${SECONDS_PASSED} == ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT}; then
      echo "Timed out after ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT} seconds waiting for files to be retrieved from tape"
      exit 1
    fi
  done

}

# Pass list of files waiting for eviction

wait_for_evict () {

  SECONDS_PASSED=0
  WAIT_FOR_EVICTED_FILE_TIMEOUT=90
  while test 0 != $(echo "$@" | tr " " "\n" | xargs -iFILE eos root://${EOS_INSTANCE} info FILE | awk '{print $4;}' | grep -F "default.0" | wc -l); do
    echo "Waiting for files to be evicted from disk: Seconds passed = ${SECONDS_PASSED}"
    sleep 1
    let SECONDS_PASSED=SECONDS_PASSED+1

    if test ${SECONDS_PASSED} == ${WAIT_FOR_EVICTED_FILE_TIMEOUT}; then
      echo "Timed out after ${WAIT_FOR_EVICTED_FILE_TIMEOUT} seconds waiting for files to be evicted from disk"
      exit 1
    fi
  done

}

# Pass "UP" or "DOWN" as argument

put_all_drives () {

  NEXT_STATE=$1
  [ "$1" = "UP" ] && PREV_STATE="DOWN" || PREV_STATE="UP"
  next_state=$(echo $NEXT_STATE | awk '{print tolower($0)}')
  prev_state=$(echo $PREV_STATE | awk '{print tolower($0)}')

  # Put all tape drives up/down
  INITIAL_DRIVES_STATE=`admin_cta --json dr ls`
  echo INITIAL_DRIVES_STATE:
  echo ${INITIAL_DRIVES_STATE} | jq -r '.[] | [ .driveName, .driveStatus] | @tsv' | column -t
  echo -n "Will put $next_state those drives : "
  drivesToModify=`echo ${INITIAL_DRIVES_STATE} | jq -r ".[] | select (.driveStatus == \"${PREV_STATE}\") | .driveName"`
  echo $drivesToModify
  for d in `echo $drivesToModify`; do
    admin_cta drive $next_state $d --reason "PUTTING DRIVE $NEXT_STATE FOR TESTS"
  done

  echo "$(date +%s): Waiting for the drives to be $next_state"
  SECONDS_PASSED=0
  WAIT_FOR_DRIVES_TIMEOUT=$((10))
  while [[ $SECONDS_PASSED < $WAIT_FOR_DRIVES_TIMEOUT ]]; do
    sleep 1
    oneStatusRemaining=0
    for d in `echo $drivesToModify`; do
      status=`admin_cta --json drive ls | jq -r ". [] | select(.driveName == \"$d\") | .driveStatus"`
      if [[ $status != $NEXT_STATE ]]; then
        oneStatusRemaining=1
      fi;
    done
    if [[ $oneStatusRemaining -eq 0 ]]; then
      echo "Drives : $drivesToModify are $next_state"
      break;
    fi
    echo -n "."
    SECONDS_PASSED=$SECONDS_PASSED+1
    if [[ $SECONDS_PASSED -gt $WAIT_FOR_DRIVES_TIMEOUT ]]; then
      die "ERROR: Timeout reach for trying to put all drives $next_state"
    fi
  done

}

put_all_drives_up () {
  put_all_drives "UP"
}

put_all_drives_down () {
  put_all_drives "DOWN"
}


################################################################
# Test preparing single file (exists on tape)
################################################################

# File is valid and exists on tape
#   - Prepare command should succeed

TEMP_FILE_OK=${EOS_TAPE_BASEDIR}/$(uuidgen)
echo
echo "Testing normal 'prepare -s' request..."

put_all_drives_up
echo "Archiving ${TEMP_FILE_OK}..."
xrdcp /etc/group root://${EOS_INSTANCE}/${TEMP_FILE_OK}
wait_for_archive ${TEMP_FILE_OK}
put_all_drives_down

echo "Trigering EOS retrieve workflow as poweruser1:powerusers..."
# We need the -s as we are staging the files from tape (see xrootd prepare definition)
REQUEST_ID=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_INSTANCE} prepare -s ${TEMP_FILE_OK})

echo "Checking 'query prepare' for retrieved file..."
QUERY_RSP=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_INSTANCE} query prepare ${REQUEST_ID} ${TEMP_FILE_OK})

PATH_EXISTS=$(echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE_OK}\").path_exists")
REQUESTED=$(  echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE_OK}\").requested")
HAS_REQID=$(  echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE_OK}\").has_reqid")
ERROR_TEXT=$( echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE_OK}\").error_text")

if [[
  "true" != "${PATH_EXISTS}" ||
  "true" != "${REQUESTED}" ||
  "true" != "${HAS_REQID}" ||
  "\"\"" != "${ERROR_TEXT}" ]]
then
  echo "ERROR: File ${TEMP_FILE_OK} not requested properly: ${QUERY_RSP}"
  exit 1
fi
echo "Test completed successfully"


################################################################
# Test preparing single file - file does not exist - should fail
################################################################

# File does not exist on tape or disk
# All files (just 1 in this case) fail to prepare
#   - Prepare command should fail and return an error

TEMP_FILE_FAIL=${EOS_NONE_BASEDIR}/$(uuidgen)
echo
echo "Testing 'prepare -s' request that returns error (1 non-existing file)..."

echo "NOT uploading test filei ${TEMP_FILE_FAIL}."

echo "Trigering EOS retrieve workflow as poweruser1:powerusers (expects error)..."
# We need the -s as we are staging the files from tape (see xrootd prepare definition)
REQUEST_ID=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_INSTANCE} prepare -s ${TEMP_FILE_FAIL})

if [ $? -eq 0 ]; then
  echo "ERROR: Preparing a single file that does not exist (all files failec) should return an error."
  exit 1
fi

echo "Test completed successfully"


################################################################
# Test when user has no prepare permissions - 1/2 files fail
################################################################

# One file does not have proper permissions to prepare, the other exists on tape
#   - Only 1 file should fail to prepare (not all)
#   - Prepare command should succeed and return a request ID
#   - Query prepare command should indicate which file failed to prepare

TEMP_FILE=${EOS_NO_P_BASEDIR}/$(uuidgen)
echo
echo "Testing 'prepare -s' request for 1 file with prepare permissions (reusing ${TEMP_FILE_OK}) and 1 files without prepare permissions (${TEMP_FILE})..."

xrdcp /etc/group root://${EOS_INSTANCE}/${TEMP_FILE}
echo "File ${TEMP_FILE} written to directory with no prepare permission."

echo "Trigering EOS retrieve workflow as poweruser1:powerusers..."
# We need the -s as we are staging the files from tape (see xrootd prepare definition)
REQUEST_ID=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_INSTANCE} prepare -s ${TEMP_FILE_OK} ${TEMP_FILE})

if [ $? -ne 0 ]; then
  echo "ERROR: Unexpected error returned by prepare command."
  exit 1
fi

# We can check 'query prepare' immediatelly because file should have been refused with error
# 'error_text' of failed file should contain an error message

echo "Checking 'query prepare' for failed to prepare file ${TEMP_FILE}..."
QUERY_RSP=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_INSTANCE} query prepare ${REQUEST_ID} ${TEMP_FILE_OK} ${TEMP_FILE})

PATH_EXISTS=$(echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE}\").path_exists")
REQUESTED=$(  echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE}\").requested")
HAS_REQID=$(  echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE}\").has_reqid")
ERROR_TEXT=$( echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE}\").error_text")

OK_FILE_ERROR_TEXT=$(echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE_OK}\").error_text")

if [[
  "true" != "${PATH_EXISTS}" ||
  "false" != "${REQUESTED}" ||
  "false" != "${HAS_REQID}" ||
  "\"\"" == "${ERROR_TEXT}" ||
  "\"\"" != "${OK_FILE_ERROR_TEXT}" ]]
then
  echo "ERROR: 'query prepare' did not return as expected: ${QUERY_RSP}"
  exit 1
fi

# Cleanup
eos root://${EOS_INSTANCE} rm ${TEMP_FILE}

echo "Test completed successfully"


################################################################
# Test when user has no prepare permissions - 2/2 files fail
################################################################

# Both files do not have proper permissions to prepare
#   - All files should fail to prepare
#   - Because all files failed, prepare command should return an error

TEMP_FILE_1=${EOS_NO_P_BASEDIR}/$(uuidgen)
TEMP_FILE_2=${EOS_NO_P_BASEDIR}/$(uuidgen)
echo
echo "Testing 'prepare -s' request for 2 file without prepare permissions (${TEMP_FILE_1}, ${TEMP_FILE_2})..."

xrdcp /etc/group root://${EOS_INSTANCE}/${TEMP_FILE_1}
xrdcp /etc/group root://${EOS_INSTANCE}/${TEMP_FILE_2}
echo "Files ${TEMP_FILE_1} ${TEMP_FILE_2} written to directory with no prepare permission."

echo "Trigering EOS retrieve workflow as poweruser1:powerusers (expects error)..."
# We need the -s as we are staging the files from tape (see xrootd prepare definition)
REQUEST_ID=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_INSTANCE} prepare -s ${TEMP_FILE_1} ${TEMP_FILE_2})

if [ $? -eq 0 ]; then
  echo "ERROR: Preparing command where no single file has prepare permissions (all files failed) should return an error."
  exit 1
fi

# Cleanup
eos root://${EOS_INSTANCE} rm ${TEMP_FILE_1}
eos root://${EOS_INSTANCE} rm ${TEMP_FILE_2}

echo "Test completed successfully"


################################################################
# Test when file does not exist - 1/2 files fail
################################################################

# One file does not exist, the other exists  on tape
#   - Only 1 file should fail to prepare (not all)
#   - Prepare command should succeed and return a request ID
#   - Query prepare command should indicate which file failed to prepare

TEMP_FILE=${EOS_NONE_BASEDIR}/$(uuidgen)
echo
echo "Testing 'prepare -s' request for 1 existing file (reusing ${TEMP_FILE_OK}) and 1 non-existing file (${TEMP_FILE})..."

echo "NOT uploading test file ${TEMP_FILE}."

echo "Trigering EOS retrieve workflow as poweruser1:powerusers..."
# We need the -s as we are staging the files from tape (see xrootd prepare definition)
REQUEST_ID=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_INSTANCE} prepare -s ${TEMP_FILE_OK} ${TEMP_FILE})

if [ $? -ne 0 ]; then
  echo "ERROR: Unexpected error returned by prepare command."
  exit 1
fi

# We can check 'query prepare' immediatelly because file should have been refused with error
# 'error_text' of failed file should contain an error message

echo "Checking 'query prepare' for failed to prepare file ${TEMP_FILE}..."
QUERY_RSP=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_INSTANCE} query prepare ${REQUEST_ID} ${TEMP_FILE_OK} ${TEMP_FILE})

PATH_EXISTS=$(echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE}\").path_exists")
REQUESTED=$(  echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE}\").requested")
HAS_REQID=$(  echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE}\").has_reqid")
ERROR_TEXT=$( echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE}\").error_text")

OK_FILE_ERROR_TEXT=$(echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE_OK}\").error_text")

if [[
  "false" != "${PATH_EXISTS}" ||
  "false" != "${REQUESTED}" ||
  "false" != "${HAS_REQID}" ||
  "\"\"" == "${ERROR_TEXT}" ||
  "\"\"" != "${OK_FILE_ERROR_TEXT}" ]]
then
  echo "ERROR: 'query prepare' did not return as expected: ${QUERY_RSP}"
  exit 1
fi
echo "Test completed successfully"


################################################################
# Test when file does not exist - 2/2 files fail
################################################################

# Both files do not exist
#   - All files should fail to prepare
#   - Because all files failed, prepare command should return an error

TEMP_FILE_1=${EOS_NO_P_BASEDIR}/$(uuidgen)
TEMP_FILE_2=${EOS_NO_P_BASEDIR}/$(uuidgen)
echo
echo "Testing 'prepare -s' request for 2 non-existing files (${TEMP_FILE_1}, ${TEMP_FILE_2})..."

echo "NOT uploading test files ${TEMP_FILE_1} ${TEMP_FILE_2}."

echo "Trigering EOS retrieve workflow as poweruser1:powerusers (expects error)..."
# We need the -s as we are staging the files from tape (see xrootd prepare definition)
REQUEST_ID=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_INSTANCE} prepare -s ${TEMP_FILE_1} ${TEMP_FILE_2})

if [ $? -eq 0 ]; then
  echo "ERROR: Preparing command where no single file exists (all files failed) should return an error."
  exit 1
fi

echo "Test completed successfully"


################################################################
# List of test files for prepare
################################################################

# Test prepare command with multiple files
# Some do exist, others do not exist, others have not the needed permissions
#   - Prepare command should succeed and return a request ID
#   - Query prepare command should indicate which files failed to prepare

TEST_FILES_TAPE_LIST=$(mktemp)
TEST_FILES_NO_P_LIST=$(mktemp)
TEST_FILES_NONE_LIST=$(mktemp)

echo
echo "Testing 'prepare -s' for multiple files..."

echo "Files to be written to tape:"
for ((file_idx=0; file_idx < ${NB_FILES_TAPE}; file_idx++)); do
  echo "${EOS_TAPE_BASEDIR}/$(uuidgen)" | tee -a ${TEST_FILES_TAPE_LIST}
done

put_all_drives_up
cat ${TEST_FILES_TAPE_LIST} | xargs -iFILE_PATH xrdcp /etc/group root://${EOS_INSTANCE}/FILE_PATH
wait_for_archive $(cat ${TEST_FILES_TAPE_LIST} | tr "\n" " ")

echo "Files to be written to directory with no prepare/evict permission:"
for ((file_idx=0; file_idx < ${NB_FILES_NO_P}; file_idx++)); do
  echo "${EOS_NO_P_BASEDIR}/$(uuidgen)" | tee -a ${TEST_FILES_NO_P_LIST}
done
cat ${TEST_FILES_NO_P_LIST} | xargs -iFILE_PATH xrdcp /etc/group root://${EOS_INSTANCE}/FILE_PATH

echo "Files without copy on disk/tape (missing files):"
for ((file_idx=0; file_idx < ${NB_FILES_MISS}; file_idx++)); do
  echo "${EOS_NONE_BASEDIR}/$(uuidgen)" | tee -a ${TEST_FILES_NONE_LIST}
done

echo "Trigering EOS retrieve workflow as poweruser1:powerusers..."
# We need the -s as we are staging the files from tape (see xrootd prepare definition)
REQUEST_ID=$(cat ${TEST_FILES_TAPE_LIST} ${TEST_FILES_NO_P_LIST} ${TEST_FILES_NONE_LIST} | KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xargs xrdfs ${EOS_INSTANCE} prepare -s)

if [ $? -ne 0 ]; then
  echo "ERROR: Unexpected error returned by prepare command."
  exit 1
fi

echo "Checking 'query prepare' for request status..."
QUERY_RSP=$(cat ${TEST_FILES_TAPE_LIST} ${TEST_FILES_NO_P_LIST} ${TEST_FILES_NONE_LIST} | KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xargs xrdfs ${EOS_INSTANCE} query prepare ${REQUEST_ID})

# Check that a request ID was produced

if ! [[ "${REQUEST_ID}" =~ ^[^\s]+$ ]]; then
  echo "ERROR: Prepare did not return an ID"
  echo $REQUEST_ID
  exit 1
fi

# Check that all files are referred in query prepare response

NB_FILES_IN_QUERY_RSP=$(echo ${QUERY_RSP} | jq ".responses | length")
NB_EXPECTED_FILES=$((${NB_FILES_TAPE}+${NB_FILES_NO_P}+${NB_FILES_MISS}))
if test ${NB_EXPECTED_FILES} != ${NB_FILES_IN_QUERY_RSP}; then
  echo "ERROR: Query prepare does not refer all ${NV_EXPECTED_FILES} files"
  echo $QUERY_RSP
  exit 1
fi

# Validate query prepare response contents

tmpjsonfile=$(mktemp)
echo ${QUERY_RSP} > ${tmpjsonfile}

# Files on tape

if [[
  ${NB_FILES_TAPE} != $(cat ${TEST_FILES_TAPE_LIST} | xargs -iFILE_PATH jq '.responses[] | select(.path == "FILE_PATH").path_exists' ${tmpjsonfile} | grep ^true$ | wc -l) ||
  ${NB_FILES_TAPE} != $(cat ${TEST_FILES_TAPE_LIST} | xargs -iFILE_PATH jq '.responses[] | select(.path == "FILE_PATH").error_text' ${tmpjsonfile} | grep ^\"\"$ | wc -l) ]]
then
  echo "ERROR: Files from tape were not requested properly."
  echo $QUERY_RSP
  rm ${tmpjsonfile}
  exit 1
fi

# Files without prepare permission

if [[
  ${NB_FILES_NO_P} != $(cat ${TEST_FILES_NO_P_LIST} | xargs -iFILE_PATH jq '.responses[] | select(.path == "FILE_PATH").path_exists' ${tmpjsonfile} | grep ^true$ | wc -l) ||
  ${NB_FILES_NO_P} != $(cat ${TEST_FILES_NO_P_LIST} | xargs -iFILE_PATH jq '.responses[] | select(.path == "FILE_PATH").error_text' ${tmpjsonfile} | grep -E ^\".+\"$ | wc -l) ]] # 'error_text should not be empty'
then
  echo "ERROR: File without prepare permission not reported properly (error_text)."
  echo $QUERY_RSP
  rm ${tmpjsonfile}
  exit 1
fi

# Non-existing files

if [[
  ${NB_FILES_MISS} != $(cat ${TEST_FILES_NONE_LIST} | xargs -iFILE_PATH jq '.responses[] | select(.path == "FILE_PATH").path_exists' ${tmpjsonfile} | grep ^false$ | wc -l) ||
  ${NB_FILES_MISS} != $(cat ${TEST_FILES_NONE_LIST} | xargs -iFILE_PATH jq '.responses[] | select(.path == "FILE_PATH").error_text' ${tmpjsonfile} | grep -E ^\".+\"$ | wc -l) ]] # 'error_text should not be empty'
then
  echo "ERROR: Non-existing file not reported properly (error_text)."
  echo $QUERY_RSP
  rm ${tmpjsonfile}
  exit 1
fi

# Cleanup

echo "Cleaning up test files..."
cat ${TEST_FILES_TAPE_LIST} | xargs -iFILE_PATH eos root://${EOS_INSTANCE} rm FILE_PATH
cat ${TEST_FILES_NO_P_LIST} | xargs -iFILE_PATH eos root://${EOS_INSTANCE} rm FILE_PATH

echo "Test completed successfully"
rm ${tmpjsonfile}


################################################################
# Testing prepare abort - Success
################################################################

# 'prepare -a' should return no error if all files succeed

TEMP_FILE=${EOS_TAPE_BASEDIR}/$(uuidgen)
echo
echo "Testing 'prepare -a' request for file ${TEMP_FILE}..."

put_all_drives_up
echo "Archiving ${TEMP_FILE}..."
xrdcp /etc/group root://${EOS_INSTANCE}/${TEMP_FILE}
wait_for_archive ${TEMP_FILE}
echo "Disabling tape drives..."
put_all_drives_down

echo "Trigering EOS retrieve workflow as poweruser1:powerusers..."
# We need the -s as we are staging the files from tape (see xrootd prepare definition)
REQUEST_ID=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_INSTANCE} prepare -s ${TEMP_FILE})

echo "Trigering EOS abort workflow as poweruser1:powerusers..."
KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_INSTANCE} prepare -a ${REQUEST_ID} ${TEMP_FILE}

if [ $? -ne 0 ]; then
  echo "ERROR: Prepare abort command should not have failed."
  exit 1
fi

echo "Checking 'query prepare' for aborted file..."
QUERY_RSP=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_INSTANCE} query prepare ${REQUEST_ID} ${TEMP_FILE})

PATH_EXISTS=$(echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE}\").path_exists")
REQUESTED=$(  echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE}\").requested")
HAS_REQID=$(  echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE}\").has_reqid")
ERROR_TEXT=$( echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE}\").error_text")

if [[
  "true" != "${PATH_EXISTS}" ||
  "false" != "${REQUESTED}" ||
  "false" != "${HAS_REQID}" ||
  "\"\"" != "${ERROR_TEXT}" ]]
then
  echo "ERROR: File ${TEMP_FILE} retrieve was not aborted properly: ${QUERY_RSP}"
  exit 1
fi

echo "Test completed successfully"


################################################################
# Testing prepare abort - 1/2 files fail
################################################################

# 'prepare -a' should return an error if any of the files failed,
#   but it should still abort all valid files

TEMP_FILE_TAPE=${EOS_TAPE_BASEDIR}/$(uuidgen)
TEMP_FILE_NONE=${EOS_NONE_BASEDIR}/$(uuidgen)
echo
echo "Testing 'prepare -a' request for existing file ${TEMP_FILE_TAPE} and non-existing ${TEMP_FILE_NONE}..."
echo "NOT uploading test file ${TEMP_FILE_NONE}."
echo "Uploading & archiving test file ${TEMP_FILE_TAPE}."

put_all_drives_up
echo "Archiving ${TEMP_FILE_TAPE}..."
xrdcp /etc/group root://${EOS_INSTANCE}/${TEMP_FILE_TAPE}
wait_for_archive ${TEMP_FILE_TAPE}
echo "Disabling tape drives..."
put_all_drives_down

echo "Trigering EOS retrieve workflow as poweruser1:powerusers, for ${TEMP_FILE_TAPE}..."
# We need the -s as we are staging the files from tape (see xrootd prepare definition)
REQUEST_ID=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_INSTANCE} prepare -s ${TEMP_FILE_TAPE})

echo "Trigering EOS abort workflow as poweruser1:powerusers, for ${TEMP_FILE_TAPE} (should succeed) and ${TEMP_FILE_NONE} (should fail)..."
echo "Error expected"
KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_INSTANCE} prepare -a ${REQUEST_ID} ${TEMP_FILE_TAPE} ${TEMP_FILE_NONE}

if [ $? -eq 0 ]; then
  echo "ERROR: Prepare abort command should have returned an error."
  exit 1
fi

echo "Checking 'query prepare' for aborted file..."
QUERY_RSP=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_INSTANCE} query prepare ${REQUEST_ID} ${TEMP_FILE_TAPE})

PATH_EXISTS=$(echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE_TAPE}\").path_exists")
REQUESTED=$(  echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE_TAPE}\").requested")
HAS_REQID=$(  echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE_TAPE}\").has_reqid")
ERROR_TEXT=$( echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${TEMP_FILE_TAPE}\").error_text")

if [[
  "true" != "${PATH_EXISTS}" ||
  "false" != "${REQUESTED}" ||
  "false" != "${HAS_REQID}" ||
  "\"\"" != "${ERROR_TEXT}" ]]
then
  echo "ERROR: File ${TEMP_FILE_TAPE} retrieve was not aborted properly: ${QUERY_RSP}"
  exit 1
fi

echo "Test completed successfully"


################################################################
# Testing prepare evict - Success
################################################################

# 'prepare -e' should return no error if all files succeed

TEMP_FILE=${EOS_TAPE_BASEDIR}/$(uuidgen)
echo
echo "Testing 'prepare -e' request for existing file ${TEMP_FILE}..."

put_all_drives_up
echo "Archiving ${TEMP_FILE}..."
xrdcp /etc/group root://${EOS_INSTANCE}/${TEMP_FILE}
echo "Disabling tape drives..."
wait_for_archive ${TEMP_FILE}

echo "Trigering EOS retrieve workflow as poweruser1:powerusers, for ${TEMP_FILE}..."
# We need the -s as we are staging the files from tape (see xrootd prepare definition)
REQUEST_ID=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_INSTANCE} prepare -s ${TEMP_FILE})
wait_for_retrieve ${TEMP_FILE}

echo "Trigering EOS evict workflow as poweruser1:powerusers..."
KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_INSTANCE} prepare -e ${TEMP_FILE}

if [ $? -ne 0 ]; then
  echo "ERROR: Prepare evict command should not have failed."
  exit 1
fi

wait_for_evict ${TEMP_FILE}

echo "Test completed successfully"


################################################################
# Testing prepare evict - 1/2 files fail
################################################################

# 'prepare -e' should return an error if any of the files failed
#   but it should still evict all valid files

TEMP_FILE_TAPE=${EOS_TAPE_BASEDIR}/$(uuidgen)
TEMP_FILE_NONE=${EOS_NONE_BASEDIR}/$(uuidgen)
echo
echo "Testing 'prepare -e' request for existing file ${TEMP_FILE_TAPE} and non-existing ${TEMP_FILE_NONE}..."
echo "NOT uploading test file ${TEMP_FILE_NONE}."
echo "Uploading & archiving test file ${TEMP_FILE_TAPE}."

put_all_drives_up
echo "Archiving ${TEMP_FILE_TAPE}..."
xrdcp /etc/group root://${EOS_INSTANCE}/${TEMP_FILE_TAPE}
wait_for_archive ${TEMP_FILE_TAPE}

echo "Trigering EOS retrieve workflow as poweruser1:powerusers, for ${TEMP_FILE_TAPE}..."
# We need the -s as we are staging the files from tape (see xrootd prepare definition)
REQUEST_ID=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_INSTANCE} prepare -s ${TEMP_FILE_TAPE})
wait_for_retrieve ${TEMP_FILE_TAPE}

echo "Trigering EOS abort workflow as poweruser1:powerusers..."
echo "Error expected"
KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_INSTANCE} prepare -e ${TEMP_FILE_TAPE} ${TEMP_FILE_NONE}

if [ $? -eq 0 ]; then
  echo "ERROR: Prepare evict command should have returned an error."
  exit 1
fi

wait_for_evict ${TEMP_FILE_TAPE}

echo "Test completed successfully"


################################################################
# Finalize
################################################################

echo
echo "OK: all tests passed"

