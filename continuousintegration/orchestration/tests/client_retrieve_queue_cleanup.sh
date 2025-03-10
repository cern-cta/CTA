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

EOS_MGM_HOST="ctaeos"

MULTICOPY_DIR_1=/eos/ctaeos/preprod/dir_1_copy
MULTICOPY_DIR_2=/eos/ctaeos/preprod/dir_2_copy
MULTICOPY_DIR_3=/eos/ctaeos/preprod/dir_3_copy

# get some common useful helpers for krb5
. /root/client_helper.sh

eospower_kdestroy &>/dev/null
eospower_kinit &>/dev/null

admin_kdestroy &>/dev/null
admin_kinit &>/dev/null

# Find tapes and tape pools

STORAGECLASS_1=$( eos root://${EOS_MGM_HOST} attr get sys.archive.storage_class ${MULTICOPY_DIR_1} | sed -n -e 's/.*="\(.*\)"/\1/p' )
STORAGECLASS_2=$( eos root://${EOS_MGM_HOST} attr get sys.archive.storage_class ${MULTICOPY_DIR_2} | sed -n -e 's/.*="\(.*\)"/\1/p' )
STORAGECLASS_3=$( eos root://${EOS_MGM_HOST} attr get sys.archive.storage_class ${MULTICOPY_DIR_3} | sed -n -e 's/.*="\(.*\)"/\1/p' )

mapfile -t TAPEPOOL_LIST_1 < <( admin_cta --json archiveroute ls | jq -r --arg STORAGECLASS "$STORAGECLASS_1" '.[] | select( .storageClass == $STORAGECLASS) | .tapepool' )
mapfile -t TAPEPOOL_LIST_2 < <( admin_cta --json archiveroute ls | jq -r --arg STORAGECLASS "$STORAGECLASS_2" '.[] | select( .storageClass == $STORAGECLASS) | .tapepool' )
mapfile -t TAPEPOOL_LIST_3 < <( admin_cta --json archiveroute ls | jq -r --arg STORAGECLASS "$STORAGECLASS_3" '.[] | select( .storageClass == $STORAGECLASS) | .tapepool' )

mapfile -t TAPE_LIST_1 < <( for t in "${TAPEPOOL_LIST_1[@]}" ; do admin_cta --json tape ls --all | jq -r --arg TAPEPOOL "$t" '.[] | select( .tapepool == $TAPEPOOL) | .vid' ; done )
mapfile -t TAPE_LIST_2 < <( for t in "${TAPEPOOL_LIST_2[@]}" ; do admin_cta --json tape ls --all | jq -r --arg TAPEPOOL "$t" '.[] | select( .tapepool == $TAPEPOOL) | .vid' ; done )
mapfile -t TAPE_LIST_3 < <( for t in "${TAPEPOOL_LIST_3[@]}" ; do admin_cta --json tape ls --all | jq -r --arg TAPEPOOL "$t" '.[] | select( .tapepool == $TAPEPOOL) | .vid' ; done )

if [ "${#TAPEPOOL_LIST_1[@]}" -ne "1" ] || [ "${#TAPE_LIST_1[@]}" -ne "1" ]; then
  echo "ERROR: Tape pool 1 misconfigured"
  exit 1
fi
if [ "${#TAPEPOOL_LIST_2[@]}" -ne "2" ] || [ "${#TAPE_LIST_2[@]}" -ne "2" ]; then
  echo "ERROR: Tape pool 2 misconfigured"
  exit 1
fi
if [ "${#TAPEPOOL_LIST_3[@]}" -ne "3" ] || [ "${#TAPE_LIST_3[@]}" -ne "3" ]; then
  echo "ERROR: Tape pool 3 misconfigured"
  exit 1
fi

# Save file with 1, 2, 3 replicas

FILE_1_COPY=${MULTICOPY_DIR_1}/$(uuidgen)
FILE_2_COPY=${MULTICOPY_DIR_2}/$(uuidgen)
FILE_3_COPY=${MULTICOPY_DIR_3}/$(uuidgen)

put_all_drives_up
xrdcp /etc/group root://${EOS_MGM_HOST}/${FILE_1_COPY}
xrdcp /etc/group root://${EOS_MGM_HOST}/${FILE_2_COPY}
xrdcp /etc/group root://${EOS_MGM_HOST}/${FILE_3_COPY}

wait_for_archive ${EOS_MGM_HOST} ${FILE_1_COPY} ${FILE_2_COPY} ${FILE_3_COPY}
put_all_drives_down

trigger_queue_cleanup() {
  # Get a list of all tapes being used, without duplicates
  repeatedTapeList=( "${TAPE_LIST_1[@]}" "${TAPE_LIST_2[@]}" "${TAPE_LIST_3[@]}" )
  tapeList=(); while IFS= read -r -d '' tape; do tapeList+=("$tape"); done < <(printf "%s\0" "${repeatedTapeList[@]}" | sort -uz)
  for i in ${!tapeList[@]}; do
    admin_cta tape ch --vid ${tapeList[$i]} --state BROKEN --reason "Trigger cleanup"
  done
  for i in ${!tapeList[@]}; do
    wait_for_tape_state ${tapeList[$i]} BROKEN
  done
  sleep 1 # Wait for a bit, to take in account caching latencies
  for i in ${!tapeList[@]}; do
    admin_cta tape ch --vid ${tapeList[$i]} --state ACTIVE
  done
  for i in ${!tapeList[@]}; do
    wait_for_tape_state ${tapeList[$i]} ACTIVE
  done
  sleep 1 # Wait for a bit, to take in account caching latencies
}

wait_for_request_cancel_report() {

  SECONDS_PASSED=0
  WAIT_TIMEOUT=90
  REQUEST_ID=$1
  FILE_PATH=$2

  echo "Waiting for request to be reported as canceled..."
  while true; do
    QUERY_RSP=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_MGM_HOST} query prepare ${REQUEST_ID} ${FILE_PATH})
    REQUESTED=$(  echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${FILE_PATH}\").requested")

    # Check if request has finally been canceled
    if [[ "false" == "${REQUESTED}" ]]; then
      break
    fi

    if test ${SECONDS_PASSED} == ${WAIT_TIMEOUT}; then
      echo "Timed out after ${WAIT_TIMEOUT} seconds"
      break
    fi

    echo "Waiting for request to be reported as canceled: Seconds passed = ${SECONDS_PASSED}"
    sleep 1
    let SECONDS_PASSED=SECONDS_PASSED+1

  done
}

change_tape_state() {
  VID=$1
  LAST_STATE=$2
  CURR_STATE=$(admin_cta --json tape ls --vid $VID | jq -r ".[] | .state")

  # If current or desired state is REPACKING_DISABLED, revert to REPACKING
  # Any state transition can be done from REPACKING, but not from REPACKING_DISABLED
  if [[
    "${CURR_STATE}" == "REPACKING_DISABLED" ||
    "${LAST_STATE}" == "REPACKING_DISABLED" ]]
  then
    admin_cta tape ch --vid $VID --state REPACKING --reason "Testing"
    wait_for_tape_state $VID REPACKING
    sleep 1 # Wait for a bit, to take in account caching latencies
  fi
  admin_cta tape ch --vid $VID --state $LAST_STATE --reason "Testing"
  wait_for_tape_state $VID $LAST_STATE
  sleep 1 # Wait for a bit, to take in account caching latencies
}

# The cta-admin commands are async, so they might return before the command has fully passed
# These functions will retry (up to a limit) to ensure we don't end up with a race condition
# Between retries there is a 1 second delay
# usage: assert_number_of_files_in_queue <vid> <expected_files> <retries>
assert_number_of_files_in_queue() {
  local delay=1 # in seconds
  local count=0
  local tape_vid="$1"
  local expected_file_count="$2"
  local retries=$3

  while true; do
    actual_file_count=$(admin_cta --json sq | jq -r --arg VID "$tape_vid" '.[] | select(.vid == $VID) | .queuedFiles')
    if [[ "$expected_file_count" -eq "$actual_file_count" ]]; then
      return 0
    fi

    count=$((count + 1))
    if [[ $count -ge $retries ]]; then
      echo "ERROR: Queue for for tape ${tape_vid} contains $actual_file_count files, while $expected_file_count were expected."
      return 1
    fi

    sleep "$delay"
  done
}


################################################################################
# Test queueing priority between different tape states
################################################################################

test_tape_state_queueing_priority() {

  TEST_NR=$1
  TAPE_STATE_LIST=("$2" "$3" "$4")
  EXPECTED_SELECTED_QUEUE=$5
  FILE_PATH=$FILE_3_COPY

  pid=()
  ret=()

  echo
  echo "########################################################################################################"
  echo " ${TEST_NR}. Testing 'Tape state priority between ${TAPE_STATE_LIST[@]}'"
  echo "########################################################################################################"
  echo "Setting up queue ${TAPE_LIST_3[0]} as ${TAPE_STATE_LIST[0]}, ${TAPE_LIST_3[1]} as ${TAPE_STATE_LIST[1]}, ${TAPE_LIST_3[2]} as ${TAPE_STATE_LIST[2]}..."

  change_tape_state ${TAPE_LIST_3[0]} ${TAPE_STATE_LIST[0]} & pid[0]=$!
  change_tape_state ${TAPE_LIST_3[1]} ${TAPE_STATE_LIST[1]} & pid[1]=$!
  change_tape_state ${TAPE_LIST_3[2]} ${TAPE_STATE_LIST[2]} & pid[2]=$!

  wait ${pid[0]}; ret[0]=$?
  wait ${pid[1]}; ret[1]=$?
  wait ${pid[2]}; ret[2]=$?

  if [ ${ret[0]} -ne 0 ] || [ ${ret[1]} -ne 0 ] || [ ${ret[2]} -ne 0 ]
  then
    echo "Failed to change tape state"
    exit 1
  fi

  echo "Requesting file prepare -s..."
  REQUEST_ID=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_MGM_HOST} prepare -s ${FILE_PATH})

  echo "Checking if request went to ${TAPE_STATE_LIST[$EXPECTED_SELECTED_QUEUE]} queue ${TAPE_LIST_3[$EXPECTED_SELECTED_QUEUE]}..."

  for i in ${!TAPE_LIST_3[@]}; do
    echo "Checking tape ${TAPE_LIST_3[$i]}..."
    if [ $i -eq $EXPECTED_SELECTED_QUEUE ]; then
      assert_number_of_files_in_queue "${TAPE_LIST_3[$i]}" 1 2
      echo "Request found on ${TAPE_STATE_LIST[$i]} queue ${TAPE_LIST_3[$i]}, as expected."
    else
      assert_number_of_files_in_queue "${TAPE_LIST_3[$i]}" 0 2
      echo "Request not found on ${TAPE_STATE_LIST[$i]} queue ${TAPE_LIST_3[$i]}, as expected."
    fi
  done

  echo "Cleaning up request and queues..."
  KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_MGM_HOST} prepare -a ${REQUEST_ID} ${FILE_PATH}
  trigger_queue_cleanup > /dev/null

  echo "OK"
}


################################################################################
# Test tape state change that removes queue : 1 copy only
################################################################################

test_tape_state_change_queue_removed() {

  TEST_NR=$1
  STATE_START=$2
  STATE_END=$3

  # Using $FILE_1_COPY, which has 1 replica in the following tape

  FILE_PATH=$FILE_1_COPY

  TAPE_0=${TAPE_LIST_1[0]}

  echo
  echo "########################################################################################################"
  echo " ${TEST_NR}. Testing 'Tape state change from $STATE_START to $STATE_END - queue removed (1 copy only)"
  echo "########################################################################################################"
  echo "Setting up $TAPE_0 queue as ${STATE_START}..."

  change_tape_state $TAPE_0 $STATE_START

  echo "Requesting file prepare -s..."
  REQUEST_ID=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_MGM_HOST} prepare -s ${FILE_PATH})

  echo "Checking that the request was queued..."

  QUERY_RSP=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_MGM_HOST} query prepare ${REQUEST_ID} ${FILE_PATH})
  PATH_EXISTS=$(echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${FILE_PATH}\").path_exists")
  REQUESTED=$(  echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${FILE_PATH}\").requested")
  HAS_REQID=$(  echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${FILE_PATH}\").has_reqid")
  ERROR_TEXT=$( echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${FILE_PATH}\").error_text")
  if [[
    "true" != "${PATH_EXISTS}" ||
    "true" != "${REQUESTED}"   ||
    "true" != "${HAS_REQID}"   ||
    "\"\"" != "${ERROR_TEXT}" ]]
  then
    echo "ERROR: Request for ${FILE_PATH} not configured as expected: ${QUERY_RSP}"
    exit 1
  fi

  assert_number_of_files_in_queue "$TAPE_0" 1 2

  echo "Changing $TAPE_0 queue to ${STATE_END}..."

  change_tape_state $TAPE_0 $STATE_END

  echo "Checking that the request was canceled and the error reported to the user..."

  wait_for_request_cancel_report ${REQUEST_ID} ${FILE_PATH}

  QUERY_RSP=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_MGM_HOST} query prepare ${REQUEST_ID} ${FILE_PATH})
  PATH_EXISTS=$(echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${FILE_PATH}\").path_exists")
  REQUESTED=$(  echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${FILE_PATH}\").requested")
  HAS_REQID=$(  echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${FILE_PATH}\").has_reqid")
  ERROR_TEXT=$( echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${FILE_PATH}\").error_text")
  if [[
    "true"  != "${PATH_EXISTS}" ||
    "false" != "${REQUESTED}"   ||
    "false" != "${HAS_REQID}"   ||
    "\"\""  == "${ERROR_TEXT}" ]]
  then
    echo "ERROR: Request for ${FILE_PATH} not removed as expected: ${QUERY_RSP}"
    exit 1
  fi

  assert_number_of_files_in_queue "$TAPE_0" 0 2

  echo "Request removed and error reported back to user, as expected."

  echo "Cleaning up request and queues..."
  KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_MGM_HOST} prepare -a ${REQUEST_ID} ${FILE_PATH}
  trigger_queue_cleanup > /dev/null

  echo "OK"
}


################################################################################
# Test tape state change that preserves queue : 1 copy only
################################################################################

test_tape_state_change_queue_preserved() {

  TEST_NR=$1
  STATE_START=$2
  STATE_END=$3

  # Using $FILE_1_COPY, which has 1 replica in the following tape

  FILE_PATH=$FILE_1_COPY

  TAPE_0=${TAPE_LIST_1[0]}

  echo
  echo "########################################################################################################"
  echo " ${TEST_NR}. Testing 'Tape state change from $STATE_START to $STATE_END - queue preserved (1 copy only)"
  echo "########################################################################################################"
  echo "Setting up $TAPE_0 queue as ${STATE_START}..."

  change_tape_state $TAPE_0 $STATE_START

  echo "Requesting file prepare -s..."
  REQUEST_ID=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_MGM_HOST} prepare -s ${FILE_PATH})

  echo "Checking that the request was queued..."

  QUERY_RSP=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_MGM_HOST} query prepare ${REQUEST_ID} ${FILE_PATH})
  PATH_EXISTS=$(echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${FILE_PATH}\").path_exists")
  REQUESTED=$(  echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${FILE_PATH}\").requested")
  HAS_REQID=$(  echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${FILE_PATH}\").has_reqid")
  ERROR_TEXT=$( echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${FILE_PATH}\").error_text")
  if [[
    "true" != "${PATH_EXISTS}" ||
    "true" != "${REQUESTED}"   ||
    "true" != "${HAS_REQID}"   ||
    "\"\"" != "${ERROR_TEXT}" ]]
  then
    echo "ERROR: Request for ${FILE_PATH} not configured as expected: ${QUERY_RSP}"
    exit 1
  fi

  assert_number_of_files_in_queue "$TAPE_0" 1 2
  echo "Changing $TAPE_0 queue to ${STATE_END}..."

  change_tape_state $TAPE_0 $STATE_END

  echo "Checking that the request was not modified on the queue..."

  QUERY_RSP=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_MGM_HOST} query prepare ${REQUEST_ID} ${FILE_PATH})
  PATH_EXISTS=$(echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${FILE_PATH}\").path_exists")
  REQUESTED=$(  echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${FILE_PATH}\").requested")
  HAS_REQID=$(  echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${FILE_PATH}\").has_reqid")
  ERROR_TEXT=$( echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${FILE_PATH}\").error_text")
  if [[
    "true" != "${PATH_EXISTS}" ||
    "true" != "${REQUESTED}"   ||
    "true" != "${HAS_REQID}"   ||
    "\"\"" != "${ERROR_TEXT}" ]]
  then
    echo "ERROR: Request for ${FILE_PATH} not preserved as expected: ${QUERY_RSP}"
    exit 1
  fi

  assert_number_of_files_in_queue "$TAPE_0" 1 2

  echo "Queue preserved, as expected."

  echo "Cleaning up request and queues..."
  KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_MGM_HOST} prepare -a ${REQUEST_ID} ${FILE_PATH}
  trigger_queue_cleanup > /dev/null

  echo "OK"
}


################################################################################
# Test tape state change that moves queue : 2 copies
################################################################################

test_tape_state_change_queue_moved() {

  TEST_NR=$1
  TAPE_0_STATE_START=$2
  TAPE_1_STATE_START=$3
  EXPECTED_QUEUE_START=$4
  TAPE_0_STATE_END=$5
  TAPE_1_STATE_END=$6
  EXPECTED_QUEUE_END=$7

  # Using $FILE_1_COPY, which has 1 replica in the following tape

  FILE_PATH=$FILE_2_COPY

  TAPE_0=${TAPE_LIST_2[0]}
  TAPE_1=${TAPE_LIST_2[1]}

  pid=()
  ret=()

  echo
  echo "########################################################################################################"
  echo " ${TEST_NR}. Testing 'Queue moved on tape state changes from ($TAPE_0_STATE_START, $TAPE_1_STATE_START) to ($TAPE_0_STATE_END, $TAPE_1_STATE_END)"
  echo "########################################################################################################"
  echo "Setting up ${TAPE_0} queue as ${TAPE_0_STATE_START} and ${TAPE_1} queue as ${TAPE_1_STATE_START}..."

  if [[ "0" != "${EXPECTED_QUEUE_START}" && "1" != "${EXPECTED_QUEUE_START}" ]]; then
    echo "Initial request should be put on queue 0 or 1."
    exit 1
  fi

  change_tape_state $TAPE_0 $TAPE_0_STATE_START & pid[0]=$!
  change_tape_state $TAPE_1 $TAPE_1_STATE_START & pid[1]=$!

  wait ${pid[0]}; ret[0]=$?
  wait ${pid[1]}; ret[1]=$?

  if [ ${ret[0]} -ne 0 ] || [ ${ret[1]} -ne 0 ]
  then
    echo "Failed to change tape state"
    exit 1
  fi

  echo "Requesting file prepare -s..."
  REQUEST_ID=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_MGM_HOST} prepare -s ${FILE_PATH})

  echo "Checking that the request was queued..."

  QUERY_RSP=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_MGM_HOST} query prepare ${REQUEST_ID} ${FILE_PATH})
  PATH_EXISTS=$(echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${FILE_PATH}\").path_exists")
  REQUESTED=$(  echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${FILE_PATH}\").requested")
  HAS_REQID=$(  echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${FILE_PATH}\").has_reqid")
  ERROR_TEXT=$( echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${FILE_PATH}\").error_text")
  if [[
    "true" != "${PATH_EXISTS}" ||
    "true" != "${REQUESTED}"   ||
    "true" != "${HAS_REQID}"   ||
    "\"\"" != "${ERROR_TEXT}" ]]
  then
    echo "ERROR: Request for ${FILE_PATH} not configured as expected: ${QUERY_RSP}"
    exit 1
  fi

  if test "0" == "${EXPECTED_QUEUE_START}"; then
    assert_number_of_files_in_queue "$TAPE_0" 1 2
    assert_number_of_files_in_queue "$TAPE_1" 0 2
  else
    assert_number_of_files_in_queue "$TAPE_0" 0 2
    assert_number_of_files_in_queue "$TAPE_1" 1 2
  fi

  # Change tape states, starting by the tape without queue

  if test "0" == "${EXPECTED_QUEUE_START}"; then
    echo "Changing $TAPE_1 queue to ${TAPE_1_STATE_END}..."
    change_tape_state $TAPE_1 $TAPE_1_STATE_END
    echo "Changing $TAPE_0 queue to ${TAPE_0_STATE_END}..."
    change_tape_state $TAPE_0 $TAPE_0_STATE_END
  else
    echo "Changing $TAPE_0 queue to ${TAPE_0_STATE_END}..."
    change_tape_state $TAPE_0 $TAPE_0_STATE_END
    echo "Changing $TAPE_1 queue to ${TAPE_1_STATE_END}..."
    change_tape_state $TAPE_1 $TAPE_1_STATE_END
  fi

  if [[ "0" == "${EXPECTED_QUEUE_END}" || "1" == "${EXPECTED_QUEUE_END}" ]]; then

    echo "Checking that the request was moved from the queue ${TAPE_LIST_2[$EXPECTED_QUEUE_START]} to the queue ${TAPE_LIST_2[$EXPECTED_QUEUE_END]}..."

    if test "0" == "${EXPECTED_QUEUE_END}"; then
      assert_number_of_files_in_queue "$TAPE_0" 1 2
      assert_number_of_files_in_queue "$TAPE_1" 0 2
    else
      assert_number_of_files_in_queue "$TAPE_0" 0 2
      assert_number_of_files_in_queue "$TAPE_1" 1 2
    fi

    echo "Request moved to new queue, as expected."

  else

    echo "Checking that the request queue ${TAPE_LIST_2[$EXPECTED_QUEUE_START]} was canceled and the error reported to the user..."

    wait_for_request_cancel_report ${REQUEST_ID} ${FILE_PATH}

    QUERY_RSP=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_MGM_HOST} query prepare ${REQUEST_ID} ${FILE_PATH})
    PATH_EXISTS=$(echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${FILE_PATH}\").path_exists")
    REQUESTED=$(  echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${FILE_PATH}\").requested")
    HAS_REQID=$(  echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${FILE_PATH}\").has_reqid")
    ERROR_TEXT=$( echo ${QUERY_RSP} | jq ".responses[] | select(.path == \"${FILE_PATH}\").error_text")
    if [[
      "true"  != "${PATH_EXISTS}" ||
      "false" != "${REQUESTED}"   ||
      "false" != "${HAS_REQID}"   ||
      "\"\""  == "${ERROR_TEXT}" ]]
    then
      echo "ERROR: Request for ${FILE_PATH} not removed as expected: ${QUERY_RSP}"
      exit 1
    fi

    assert_number_of_files_in_queue "$TAPE_0" 0 2
    assert_number_of_files_in_queue "$TAPE_1" 0 2
    echo "Request removed and error reported back to user, as expected."
  fi

  echo "Cleaning up request and queues..."
  KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_MGM_HOST} prepare -a ${REQUEST_ID} ${FILE_PATH}
  trigger_queue_cleanup > /dev/null

  echo "OK"
}


################################################################
# Finalize
################################################################

test_tape_state_queueing_priority 1 DISABLED DISABLED ACTIVE 2                  # ACTIVE queue has priority over DISABLED queue (1)
test_tape_state_queueing_priority 2 DISABLED ACTIVE DISABLED 1                  # ACTIVE queue has priority over DISABLED queue (2)
test_tape_state_queueing_priority 3 REPACKING BROKEN DISABLED 2                 # DISABLED queue selected when no ACTIVE queue is available (1)
test_tape_state_queueing_priority 4 BROKEN DISABLED REPACKING 1                 # DISABLED queue selected when no ACTIVE queue is available (2)
test_tape_state_queueing_priority 5 BROKEN REPACKING EXPORTED 9999              # Request not queued on REPACKING or BROKEN or EXPORTED queues
test_tape_state_queueing_priority 6 REPACKING REPACKING REPACKING_DISABLED 9999 # Request not queued on REPACKING or REPACKING_DISABLED queues
test_tape_state_change_queue_removed 7 ACTIVE REPACKING                         # Request canceled and reported to user, after state changed from ACTIVE to REPACKING
test_tape_state_change_queue_removed 8 ACTIVE BROKEN                            # Request canceled and reported to user, after state changed from ACTIVE to BROKEN
test_tape_state_change_queue_removed 9 ACTIVE EXPORTED                          # Request canceled and reported to user, after state changed from ACTIVE to EXPORTED
test_tape_state_change_queue_removed 10 DISABLED REPACKING                      # Request canceled and reported to user, after state changed from DISABLED to REPACKING
test_tape_state_change_queue_removed 11 DISABLED BROKEN                         # Request canceled and reported to user, after state changed from DISABLED to BROKEN
test_tape_state_change_queue_removed 12 DISABLED EXPORTED                       # Request canceled and reported to user, after state changed from DISABLED to EXPORTED
test_tape_state_change_queue_preserved 13 ACTIVE DISABLED                       # Request preserved on queue, after state changed from ACTIVE to DISABLED
test_tape_state_change_queue_preserved 14 DISABLED ACTIVE                       # Request preserved on queue, after state changed from DISABLED to ACTIVE
test_tape_state_change_queue_moved 15 ACTIVE DISABLED 0 REPACKING ACTIVE 1      # State changed from ACTIVE to REPACKING, requests moved to another ACTIVE queue
test_tape_state_change_queue_moved 16 DISABLED ACTIVE 1 DISABLED BROKEN 0       # State changed from ACTIVE to BROKEN, request moved to another DISABLED queue (ACTIVE queue not available)
test_tape_state_change_queue_moved 17 ACTIVE BROKEN 0 REPACKING BROKEN 9999     # State changed from ACTIVE to REPACKING, request canceled and reported to user (ACTIVE/DISABLED queue not available))

echo
echo "OK: all tests passed"
