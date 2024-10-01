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
#   - This tests exists to make sure that tape-enabled files with no tape copy
#   cannot be evicted from disk.
#
# EXPECTED BEHAVIOUR
#
#   # The following steps will be carried:
#
#   1. Put the destination tape drives down.
#   2. Write a file to EOSCTA for archiving.
#   3. Execute eos root://MGM_HOST stagerrm PATH.
#   4. Test the exit code of the command is failure.
#   5. Test the disk replica still exists.
#   6. Execute xrdfs MGM_HOST prepare -e PATH.
#   7. Test the exit code of the command is failure.
#   8. Test the disk replica still exists.
#   9. Put the destination tape drives up.
#   10. Test the file is successfully archived to tape.
#
################################################################################

EOS_INSTANCE=ctaeos

EOS_BASEDIR=/eos/ctaeos
EOS_TAPE_BASEDIR=$EOS_BASEDIR/cta

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
  INITIAL_DRIVES_STATE=$(admin_cta --json dr ls)
  echo INITIAL_DRIVES_STATE:
  echo ${INITIAL_DRIVES_STATE} | jq -r '.[] | [ .driveName, .driveStatus] | @tsv' | column -t
  echo -n "Will put $next_state those drives : "
  drivesToModify=$(echo ${INITIAL_DRIVES_STATE} | jq -r ".[].driveName")
  echo $drivesToModify
  for d in $(echo $drivesToModify); do
    admin_cta drive $next_state $d --reason "PUTTING DRIVE $NEXT_STATE FOR TESTS"
  done

  echo "$(date +%s): Waiting for the drives to be $next_state"
  SECONDS_PASSED=0
  WAIT_FOR_DRIVES_TIMEOUT=$((10))
  while [[ $SECONDS_PASSED -lt $WAIT_FOR_DRIVES_TIMEOUT ]]; do
    sleep 1
    oneStatusRemaining=0
    for d in $(echo $drivesToModify); do
      status=$(admin_cta --json drive ls | jq -r ". [] | select(.driveName == \"$d\") | .driveStatus")
      if [[ $NEXT_STATE == "DOWN" ]]; then
        # Anything except DOWN is not acceptable
        if [[ $status != "DOWN" ]]; then
          oneStatusRemaining=1
        fi;
      else
        # Only DOWN is not OK. Starting, Unmounting, Running == UP
        if [[ $status == "DOWN" ]]; then
          oneStatusRemaining=1
        fi;
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

error()
{
  echo "ERROR: $*" >&2
  exit 1
}

################################################################
# Test deleting file before tape copy has been generated
################################################################

# 1. Put the destination tape drives down
TEMP_FILE=${EOS_TAPE_BASEDIR}/$(uuidgen)
echo
echo "Putting all drives down. No file will be written to tape..."
put_all_drives_down

# 2. Write a file to EOSCTA for archiving
echo "Write file ${TEMP_FILE} for archival..."
xrdcp /etc/group root://${EOS_INSTANCE}/${TEMP_FILE}

# 3/4. Check that stagerrm fails
echo "Testing 'eos root://${EOS_INSTANCE} stagerrm ${TEMP_FILE}'..."
KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos root://${EOS_INSTANCE} stagerrm ${TEMP_FILE}
if [ $? -eq 0 ]; then
  error "eos stagerrm command succeeded where it should have failed"
else
  echo "eos stagerrm command failed as expected"
fi

# 5. Check that disk replica still exists
echo "Checking that ${TEMP_FILE} replica still exists on disk..."
if test 0 == $(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos root://${EOS_INSTANCE} info ${TEMP_FILE} | grep -F "default.0" | wc -l); then
  error "eos stagerrm removed disk replica, when it should have failed"
else
  echo "eos stagerrm did not remove disk replica, as expected"
fi

# 6/7. Check that prepare -e fails
echo "Testing 'xrdfs root://${EOS_INSTANCE} prepare -e ${TEMP_FILE}'..."
KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs root://${EOS_INSTANCE} prepare -e ${TEMP_FILE}
if [ $? -eq 0 ]; then
  #error "prepare -e command succeeded where it should have failed"
  # 'prepare -e' will not return an error because WFE errors are not propagated to the user. Therefore, we ignore this check.
  :
else
  echo "prepare -e stagerrm command failed as expected"
fi

# 8. Check that disk replica still exists
echo "Checking that ${TEMP_FILE} replica still exists on disk..."
if test 0 == $(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos root://${EOS_INSTANCE} info ${TEMP_FILE} | grep -F "default.0" | wc -l); then
  error "prepare -e removed disk replica, when it should have failed"
else
  echo "prepare -e did not remove disk replica, as expected"
fi

# 9. Put the destination tape drives up and wait for archival
echo "Putting all drives up. File will finally be written to tape and removed from disk..."
put_all_drives_up

echo "Waiting for archival of ${TEMP_FILE}..."
wait_for_archive ${TEMP_FILE}

# 10. Check that disk replica was deleted and that new tape replica exists
echo "Checking that ${TEMP_FILE} replica no longer exists on disk..."
if test 0 != $(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos root://${EOS_INSTANCE} info ${TEMP_FILE} | grep -F "default.0" | wc -l); then
  error "Disk replica not removed, when it should have been done after archival"
else
  echo "Disk replica was removed, as expected"
fi

echo "Checking that ${TEMP_FILE} replica no longer exists on disk..."
if test 0 == $(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos root://${EOS_INSTANCE} info ${TEMP_FILE} | grep -F "tape" | wc -l); then
  error "Tape replica does not exist, when it should have been created after archival"
else
  echo "Tape replica creted, as expected"
fi


################################################################
# Finalize
################################################################

echo
echo "OK: all tests passed"
