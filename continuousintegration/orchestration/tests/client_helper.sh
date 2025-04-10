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


################################################################
# Helper functions - KRB5
################################################################

###
# Helper functions for tests running on client pod.
#
# to use in your tests symply source this file:
# . /root/client_helper.sh on the client pod
#
# admin_kinit: kinit for CTAADMIN_USER
# admin_klist: klist for CTAADMIN_USER
# admin_kdestroy: kdestroy for CTAADMIN_USER
# admin_cta: runs a cta command as CTAADMIN_USER

EOSPOWER_USER="poweruser1"
CTAADMIN_USER="ctaadmin2"
EOSADMIN_USER="eosadmin1"
USER="user1"
KRB5_REALM="TEST.CTA"
TOKEN_TIMEOUT=604800

die() {
  echo "$@" 1>&2
  test -z ${TAILPID} || kill ${TAILPID} &> /dev/null
  exit 1
}

user_kinit() {
  kinit -kt /root/${USER}.keytab ${USER}@${KRB5_REALM}
  klist
}

admin_cta() {
  KRB5CCNAME=/tmp/${CTAADMIN_USER}/krb5cc_0 cta-admin "$@"
}

admin_klist() {
  KRB5CCNAME=/tmp/${CTAADMIN_USER}/krb5cc_0 klist
}

admin_kinit() {
  KRB5CCNAME=/tmp/${CTAADMIN_USER}/krb5cc_0 kinit -kt /root/${CTAADMIN_USER}.keytab ${CTAADMIN_USER}@${KRB5_REALM}
  admin_klist
}

admin_kdestroy() {
  KRB5CCNAME=/tmp/${CTAADMIN_USER}/krb5cc_0 kdestroy
  admin_klist
}

eospower_eos() {
  XrdSecPROTOCOL=krb5 KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 eos $@
}

eospower_klist() {
  KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 klist
}

eospower_kinit() {
  KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 kinit -kt /root/${EOSPOWER_USER}.keytab ${EOSPOWER_USER}@${KRB5_REALM}
  eospower_klist
}

eospower_kdestroy() {
  KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 kdestroy
  eospower_klist
}

eosadmin_eos() {
  XrdSecPROTOCOL=krb5 KRB5CCNAME=/tmp/${EOSADMIN_USER}/krb5cc_0 eos -r 0 0 $@
}

eosadmin_klist() {
  KRB5CCNAME=/tmp/${EOSADMIN_USER}/krb5cc_0 klist
}

eosadmin_kinit() {
  KRB5CCNAME=/tmp/${EOSADMIN_USER}/krb5cc_0 kinit -kt /root/${EOSADMIN_USER}.keytab ${EOSADMIN_USER}@${KRB5_REALM}
  eosadmin_klist
}

eosadmin_kdestroy() {
  KRB5CCNAME=/tmp/${EOSADMIN_USER}/krb5cc_0 kdestroy
  eosadmin_klist
}

################################################################
# Helper functions - Requests
################################################################

# Pass list of files waiting for archival
# This sciprt fails if there are files stored in the target directory as it just counts the lines.
wait_for_archive () {

  EOS_MGM_HOST=$1
  SECONDS_PASSED=0
  WAIT_FOR_ARCHIVED_FILE_TIMEOUT=90

  while test $(($# - 1)) != $(echo "${@:2}" | tr " " "\n" | xargs -iFILE eos root://${EOS_MGM_HOST} info FILE | awk '{print $4;}' | grep tape | wc -l); do
    echo "$(date +%s) Waiting for files to be archived to tape: seconds passed = ${SECONDS_PASSED}"
    sleep 1
    let SECONDS_PASSED=SECONDS_PASSED+1

    if test ${SECONDS_PASSED} == ${WAIT_FOR_ARCHIVED_FILE_TIMEOUT}; then
      echo "$(date +%s) ERROR: Timed out after ${WAIT_FOR_ARCHIVED_FILE_TIMEOUT} seconds waiting for files to be archived to tape"
      exit 1
    fi
  done

}

# Pass list of files waiting for retrieval

wait_for_retrieve () {

  EOS_MGM_HOST=$1
  SECONDS_PASSED=0
  WAIT_FOR_RETRIEVED_FILE_TIMEOUT=90
  while test $(($# - 1)) != $(echo "${@:2}" | tr " " "\n" | xargs -iFILE eos root://${EOS_MGM_HOST} info FILE | awk '{print $4;}' | grep -F "default.0" | wc -l); do
    echo "$(date +%s) Waiting for files to be retrieved from tape: Seconds passed = ${SECONDS_PASSED}"
    sleep 1
    let SECONDS_PASSED=SECONDS_PASSED+1

    if test ${SECONDS_PASSED} == ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT}; then
      echo "$(date +%s) ERROR: Timed out after ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT} seconds waiting for files to be retrieved from tape"
      exit 1
    fi
  done

}

# Pass list of files waiting for eviction

wait_for_evict () {

  EOS_MGM_HOST=$1
  SECONDS_PASSED=0
  WAIT_FOR_EVICTED_FILE_TIMEOUT=90
  while test 0 != $(echo "${@:2}" | tr " " "\n" | xargs -iFILE eos root://${EOS_MGM_HOST} info FILE | awk '{print $4;}' | grep -F "default.0" | wc -l); do
    echo "$(date +%s) Waiting for files to be evicted from disk: Seconds passed = ${SECONDS_PASSED}"
    sleep 1
    let SECONDS_PASSED=SECONDS_PASSED+1

    if test ${SECONDS_PASSED} == ${WAIT_FOR_EVICTED_FILE_TIMEOUT}; then
      echo "$(date +%s) ERROR: Timed out after ${WAIT_FOR_EVICTED_FILE_TIMEOUT} seconds waiting for files to be evicted from disk"
      exit 1
    fi
  done

}

# Wait for tape change

wait_for_tape_state() {

  SECONDS_PASSED=0
  WAIT_FOR_EVICTED_FILE_TIMEOUT=90
  echo "$(date +%s) Waiting for tape $1 state to change to $2: Seconds passed = ${SECONDS_PASSED}"
  while test $2 != $(admin_cta --json tape ls --vid $1 | jq -r '.[] | .state'); do
    sleep 1
    let SECONDS_PASSED=SECONDS_PASSED+1
    echo "$(date +%s) Waiting for tape $1 state to change to $2: Seconds passed = ${SECONDS_PASSED}"

    if test ${SECONDS_PASSED} == ${WAIT_FOR_EVICTED_FILE_TIMEOUT}; then
      echo "$(date +%s) ERROR: Timed out after ${WAIT_FOR_EVICTED_FILE_TIMEOUT} seconds waiting for tape $1 state to change to $2"
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
      echo "$(date +%s) Drives : $drivesToModify are $next_state"
      break;
    fi
    echo -n "."
    SECONDS_PASSED=$SECONDS_PASSED+1
    if [[ $SECONDS_PASSED -gt $WAIT_FOR_DRIVES_TIMEOUT ]]; then
      die "$(date +%s) ERROR: Timeout reach for trying to put all drives $next_state"
    fi
  done

}

put_all_drives_up () {
  put_all_drives "UP"
}

put_all_drives_down () {
  put_all_drives "DOWN"
}
