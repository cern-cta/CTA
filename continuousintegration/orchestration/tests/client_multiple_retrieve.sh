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
#   - This script tests the usage of the evict counter (xattr
#   sys.retrieve.evict_counter) to handle eviction for multiple staging requests
#   on the same file.
#   - This behaviour allows multiple clients to trigger a PREPARE of the same
#   file and use it safelly, because it will only be removed once the last
#   client has sent the EVICT_PREPARE.
#
# EXPECTED BEHAVIOUR
#
#   1. When a PREPARE event succeeds, the list of request IDs is set to empty.
#   Before that, the evict counter should be initialised to the number of
#   request IDs in the list.
#   2. When an EVICT event is received, the counter should be decremented by 1.
#   3. If a PREPARE is received for a file which is already on disk, the counter
#   should be incremented by 1.
#   4. When the counter reaches 0, the disk replica should be evicted.
#
#   From an end user point-of-view, the evict count should not
#   change the observed behaviour.
#
################################################################################


EOS_INSTANCE=ctaeos
EOS_BASEDIR=/eos/ctaeos/cta
EVICT_COUNTER_ATTR="sys.retrieve.evict_counter"

NB_RETRIEVES=2
NB_RETRIEVES_EXTRA=2
NB_FILES=4

# get some common useful helpers for krb5
. /root/client_helper.sh

eospower_kdestroy
eospower_kinit

FAILED_LIST=$(mktemp)
touch ${FAILED_LIST}

################################################################################
# Define list of test files
################################################################################

TEST_FILES_LIST=$(mktemp)

echo
echo "Test files list (${NB_FILES} files):"
for ((file_idx=0; file_idx < ${NB_FILES}; file_idx++)); do
  echo "${EOS_BASEDIR}/$(uuidgen)" | tee -a ${TEST_FILES_LIST}
done


################################################################################
# Copy files and wait for them to be archived
################################################################################

echo
echo "Archiving test files..."

cat ${TEST_FILES_LIST} | xargs -iFILE_PATH xrdcp --silent /etc/group root://${EOS_INSTANCE}/FILE_PATH

SECONDS_PASSED=0
WAIT_FOR_ARCHIVED_FILE_TIMEOUT=-1
while test ${NB_FILES} != `cat ${TEST_FILES_LIST} | xargs -iFILE_PATH eos root://${EOS_INSTANCE} info FILE_PATH | awk '{print $4;}' | grep tape | wc -l`; do
  echo "Waiting for files to be archived to tape: seconds passed = ${SECONDS_PASSED}"
  sleep 1
  let SECONDS_PASSED=SECONDS_PASSED+1

  if test ${SECONDS_PASSED} == ${WAIT_FOR_ARCHIVED_FILE_TIMEOUT}; then
    echo "ERROR: Timed out after ${WAIT_FOR_ARCHIVED_FILE_TIMEOUT} seconds waiting for files to be archived to tape"
    exit 1
  fi
done
echo "Files archived successfully."


################################################################################
# Trigger EOS retrieve workflow
################################################################################

# NB_RETRIEVE times for each file

echo
echo "Trigering EOS retrieve workflow as poweruser1:powerusers..."
echo "${NB_RETRIEVES} retrieve requests per file"

# We need the -s as we are staging the files from tape (see xrootd prepare definition)
for ((retrieve_req=0; retrieve_req < ${NB_RETRIEVES}; retrieve_req++)); do
  cat ${TEST_FILES_LIST} | KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xargs xrdfs ${EOS_INSTANCE} prepare -s
done

# Wait for the copy to appear on disk

SECONDS_PASSED=0
WAIT_FOR_RETRIEVED_FILE_TIMEOUT=90
while test ${NB_FILES} != `cat ${TEST_FILES_LIST} | xargs -iFILE_PATH eos root://${EOS_INSTANCE} info FILE_PATH | awk '{print $4;}' | grep -F "default.0" | wc -l`; do
  echo "Waiting for files to be retrieved from tape: seconds passed = ${SECONDS_PASSED}"
  sleep 1
  let SECONDS_PASSED=SECONDS_PASSED+1

  if test ${SECONDS_PASSED} == ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT}; then
    echo "ERROR: Timed out after ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT} seconds waiting for files to be retrieved from tape"
    exit 1
  fi
done
echo "Files retrieved successfully"


################################################################################
# Check evict counter
################################################################################

# The evict counter is stored in the xattr sys.retrieve.evict_counter.
# This step checks that its value is equal to the number of PREPARE requests
# previously executed. Value should be equal to NB_RETRIEVES

EXPECTED_COUNTER_VAL=${NB_RETRIEVES}

echo
echo "Checking evict counter..."
echo "Value should be ${EXPECTED_COUNTER_VAL} for each file"

rm -f ${FAILED_LIST}
touch ${FAILED_LIST}
if test 0 != $(cat ${TEST_FILES_LIST} | xargs -iFILE_PATH bash -c "KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_INSTANCE} query opaquefile FILE_PATH?mgm.pcmd=xattr\&mgm.subcmd=get\&mgm.xattrname=${EVICT_COUNTER_ATTR} | grep -v value=${EXPECTED_COUNTER_VAL} | sed -e 's%\(.*\)%FILE_PATH: \1%g'" | tee ${FAILED_LIST} | wc -l); then
  echo "ERROR: Attr ${EVICT_COUNTER_ATTR} does not have expected value ${EXPECTED_COUNTER_VAL} for some files:"
  cat ${FAILED_LIST}
  exit 1
fi
echo "Evict counter value is correct"

################################################################################
# Re-trigger EOS retrieve workflow
################################################################################

# Repeat a PREPARE request for the same files. NB_RETRIEVE_EXTRA times for each file.
# Files are already on disk, so the eviction counter value should be incremented by
# NB_RETRIEVE_EXTRA.

echo
echo "Trigering EOS retrieve workflow (again) as poweruser1:powerusers..."
echo "${NB_RETRIEVES_EXTRA} new retrieve requests per file"
# We need the -s as we are staging the files from tape (see xrootd prepare definition)
for ((retrieve_req=0; retrieve_req < ${NB_RETRIEVES_EXTRA}; retrieve_req++)); do
  cat ${TEST_FILES_LIST} | KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xargs xrdfs ${EOS_INSTANCE} prepare -s
done


################################################################################
# Re-check evict counter
################################################################################

# Check that the new evict counter value is equal to NB_RETRIEVES + NB_RETRIEVES_EXTRA

EXPECTED_COUNTER_VAL=$((${NB_RETRIEVES}+${NB_RETRIEVES_EXTRA}))

echo
echo "Re-checking evict counter..."
echo "Value should be ${EXPECTED_COUNTER_VAL} for each file"

rm -f ${FAILED_LIST}
touch ${FAILED_LIST}
if test 0 != $(cat ${TEST_FILES_LIST} | xargs -iFILE_PATH bash -c "KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_INSTANCE} query opaquefile FILE_PATH?mgm.pcmd=xattr\&mgm.subcmd=get\&mgm.xattrname=${EVICT_COUNTER_ATTR} | grep -v value=${EXPECTED_COUNTER_VAL} | sed -e 's%\(.*\)%FILE_PATH: \1%g'" | tee ${FAILED_LIST} | wc -l); then
  echo "ERROR: Attr ${EVICT_COUNTER_ATTR} does not have expected value ${EXPECTED_COUNTER_VAL} for some files:"
  cat ${FAILED_LIST}
  exit 1
fi
echo "Evict counter value is correct"

################################################################################
# Trigger EOS evict workflow
################################################################################

# Request each file to be evicted (NB_RETRIEVES + NB_RETRIEVES_EXTRA) times.
# Each EVICT_PREPARE request should reduce eviction counter by 1.
# When counter reaches 0, disk replica should be removed.

STARTING_COUNTER_VAL=$((${NB_RETRIEVES}+${NB_RETRIEVES_EXTRA}))

echo
echo "Requesting prepare evict until disk replicas are deleted..."
for ((expected_counter_val=${STARTING_COUNTER_VAL}; expected_counter_val > 0; expected_counter_val--)); do

  rm -f ${FAILED_LIST}
  touch ${FAILED_LIST}
  if test 0 != $(cat ${TEST_FILES_LIST} | xargs -iFILE_PATH bash -c "KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_INSTANCE} query opaquefile FILE_PATH?mgm.pcmd=xattr\&mgm.subcmd=get\&mgm.xattrname=${EVICT_COUNTER_ATTR} | grep -v value=${expected_counter_val} | sed -e 's%\(.*\)%FILE_PATH: \1%g'" | tee ${FAILED_LIST} | wc -l); then
  echo "ERROR: Attr ${EVICT_COUNTER_ATTR} does not have expected value ${EXPECTED_COUNTER_VAL} for some files:"
    cat ${FAILED_LIST}
    exit 1
  fi

  rm -f ${FAILED_LIST}
  touch ${FAILED_LIST}
  if test 0 != $(cat ${TEST_FILES_LIST} | xargs -iFILE_PATH eos root://${EOS_INSTANCE} ls -y FILE_PATH | egrep -v '^d[1-9][0-9]*::t1' | tee ${FAILED_LIST} | wc -l); then
    echo "ERROR: Attr ${EVICT_COUNTER_ATTR} is higher than 0. Files should have not been evicted."
    cat ${FAILED_LIST}
    exit 1
  fi

  echo "${EVICT_COUNTER_ATTR} to be reduced from ${expected_counter_val} to $(($expected_counter_val-1))"
  cat ${TEST_FILES_LIST} | KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xargs xrdfs ${EOS_INSTANCE} prepare -e

done


################################################################################
# Validate file eviction
################################################################################

# All files should have been evicted by now.

rm -f ${FAILED_LIST}
touch ${FAILED_LIST}
if test 0 != $(cat ${TEST_FILES_LIST} | xargs -iFILE_PATH eos root://${EOS_INSTANCE} ls -y FILE_PATH | egrep '^d[1-9][0-9]*::t1' | tee ${FAILED_LIST} | wc -l); then
  echo "ERROR: Files should have been evicted when attr ${EVICT_COUNTER_ATTR} is zero."
  cat ${FAILED_LIST}
  exit 1
fi

echo "Files replicas evicted from disk successfully"


################################################################################
# Cleanup
################################################################################

echo
echo "Cleaning up test files..."
cat ${TEST_FILES_LIST} | xargs -iFILE_PATH eos root://${EOS_INSTANCE} rm FILE_PATH
echo "OK: all tests passed"
