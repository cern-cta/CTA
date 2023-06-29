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
#   - These tests exists to make sure that the evict command works as expected
#
################################################################################

EOS_INSTANCE=ctaeos

EOS_BASEDIR=/eos/ctaeos
EOS_TAPE_BASEDIR=$EOS_BASEDIR/cta

FSID_DUMMY_1_VALUE=101
FSID_DUMMY_1_NAME="dummy_1"
FSID_DUMMY_2_VALUE=102
FSID_DUMMY_2_NAME="dummy_2"
FSID_DUMMY_3_VALUE=103
FSID_DUMMY_3_NAME="dummy_3"
FSID_NOT_SET_VALUE=200

FSID_TAPE=65535

# get some common useful helpers for krb5
. /root/client_helper.sh

error()
{
  echo "ERROR: $*" >&2
  exit 1
}

eospower_kdestroy &>/dev/null
eospower_kinit &>/dev/null

admin_kdestroy &>/dev/null
admin_kinit &>/dev/null

################################################################
# Test deleting file before tape copy has been generated
################################################################

# 1. Put the destination tape drives down
TEMP_FILE=${EOS_TAPE_BASEDIR}/$(uuidgen)
echo
echo "Putting all drives down. No file will be written to tape..."
put_all_drives_down

# 2.1. Write a file to EOSCTA for archiving
echo "Write file ${TEMP_FILE} for archival..."
xrdcp /etc/group root://${EOS_INSTANCE}/${TEMP_FILE}

# 2.2. Get FSID from disk replica
DISK_FSID=$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos --json root://${EOS_INSTANCE} info ${TEMP_FILE} | jq -r '.locations[] | select(.schedgroup | startswith("default")) | .fsid')

# 3.1. Check that evict fails when there is no tape replica
echo "Testing 'eos root://${EOS_INSTANCE} evict ${TEMP_FILE}'..."
KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos root://${EOS_INSTANCE} evict ${TEMP_FILE}
if [ $? -eq 0 ]; then
  error "'eos evict' command succeeded where it should have failed"
else
  echo "'eos evict' command failed as expected"
fi

# 3.2. Check that evict --force --fsid <fsid> fails when there is no tape replica
echo "Testing 'eos root://${EOS_INSTANCE} evict --force --fsid ${DISK_FSID} ${TEMP_FILE}'..."
KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos root://${EOS_INSTANCE} evict --force --fsid ${DISK_FSID} ${TEMP_FILE}
if [ $? -eq 0 ]; then
  error "'eos evict' command succeeded where it should have failed"
else
  echo "'eos evict' command failed as expected"
fi

# 4. Check that disk replica still exists
echo "Checking that ${TEMP_FILE} replica still exists on disk..."
if test 0 == "$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos --json root://${EOS_INSTANCE} info ${TEMP_FILE}  | jq -r '.locations[] | select(.schedgroup | startswith("default")) | .schedgroup' | wc -l)"; then
  error "'eos evict' removed disk replica, when it should have failed"
else
  echo "'eos evict' did not remove disk replica, as expected"
fi

# 5. Put the destination tape drives up and wait for archival
echo "Putting all drives up. File will finally be written to tape and removed from disk..."
put_all_drives_up

echo "Waiting for archival of ${TEMP_FILE}..."
wait_for_archive ${EOS_INSTANCE} ${TEMP_FILE}

# 6.1 Check that disk replica was deleted and that new tape replica exists
echo "Checking that ${TEMP_FILE} replica no longer exists on disk..."
if test 0 != "$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos --json root://${EOS_INSTANCE} info ${TEMP_FILE} | jq -r '.locations[] | select(.schedgroup | startswith("default")) | .schedgroup' | wc -l)"; then
  error "Disk replica not removed, when it should have been done after archival"
else
  echo "Disk replica was removed, as expected"
fi

# 6.2 Check that disk replica was deleted and that new tape replica exists
echo "Checking that ${TEMP_FILE} replica exists on tape..."
if test 0 == "$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos --json root://${EOS_INSTANCE} info ${TEMP_FILE} | jq -r '.locations[] | select(.schedgroup | startswith("tape")) | .schedgroup' | wc -l)"; then
  error "Tape replica does not exist, when it should have been created after archival"
else
  echo "Tape replica created, as expected"
fi

# 7. Staging file multiple times
NB_RETRIEVES=3
echo "Retrieving ${TEMP_FILE} to disk ${NB_RETRIEVES} times..."
# We need the -s as we are staging the files from tape (see xrootd prepare definition)
for ((retrieve_req=0; retrieve_req < ${NB_RETRIEVES}; retrieve_req++)); do
  KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_INSTANCE} prepare -s ${TEMP_FILE}
done

# 8. Wait for the copy to appear on disk
SECONDS_PASSED=0
WAIT_FOR_RETRIEVED_FILE_TIMEOUT=90
while test 0 == "$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos --json root://${EOS_INSTANCE} info ${TEMP_FILE}  | jq -r '.locations[] | select(.schedgroup | startswith("default")) | .schedgroup' | wc -l)"; do
  echo "Waiting for files to be retrieved from tape: seconds passed = ${SECONDS_PASSED}"
  sleep 1
  let SECONDS_PASSED=SECONDS_PASSED+1
  if test ${SECONDS_PASSED} == ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT}; then
    error "Timed out after ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT} seconds waiting for files to be retrieved from tape"
  fi
done
echo "File retrieve successfully"

# 9. Check eviction counter
echo "Checking that evict decreases eviction counter (without --force/--fsid)..."
for ((retrieve_req=${NB_RETRIEVES}; retrieve_req > 0; retrieve_req--)); do
  echo "Checking evict counter, expecting ${retrieve_req}..."
  if test ${retrieve_req} != "$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos --json root://${EOS_INSTANCE} attr get sys.retrieve.evict_counter ${TEMP_FILE} | jq -r '.attr.get[].sys.retrieve.evict_counter')"; then
    error "Evict counter sys.retrieve.evict_counter value is not ${retrieve_req}"
  fi
  KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos root://${EOS_INSTANCE} evict ${TEMP_FILE}
done

# 10. Check that file was evicted after clearing the eviction counter
echo "Checking that ${TEMP_FILE} was evicted after clearing the eviction counter..."
if test 0 != "$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos --json root://${EOS_INSTANCE} info ${TEMP_FILE} | jq -r '.locations[] | select(.schedgroup | startswith("default")) | .schedgroup' | wc -l)"; then
  error "Disk replica not evicted, when it should have been done after the eviction counter reached zero"
else
  echo "Disk replica was evicted, as expected"
fi

# 11. Setting up dummy spaces and file systems for testing
echo "Setting up dummy spaces and file systems (${FSID_DUMMY_1_NAME}=${FSID_DUMMY_1_VALUE}, ${FSID_DUMMY_2_NAME} =${FSID_DUMMY_2_VALUE}, ${FSID_DUMMY_3_NAME}=${FSID_DUMMY_3_VALUE}) for testing..."
XrdSecPROTOCOL=sss eos -r 0 0 root://${EOS_INSTANCE} space define ${FSID_DUMMY_1_NAME}
XrdSecPROTOCOL=sss eos -r 0 0 root://${EOS_INSTANCE} space define ${FSID_DUMMY_2_NAME}
XrdSecPROTOCOL=sss eos -r 0 0 root://${EOS_INSTANCE} space define ${FSID_DUMMY_3_NAME}
XrdSecPROTOCOL=sss eos -r 0 0 root://${EOS_INSTANCE} fs add -m ${FSID_DUMMY_1_VALUE} ${FSID_DUMMY_1_NAME} localhost:1234 /does_not_exist_1 ${FSID_DUMMY_1_NAME}
XrdSecPROTOCOL=sss eos -r 0 0 root://${EOS_INSTANCE} fs add -m ${FSID_DUMMY_2_VALUE} ${FSID_DUMMY_2_NAME} localhost:1234 /does_not_exist_2 ${FSID_DUMMY_2_NAME}
XrdSecPROTOCOL=sss eos -r 0 0 root://${EOS_INSTANCE} fs add -m ${FSID_DUMMY_3_VALUE} ${FSID_DUMMY_3_NAME} localhost:1234 /does_not_exist_3 ${FSID_DUMMY_3_NAME}

# 12. Adding dummy file systems to namespace
echo "Adding dummy filesystems ${FSID_DUMMY_1_NAME}, ${FSID_DUMMY_2_NAME} and ${FSID_DUMMY_3_NAME} to ${TEMP_FILE} ..."
XrdSecPROTOCOL=sss eos -r 0 0 root://${EOS_INSTANCE} file tag "${TEMP_FILE}" +${FSID_DUMMY_1_VALUE}
XrdSecPROTOCOL=sss eos -r 0 0 root://${EOS_INSTANCE} file tag "${TEMP_FILE}" +${FSID_DUMMY_2_VALUE}
XrdSecPROTOCOL=sss eos -r 0 0 root://${EOS_INSTANCE} file tag "${TEMP_FILE}" +${FSID_DUMMY_3_VALUE}

# 13. Check that there are 4 replicas advertised on the namespace now
echo "Checking that ${TEMP_FILE} has 4 replicas advertised on the namespace..."
if test 4 != "$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos --json root://${EOS_INSTANCE} info ${TEMP_FILE} | jq -r '.locations[] | .schedgroup' | wc -l)"; then
  error "The number of replicas is not the expected one"
else
  echo "Disk replica count is as expected"
fi

# 14. Test removing tape replica, should fail
echo "Trying to remove tape replica with fsid ${FSID_TAPE}..."
KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos root://${EOS_INSTANCE} evict --force --fsid ${FSID_TAPE} ${TEMP_FILE}
if [ $? -eq 0 ]; then
  error "'eos evict --force --fsid ${FSID_TAPE}' command succeeded where it should have failed"
else
  echo "'eos evict --force --fsid ${FSID_TAPE}' command failed as expected"
fi
if test 4 != "$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos --json root://${EOS_INSTANCE} info ${TEMP_FILE} | jq -r '.locations[] | .schedgroup' | wc -l)"; then
  error "The number of replicas is not the expected one"
else
  echo "Disk replica count is as expected"
fi

# 15. Test removing non-existing replica, should fail
echo "Trying to remove tape replica with non existing fsid ${FSID_NOT_SET_VALUE}..."
KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos root://${EOS_INSTANCE} evict --force --fsid ${FSID_NOT_SET_VALUE} ${TEMP_FILE}
if [ $? -eq 0 ]; then
  error "'eos evict --force --fsid ${FSID_NOT_SET_VALUE}' command succeeded where it should have failed"
else
  echo "'eos evict --force --fsid ${FSID_NOT_SET_VALUE}' command failed as expected"
fi
if test 4 != "$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos --json root://${EOS_INSTANCE} info ${TEMP_FILE} | jq -r '.locations[] | .schedgroup' | wc -l)"; then
  error "The number of replicas is not the expected one"
else
  echo "Disk replica count is as expected"
fi

# 16. Test removing existing replica, should fail when --force is not used
echo "Testing 'eos root://${EOS_INSTANCE} evict --fsid ${FSID_DUMMY_1_VALUE} ${TEMP_FILE}'... without '--force'"
KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos root://${EOS_INSTANCE} evict --fsid ${FSID_DUMMY_1_VALUE} ${TEMP_FILE}
if [ $? -eq 0 ]; then
  error "'eos evict' command succeeded where it should have failed"
else
  echo "'eos evict' command failed as expected"
fi

# 17. Test removing one existing replica, should succeed and keep remaining replicas intact
echo "Trying to remove a replica with existing fsid ${FSID_DUMMY_1_VALUE} ${TEMP_FILE} and --force..."
KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos root://${EOS_INSTANCE} evict --force --fsid ${FSID_DUMMY_1_VALUE} ${TEMP_FILE}
if [ $? -ne 0 ]; then
  error "'eos evict --force --fsid ${FSID_DUMMY_1_VALUE}' command failed where it should have succeeded"
else
  echo "'eos evict --force --fsid ${FSID_DUMMY_1_VALUE}' command succeeded as expected"
fi
if test 3 != "$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos --json root://${EOS_INSTANCE} info ${TEMP_FILE} | jq -r '.locations[] | .schedgroup' | wc -l)"; then
  error "The number of replicas is not the expected one"
else
  echo "Disk replica count is as expected"
fi

# 18. Test removing one existing replica, should succeed and keep remaining replicas intact
echo "Trying to remove a replica with existing fsid ${FSID_DUMMY_2_VALUE} ${TEMP_FILE} and -f..."
KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos root://${EOS_INSTANCE} evict -f --fsid ${FSID_DUMMY_2_VALUE} ${TEMP_FILE}
if [ $? -ne 0 ]; then
  error "'eos evict -f --fsid ${FSID_DUMMY_2_VALUE}' command failed where it should have succeeded"
else
  echo "'eos evict -f --fsid ${FSID_DUMMY_2_VALUE}' command succeeded as expected"
fi
if test 2 != "$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos --json root://${EOS_INSTANCE} info ${TEMP_FILE} | jq -r '.locations[] | .schedgroup' | wc -l)"; then
  error "The number of replicas is not the expected one"
else
  echo "Disk replica count is as expected"
fi

# 19. Test removing all remaining replicas
echo "Trying to remove all disk replicas..."
KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos root://${EOS_INSTANCE} evict --force ${TEMP_FILE}
if [ $? -ne 0 ]; then
  error "'eos evict' command failed where it should have succeeded"
else
  echo "'eos evict' command succeeded as expected"
fi
if test 1 != "$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos --json root://${EOS_INSTANCE} info ${TEMP_FILE} | jq -r '.locations[] | .schedgroup' | wc -l)"; then
  error "The number of replicas is not the expected one"
else
  echo "Disk replica count is as expected"
fi
if test 1 != "$(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos --json root://${EOS_INSTANCE} info ${TEMP_FILE} | jq -r '.locations[] | select(.schedgroup | startswith("tape")) | .schedgroup' | wc -l)"; then
  error "The tape replica was removed, when it should have not"
else
  echo "Tape replica was preserved"
fi

################################################################
# Finalize
################################################################

echo
echo "OK: all tests passed"
