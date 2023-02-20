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

EOSINSTANCE=ctaeos
TEST_FILE_NAME=`uuidgen`

# get some common useful helpers for krb5
. /root/client_helper.sh

eospower_kdestroy
eospower_kinit

echo "xrdcp /etc/group root://${EOSINSTANCE}//eos/ctaeos/cta/${TEST_FILE_NAME}"
xrdcp /etc/group root://${EOSINSTANCE}//eos/ctaeos/cta/${TEST_FILE_NAME}

SECONDS_PASSED=0
WAIT_FOR_ARCHIVED_FILE_TIMEOUT=90
while test 0 = `eos root://${EOSINSTANCE} info /eos/ctaeos/cta/${TEST_FILE_NAME} | awk '{print $4;}' | grep tape | wc -l`; do
  echo "Waiting for file to be archived to tape: Seconds passed = ${SECONDS_PASSED}"
  sleep 1
  let SECONDS_PASSED=SECONDS_PASSED+1

  if test ${SECONDS_PASSED} == ${WAIT_FOR_ARCHIVED_FILE_TIMEOUT}; then
    echo "Timed out after ${WAIT_FOR_ARCHIVED_FILE_TIMEOUT} seconds waiting for file to be archived to tape"
    exit 1
  fi
done

echo
echo "FILE ARCHIVED TO TAPE"
echo
eos root://${EOSINSTANCE} info /eos/ctaeos/cta/${TEST_FILE_NAME}
echo
echo "Information about the testing file:"
echo "********"
  eos root://${EOSINSTANCE} attr ls /eos/ctaeos/cta/${TEST_FILE_NAME}
  eos root://${EOSINSTANCE} ls -l /eos/ctaeos/cta/${TEST_FILE_NAME}
  eos root://${EOSINSTANCE} info /eos/ctaeos/cta/${TEST_FILE_NAME}
echo
echo "Removing disk replica as poweruser1:powerusers (12001:1200)"
# XrdSecPROTOCOL=sss eos -r 12001 1200 root://${EOSINSTANCE} file drop /eos/ctaeos/cta/${TEST_FILE_NAME} 1
XrdSecPROTOCOL=sss eos -r 0 0 root://${EOSINSTANCE} file drop /eos/ctaeos/cta/${TEST_FILE_NAME} 1
echo
echo "Information about the testing file without disk replica"
  eos root://${EOSINSTANCE} ls -l /eos/ctaeos/cta/${TEST_FILE_NAME}
  eos root://${EOSINSTANCE} info /eos/ctaeos/cta/${TEST_FILE_NAME}
echo
echo "Trigerring EOS retrieve workflow as poweruser1:powerusers (12001:1200)"
#echo "XrdSecPROTOCOL=sss xrdfs ${EOSINSTANCE} prepare -s \"/eos/ctaeos/cta/${TEST_FILE_NAME}?eos.ruid=12001&eos.rgid=1200\""
#  XrdSecPROTOCOL=sss xrdfs ${EOSINSTANCE} prepare -s "/eos/ctaeos/cta/${TEST_FILE_NAME}?eos.ruid=12001&eos.rgid=1200"

# We need the -s as we are staging the files from tape (see xrootd prepare definition)
KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOSINSTANCE} prepare -s /eos/ctaeos/cta/${TEST_FILE_NAME}

# Wait for the copy to appear on disk
SECONDS_PASSED=0
WAIT_FOR_RETRIEVED_FILE_TIMEOUT=90
while test 0 = `eos root://${EOSINSTANCE} info /eos/ctaeos/cta/${TEST_FILE_NAME} | awk '{print $4;}' | grep -F "default.0" | wc -l`; do
  echo "Waiting for file to be retrieved from tape: Seconds passed = ${SECONDS_PASSED}"
  sleep 1
  let SECONDS_PASSED=SECONDS_PASSED+1

  if test ${SECONDS_PASSED} == ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT}; then
    echo "Timed out after ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT} seconds waiting for file to be retrieved from tape"
    exit 1
  fi
done
echo
echo "FILE RETRIEVED FROM DISK"
echo
echo "Information about the testing file:"
echo "********"
  eos root://${EOSINSTANCE} attr ls /eos/ctaeos/cta/${TEST_FILE_NAME}
  eos root://${EOSINSTANCE} ls -l /eos/ctaeos/cta/${TEST_FILE_NAME}
  eos root://${EOSINSTANCE} info /eos/ctaeos/cta/${TEST_FILE_NAME}

# Delete the file so it doesn't interfere with tests in client_ar.sh
echo "eos root://${EOSINSTANCE} rm /eos/ctaeos/cta/${TEST_FILE_NAME}"
eos root://${EOSINSTANCE} rm /eos/ctaeos/cta/${TEST_FILE_NAME}

#
# Check we can copy zero-length files into the namespace by touch and copy
#
echo "eos root://${EOSINSTANCE} touch /eos/ctaeos/cta/${TEST_FILE_NAME}.touch"
eos root://${EOSINSTANCE} touch /eos/ctaeos/cta/${TEST_FILE_NAME}.touch
echo "eos root://${EOSINSTANCE} cp /eos/ctaeos/cta/${TEST_FILE_NAME}.touch /tmp/${TEST_FILE_NAME}.touch"
eos root://${EOSINSTANCE} cp /eos/ctaeos/cta/${TEST_FILE_NAME}.touch /tmp/${TEST_FILE_NAME}.touch
echo "eos root://${EOSINSTANCE} cp /tmp/${TEST_FILE_NAME}.touch /eos/ctaeos/cta/${TEST_FILE_NAME}.zero"
eos root://${EOSINSTANCE} cp /tmp/${TEST_FILE_NAME}.touch /eos/ctaeos/cta/${TEST_FILE_NAME}.zero
echo "eos root://${EOSINSTANCE} cp /eos/ctaeos/cta/${TEST_FILE_NAME}.zero /tmp/${TEST_FILE_NAME}.zero"
eos root://${EOSINSTANCE} cp /eos/ctaeos/cta/${TEST_FILE_NAME}.zero /tmp/${TEST_FILE_NAME}.zero

if [ -f /tmp/${TEST_FILE_NAME}.touch -a ! -s /tmp/${TEST_FILE_NAME}.touch -a -f /tmp/${TEST_FILE_NAME}.zero -a ! -s /tmp/${TEST_FILE_NAME}.zero ]; then
  echo "Zero-length file copy succeeded"
  zeroLengthTests=1
else
  echo "Zero-length file copy failed"
  zeroLengthTests=0
fi
# Clean up
echo "eos root://${EOSINSTANCE} rm /eos/ctaeos/cta/${TEST_FILE_NAME}.touch"
eos root://${EOSINSTANCE} rm /eos/ctaeos/cta/${TEST_FILE_NAME}.touch
echo "eos root://${EOSINSTANCE} rm /eos/ctaeos/cta/${TEST_FILE_NAME}.zero"
eos root://${EOSINSTANCE} rm /eos/ctaeos/cta/${TEST_FILE_NAME}.zero
rm -f /tmp/${TEST_FILE_NAME}.touch /tmp/${TEST_FILE_NAME}.zero

# Report results
msgNum=$(grep "\"File suc" /mnt/logs/tpsrv*/taped/cta/cta-taped.log | grep ${TEST_FILE_NAME} | tail -n 4 | wc -l)
if [ "$msgNum" = "4" -a $zeroLengthTests -eq 1 ]; then
  echo "OK: all tests passed"
    rc=0
else
  echo "FAIL: tests failed"
    rc=1
fi
