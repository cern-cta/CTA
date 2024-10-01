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



TEST_DIR=${EOS_BASEDIR}
TEST_FILE_NAME=`uuidgen`

#
# Check we can copy zero-length files into the namespace by touch and copy
#
echo "eos root://${EOSINSTANCE} touch ${TEST_DIR}${TEST_FILE_NAME}.touch"
eos root://${EOSINSTANCE} touch ${TEST_DIR}${TEST_FILE_NAME}.touch
echo "eos root://${EOSINSTANCE} cp ${TEST_DIR}${TEST_FILE_NAME}.touch /tmp/${TEST_FILE_NAME}.touch"
eos root://${EOSINSTANCE} cp ${TEST_DIR}${TEST_FILE_NAME}.touch /tmp/${TEST_FILE_NAME}.touch
echo "eos root://${EOSINSTANCE} cp /tmp/${TEST_FILE_NAME}.touch ${TEST_DIR}${TEST_FILE_NAME}.zero"
eos root://${EOSINSTANCE} cp /tmp/${TEST_FILE_NAME}.touch ${TEST_DIR}${TEST_FILE_NAME}.zero
echo "eos root://${EOSINSTANCE} cp ${TEST_DIR}${TEST_FILE_NAME}.zero /tmp/${TEST_FILE_NAME}.zero"
eos root://${EOSINSTANCE} cp ${TEST_DIR}${TEST_FILE_NAME}.zero /tmp/${TEST_FILE_NAME}.zero

if [ -f /tmp/${TEST_FILE_NAME}.touch -a ! -s /tmp/${TEST_FILE_NAME}.touch -a -f /tmp/${TEST_FILE_NAME}.zero -a ! -s /tmp/${TEST_FILE_NAME}.zero ]; then
  echo "Zero-length file copy succeeded"
  zeroLengthTests=1
else
  echo "Zero-length file copy failed"
  zeroLengthTests=0
fi

# Clean up
echo "eos root://${EOSINSTANCE} rm ${TEST_DIR}${TEST_FILE_NAME}.touch"
eos root://${EOSINSTANCE} rm ${TEST_DIR}${TEST_FILE_NAME}.touch
echo "eos root://${EOSINSTANCE} rm ${TEST_DIR}${TEST_FILE_NAME}.zero"
eos root://${EOSINSTANCE} rm ${TEST_DIR}${TEST_FILE_NAME}.zero
rm -f /tmp/${TEST_FILE_NAME}.touch /tmp/${TEST_FILE_NAME}.zero

# Report results
msgNum=$(grep "\"File suc" /mnt/logs/tpsrv*/taped/cta/cta-taped.log | grep ${TEST_FILE_NAME} | tail -n 4 | wc -l)
if [ "$msgNum" = "4" ] && [ $zeroLengthTests -eq 1 ]; then
  echo "OK: all tests passed"
  #rc=0
  exit 0
else
  echo "FAIL: tests failed"
  #rc=1
  exit 1
fi
