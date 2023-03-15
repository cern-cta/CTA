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
TEST_DIR=/eos/ctaeos/cta/

# get some common useful helpers for krb5
. /root/client_helper.sh

eospower_kdestroy
eospower_kinit

cat <<EOF > /opt/run/bin/tracker_simple.schema

CREATE TABLE client_simple_tests_${TEST_FILE_NAME}(
       filename TEXT PRIMARY KEY,
       archived INTEGER DEFAULT 1,
       staged   INTEGER DEFAULT 0,
       deleted  INTEGER DEFAULT 0,
       evicted  INTEGER DEFAULT 0
);
EOF

sqlite3 /root/trackerdb_simple.db < /opt/run/bin/tracker_simple.schema
export DB_NAME="/root/trackerdb_simple.db"
export TEST_TABLE="client_simple_tests_${TEST_FILE_NAME}"


db_insert ${TEST_FILE_NAME}

echo "xrdcp /etc/group root://${EOSINSTANCE}/${TEST_DIR}${TEST_FILE_NAME}"
xrdcp /etc/group root://${EOSINSTANCE}/${TEST_DIR}${TEST_FILE_NAME}

wait_for_archive ${EOSINSTANCE} "${TEST_DIR}${TEST_FILE_NAME}"

echo
echo "FILE ARCHIVED TO TAPE"
echo
eos root://${EOSINSTANCE} info ${TEST_DIR}${TEST_FILE_NAME}
echo "Updating test DB"
db_update 'archived' ${TEST_FILE_NAME} 1 '='
db_info '*'

echo
echo "Information about the testing file:"
echo "********"
  eos root://${EOSINSTANCE} attr ls ${TEST_DIR}${TEST_FILE_NAME}
  eos root://${EOSINSTANCE} ls -l ${TEST_DIR}${TEST_FILE_NAME}
eos root://${EOSINSTANCE} info ${TEST_DIR}${TEST_FILE_NAME}

echo
echo "Removing disk replica as poweruser1:powerusers (12001:1200)"
# XrdSecPROTOCOL=sss eos -r 12001 1200 root://${EOSINSTANCE} file drop /eos/ctaeos/cta/${TEST_FILE_NAME} 1
XrdSecPROTOCOL=sss eos -r 0 0 root://${EOSINSTANCE} file drop "${TEST_DIR}${TEST_FILE_NAME}" 1


echo
echo "Information about the testing file without disk replica"
  eos root://${EOSINSTANCE} ls -l ${TEST_DIR}${TEST_FILE_NAME}
  eos root://${EOSINSTANCE} info ${TEST_DIR}${TEST_FILE_NAME}


echo
echo "Trigerring EOS retrieve workflow as poweruser1:powerusers (12001:1200)"
#echo "XrdSecPROTOCOL=sss xrdfs ${EOSINSTANCE} prepare -s \"/eos/ctaeos/cta/${TEST_FILE_NAME}?eos.ruid=12001&eos.rgid=1200\""
#  XrdSecPROTOCOL=sss xrdfs ${EOSINSTANCE} prepare -s "/eos/ctaeos/cta/${TEST_FILE_NAME}?eos.ruid=12001&eos.rgid=1200"

# We need the -s as we are staging the files from tape (see xrootd prepare definition)
KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOSINSTANCE} prepare -s ${TEST_DIR}${TEST_FILE_NAME}

# Wait for the copy to appear on disk
wait_for_retrieve ${EOSINSTANCE} "${TEST_DIR}${TEST_FILE_NAME}"
db_update "staged" ${TEST_FILE_NAME} 1 "+"
db_info "*"

echo
echo "FILE RETRIEVED FROM DISK"
echo
echo "Information about the testing file:"
echo "********"
  eos root://${EOSINSTANCE} attr ls ${TEST_DIR}${TEST_FILE_NAME}
  eos root://${EOSINSTANCE} ls -l ${TEST_DIR}${TEST_FILE_NAME}
  eos root://${EOSINSTANCE} info ${TEST_DIR}${TEST_FILE_NAME}

# Delete the file so it doesn't interfere with tests in client_ar.sh
echo "eos root://${EOSINSTANCE} rm ${TEST_DIR}${TEST_FILE_NAME}"
eos root://${EOSINSTANCE} rm ${TEST_DIR}${TEST_FILE_NAME}
db_update "archived" ${TEST_FILE_NAME} 1 "-"
db_update "staged" ${TEST_FILE_NAME} 1 "-"
db_update "deleted" ${TEST_FILE_NAME} 1 "+"
db_info "filename, evicted, deleted"


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
if [ "$msgNum" = "4" -a $zeroLengthTests -eq 1 ]; then
  echo "OK: all tests passed"
    rc=0
else
  echo "FAIL: tests failed"
    rc=1
fi
