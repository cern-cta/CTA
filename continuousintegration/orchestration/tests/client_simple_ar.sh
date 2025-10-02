#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
#
# SPDX-License-Identifier: GPL-3.0-or-later


EOS_MGM_HOST="ctaeos"
TEST_FILE_NAME=$(uuidgen | sed 's/-//g')
TEST_DIR=/eos/ctaeos/cta

# get some common useful helpers for krb5
eospower_kdestroy
eospower_kinit

echo "xrdcp /etc/group root://${EOS_MGM_HOST}/${TEST_DIR}/${TEST_FILE_NAME}"
xrdcp /etc/group root://${EOS_MGM_HOST}/${TEST_DIR}/${TEST_FILE_NAME}

wait_for_archive ${EOS_MGM_HOST} "${TEST_DIR}/${TEST_FILE_NAME}"

echo
echo "FILE ARCHIVED TO TAPE"
echo
eos root://${EOS_MGM_HOST} info ${TEST_DIR}/${TEST_FILE_NAME}

echo
echo "Information about the testing file:"
echo "********"
  eos root://${EOS_MGM_HOST} attr ls ${TEST_DIR}/${TEST_FILE_NAME}
  eos root://${EOS_MGM_HOST} ls -l ${TEST_DIR}/${TEST_FILE_NAME}
  eos root://${EOS_MGM_HOST} info ${TEST_DIR}/${TEST_FILE_NAME}

echo
echo "Removing disk replica as poweruser1:powerusers (12001:1200)"
KRB5CCNAME=/tmp/${EOS_POWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos -r 0 0 root://${EOS_MGM_HOST} file drop "${TEST_DIR}/${TEST_FILE_NAME}" 1


echo
echo "Information about the testing file without disk replica"
  eos root://${EOS_MGM_HOST} ls -l ${TEST_DIR}/${TEST_FILE_NAME}
  eos root://${EOS_MGM_HOST} info ${TEST_DIR}/${TEST_FILE_NAME}


echo
echo "Trigerring EOS retrieve workflow as poweruser1:powerusers (12001:1200)"
# We need the -s as we are staging the files from tape (see xrootd prepare definition)
KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_MGM_HOST} prepare -s ${TEST_DIR}/${TEST_FILE_NAME}

# Wait for the copy to appear on disk
wait_for_retrieve ${EOS_MGM_HOST} "${TEST_DIR}/${TEST_FILE_NAME}"

echo
echo "FILE RETRIEVED FROM DISK"
echo
echo "Information about the testing file:"
echo "********"
  eos root://${EOS_MGM_HOST} attr ls ${TEST_DIR}/${TEST_FILE_NAME}
  eos root://${EOS_MGM_HOST} ls -l ${TEST_DIR}/${TEST_FILE_NAME}
  eos root://${EOS_MGM_HOST} info ${TEST_DIR}/${TEST_FILE_NAME}

# Delete the file so it doesn't interfere with tests in client_ar.sh
echo "eos root://${EOS_MGM_HOST} rm ${TEST_DIR}/${TEST_FILE_NAME}"
eos root://${EOS_MGM_HOST} rm ${TEST_DIR}/${TEST_FILE_NAME}
