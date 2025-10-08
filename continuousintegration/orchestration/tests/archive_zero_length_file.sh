#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later



################################################################################
# DESCRIPTION
#
#   - This script checks that the archival of a 0-length file fails.
#   - For it to work, the configuration 'cta.archivefile.zero_length_files_forbidden'
#   must be set to 'on'.
#
################################################################################

EOS_MGM_HOST="ctaeos"
CTA_TEST_DIR=/eos/ctaeos/cta
TEST_FILE_NAME="empty_file"

# get Krb5 credentials
. /root/client_helper.sh
eosadmin_kdestroy
eosadmin_kinit

echo "Trying to archive a 0-length"

err_msg_file=$(mktemp)
touch /etc/${TEST_FILE_NAME}
echo "xrdcp /etc/${TEST_FILE_NAME} root://${EOS_MGM_HOST}/${CTA_TEST_DIR}/${TEST_FILE_NAME}"
xrdcp /etc/${TEST_FILE_NAME} root://${EOS_MGM_HOST}/${CTA_TEST_DIR}/${TEST_FILE_NAME} 2>&1 | sed 's/[^[:print:]\t]//g' | tee "${err_msg_file}"
if [ "${PIPESTATUS[0]}" -eq 0 ]
then
  echo "xrdcp command succeeded where it should have failed"
  exit 1
elif test 0 == "$(grep -c -i "0-length" < "${err_msg_file}")"
then
  echo "xrdcp command failed, but with unexpected error: $(cat "${err_msg_file}")"
  exit 1
else
  echo "xrdcp command failed as expected with error: $(cat "${err_msg_file}")"
fi
rm -f "${err_msg_file}"

echo
echo "OK: all tests passed"
exit 0
