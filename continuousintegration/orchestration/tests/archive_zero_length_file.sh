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
#   - This script checks that the archival of a 0-length file fails.
#   - For it to work, the configuration 'cta.archivefile.disallow_zero_length'
#   must be set to 'on'.
#
################################################################################

EOSINSTANCE=ctaeos
CTA_TEST_DIR=/eos/ctaeos/cta
TEST_FILE_NAME="empty_file"

eosadmin_kdestroy
eosadmin_kinit

echo "Trying to archive a 0-length"

err_msg_file=$(mktemp)
touch /etc/${TEST_FILE_NAME}
echo "xrdcp /etc/${TEST_FILE_NAME} root://${EOSINSTANCE}/${CTA_TEST_DIR}/${TEST_FILE_NAME}"
xrdcp /etc/${TEST_FILE_NAME} root://${EOSINSTANCE}/${CTA_TEST_DIR}/${TEST_FILE_NAME} 2>&1 | sed 's/[^[:print:]\t]//g' | tee "${err_msg_file}"
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
