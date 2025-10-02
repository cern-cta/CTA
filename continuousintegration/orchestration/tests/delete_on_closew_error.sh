#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
#
# SPDX-License-Identifier: GPL-3.0-or-later



################################################################################
# DESCRIPTION
#
#   - This script checks the delete-on-close event, triggered if archiving fails
#   before the archive request is queued.
#   - After this event there should be no replicas of the file to be found, and
#   the namespace entry should not exist.
#   - Since this happens before the CLOSEW event, the file should be deleted
#   without any archive request being queued.
#
################################################################################


EOS_MGM_HOST="ctaeos"
CTA_TEST_DIR=/eos/ctaeos/cta

cleanup()
{
  echo "Deleting ${CTA_TEST_DIR}/fail_on_closew_test"
  eosadmin_eos root://${EOS_MGM_HOST} rm -rf ${CTA_TEST_DIR}/fail_on_closew_test
}

error()
{
  echo "ERROR: $*" >&2
  cleanup
  exit 1
}

# get Krb5 credentials
. /root/client_helper.sh
eosadmin_kdestroy
eosadmin_kinit


################################################################################
# Testing failing CLOSEW - fail_on_closew_test
################################################################################

# The storage class 'fail_on_closew_test' attribute causes the 'delete-on-close'
#   event to be automatically triggered when a file is written to that directory.
# This can be used to test the 'delete-on-close' event.

TEST_FILE_NAME=$(uuidgen)

# Create a subdirectory with a different storage class for the delete on CLOSEW test

echo "Creating ${CTA_TEST_DIR}/fail_on_closew_test event"
eosadmin_eos root://${EOS_MGM_HOST} mkdir ${CTA_TEST_DIR}/fail_on_closew_test || error "Failed to create directory ${CTA_TEST_DIR}/fail_on_closew_test"
eosadmin_eos root://${EOS_MGM_HOST} attr set sys.archive.storage_class=fail_on_closew_test ${CTA_TEST_DIR}/fail_on_closew_test || error "Failed to set sys.archive.storage_class=fail_on_closew_test on ${CTA_TEST_DIR}/fail_on_closew_test"

echo "xrdcp /etc/group root://${EOS_MGM_HOST}/${CTA_TEST_DIR}/fail_on_closew_test/${TEST_FILE_NAME}"
xrdcp /etc/group root://${EOS_MGM_HOST}/${CTA_TEST_DIR}/fail_on_closew_test/${TEST_FILE_NAME}
if [ $? -eq 0 ]
then
  error "xrdcp command succeeded where it should have failed"
else
  echo "xrdcp command failed as expected"
fi

# Since CTA Frontend will fail on CLOSEW, EOS should delete the file

# Check that the EOS namespace entry has been removed
# This means all replicas are deleted from tape and disks

eos root://${EOS_MGM_HOST} fileinfo ${CTA_TEST_DIR}/fail_on_closew_test/${TEST_FILE_NAME}
if [ $? -eq 2 ]
then
  echo "Success: EOS namespace entry was removed"
else
  error "EOS namespace entry is still present"
fi


################################################################################
# Cleanup and finalize
################################################################################

echo
cleanup
echo "OK: all tests passed"
exit 0
