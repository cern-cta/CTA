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

set -e

################################################################################
# DESCRIPTION
#
#   - This tests exists to make sure that tape-enabled files with no tape copy
#   cannot be evicted from disk.
#
# EXPECTED BEHAVIOUR
#
#   # The following steps will be carried:
#
#   1. Put the destination tape drives down.
#   2. Write a file to EOSCTA for archiving.
#   3. Execute eos root://MGM_HOST stagerrm PATH.
#   4. Test the exit code of the command is failure.
#   5. Test the disk replica still exists.
#   6. Execute xrdfs MGM_HOST prepare -e PATH.
#   7. Test the exit code of the command is failure.
#   8. Test the disk replica still exists.
#   9. Put the destination tape drives up.
#   10. Test the file is successfully archived to tape.
#
################################################################################

EOS_MGM_HOST="ctaeos"

EOS_BASEDIR=/eos/ctaeos
EOS_TAPE_BASEDIR=$EOS_BASEDIR/cta

# get some common useful helpers for krb5
. /root/client_helper.sh

eospower_kdestroy &>/dev/null || true
eospower_kinit &>/dev/null

admin_kdestroy &>/dev/null || true
admin_kinit &>/dev/null

error()
{
  echo "ERROR: $*" >&2
  exit 1
}

################################################################
# Test deleting file before tape copy has been generated
################################################################

# 1. Put the destination tape drives down
TEMP_FILE=${EOS_TAPE_BASEDIR}/$(uuidgen)
echo
echo "Putting all drives down. No file will be written to tape..."
put_all_drives_down

# 2. Write a file to EOSCTA for archiving
echo "Write file ${TEMP_FILE} for archival..."
xrdcp /etc/group root://${EOS_MGM_HOST}/${TEMP_FILE}

# 3/4. Check that stagerrm fails
echo "Testing 'eos root://${EOS_MGM_HOST} stagerrm ${TEMP_FILE}'..."
if KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos root://${EOS_MGM_HOST} stagerrm ${TEMP_FILE}; then
  error "eos stagerrm command succeeded where it should have failed"
else
  echo "eos stagerrm command failed as expected"
fi

# 5. Check that disk replica still exists
echo "Checking that ${TEMP_FILE} replica still exists on disk..."
if test 0 == $(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos root://${EOS_MGM_HOST} info ${TEMP_FILE} | grep -F "default.0" | wc -l); then
  error "eos stagerrm removed disk replica, when it should have failed"
else
  echo "eos stagerrm did not remove disk replica, as expected"
fi

# 6/7. Check that prepare -e fails
echo "Testing 'xrdfs root://${EOS_MGM_HOST} prepare -e ${TEMP_FILE}'..."

if KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs root://${EOS_MGM_HOST} prepare -e ${TEMP_FILE}; then
  #error "prepare -e command succeeded where it should have failed"
  # 'prepare -e' will not return an error because WFE errors are not propagated to the user. Therefore, we ignore this check.
  :
else
  echo "prepare -e stagerrm command failed as expected"
fi

# 8. Check that disk replica still exists
echo "Checking that ${TEMP_FILE} replica still exists on disk..."
if test 0 == $(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos root://${EOS_MGM_HOST} info ${TEMP_FILE} | grep -F "default.0" | wc -l); then
  error "prepare -e removed disk replica, when it should have failed"
else
  echo "prepare -e did not remove disk replica, as expected"
fi

# 9. Put the destination tape drives up and wait for archival
echo "Putting all drives up. File will finally be written to tape and removed from disk..."
put_all_drives_up

echo "Waiting for archival of ${TEMP_FILE}..."
wait_for_archive ${TEMP_FILE}

# 10. Check that disk replica was deleted and that new tape replica exists
echo "Checking that ${TEMP_FILE} replica no longer exists on disk..."
if test 0 != $(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos root://${EOS_MGM_HOST} info ${TEMP_FILE} | grep -F "default.0" | wc -l); then
  error "Disk replica not removed, when it should have been done after archival"
else
  echo "Disk replica was removed, as expected"
fi

echo "Checking that ${TEMP_FILE} replica no longer exists on disk..."
if test 0 == $(KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos root://${EOS_MGM_HOST} info ${TEMP_FILE} | grep -F "tape" | wc -l); then
  error "Tape replica does not exist, when it should have been created after archival"
else
  echo "Tape replica creted, as expected"
fi


################################################################
# Finalize
################################################################

echo
echo "OK: all tests passed"
