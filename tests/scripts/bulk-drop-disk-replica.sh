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

# this script deletes all disk replicas from a given EOS directory that are dX::t1 ,,X>=1



EOS_MGM_URL=root://eosctatape.cern.ch
TEST_FILE_NAME=/var/tmp/cta-test-temporary-file-$$
TEST_FILE_LIST=/var/tmp/cta-test-temporary-list-$$
TEMPORARY_PID_FILE=/var/tmp/cta-test-temporary-pid-file-$$
EOS_KEYTAB=/var/tmp/cta-test-temporary-kt-$$
TAPE_FS_ID=65535

TIMEOUT=72000 # pump up da jam

SLEEP=15

export XRD_STREAMTIMEOUT=3600     # increased from 60s
export XRD_TIMEOUTRESOLUTION=3600 # increased from 15s

# Usage
if [ "" == "$1" ]
then
  echo "Usage: $0 <eosdirectory>"
  exit -1
fi

EOS_DIR=$1

umask 077

USER_ID=`/usr/bin/id -u`
GROUP_ID=`/usr/bin/id -g`


echo "EOS directory: $EOS_DIR, User ID: $USER_ID, Group ID: $GROUP_ID, Test file name: $TEST_FILE_NAME, Test file list: $TEST_FILE_LIST"



# Generate file list
/usr/bin/date "+%Y-%m-%d %H:%M:%S"
/usr/bin/echo -e "***\n*** Creating file list $TEST_FILE_LIST for files in $EOS_DIR directory ...\n***"
/usr/bin/rm -f $TEST_FILE_LIST

eos $EOS_MGM_URL ls -y $EOS_DIR > $TEST_FILE_LIST.tmp || exit -1
grep -v ^d0:: $TEST_FILE_LIST.tmp | grep -v '^\<d.::t0\>' | awk "{print \"$EOS_DIR/\" \$NF}" > $TEST_FILE_LIST
/usr/bin/rm -f $TEST_FILE_LIST.tmp

FILES_COUNT=`cat $TEST_FILE_LIST | wc -l`
/usr/bin/date "+%Y-%m-%d %H:%M:%S"
/usr/bin/echo -e "***\n*** Files for which to drop disk copy: $FILES_COUNT \n***"

for FILE_PATH in `cat $TEST_FILE_LIST`; do
  for DISK_FSID in `eos file info "${FILE_PATH}" -m | sed s/\ /'\n'/g | grep fsid | sed s/fsid=// | grep -v ${TAPE_FS_ID}`; do
     #echo "deleting disk replica with fsid ${DISK_FSID} for ${FILE_PATH}"
    if ! eos -r 0 0 file drop "${FILE_PATH}" ${DISK_FSID} >/dev/null; then
      echo "failed to delete disk replica with fsid ${DISK_FSID} for ${FILE_PATH}"
    fi
  done
done


# Cleanup
/usr/bin/date "+%Y-%m-%d %H:%M:%S"
# /usr/bin/rm -f $TEST_FILE_NAME $TEST_FILE_LIST

exit 0
