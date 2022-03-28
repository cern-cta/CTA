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

# this script retrieves all files from a given EOS directory that are d0::t1



EOS_MGM_URL=root://eosctatape.cern.ch
TEST_FILE_NAME=/var/tmp/cta-test-temporary-file-$$
TEST_FILE_LIST=/var/tmp/cta-test-temporary-list-$$
TEMPORARY_PID_FILE=/var/tmp/cta-test-temporary-pid-file-$$
EOS_KEYTAB=/var/tmp/cta-test-temporary-kt-$$

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

eos $EOS_MGM_URL ls -y $EOS_DIR > $TEST_FILE_LIST.all || exit -1
grep ^d0::t1 $TEST_FILE_LIST.all | awk "{print \"$EOS_DIR/\" \$NF}" > $TEST_FILE_LIST
/usr/bin/sed -e 's/^/ls -y /' $TEST_FILE_LIST > $TEST_FILE_LIST.eosh # Prepare the file for EOS batch mode
/usr/bin/rm -f $TEST_FILE_LIST.all

FILES_COUNT=`cat $TEST_FILE_LIST | wc -l`
/usr/bin/date "+%Y-%m-%d %H:%M:%S"
/usr/bin/echo -e "***\n*** Files to be recalled: $FILES_COUNT \n***"


# Retrieve files from tape (using here hard coded CTA keytab - THIS IS NOT SAFE !!!)
/usr/bin/date "+%Y-%m-%d %H:%M:%S"
/usr/bin/echo -e "***\n*** Retrieving tape replicas for $FILES_COUNT files in the $EOS_DIR directory from $TEST_FILE_LIST file list onto EOS disk ...\n***"
echo "0 u:cta g:cta n:cta-taped N:6425591835858577167 c:1496074683 e:0 f:0 k:b7825f9dd4d72952e429e342dd687aa8735411c29587ddd052613e33c0792e0b" > $EOS_KEYTAB
chmod 400 $EOS_KEYTAB
/usr/bin/xargs -a $TEST_FILE_LIST -I {} echo "{}?eos.ruid=$USER_ID&eos.rgid=$GROUP_ID" | XrdSecPROTOCOL=sss XrdSecSSSKT=$EOS_KEYTAB /usr/bin/xargs -n 5 -P50 /usr/bin/xrdfs eosctatape prepare -s


# Wait $TIMEOUT for files until they are recalled
/usr/bin/date "+%Y-%m-%d %H:%M:%S"
/usr/bin/echo -e "***\n*** Waiting until the files are retrieved from tape (expecting d1::t1 in front of each file entry using the EOS ls -y command), timeout after $TIMEOUT seconds ...\n***"
FILES_TO_BE_RETRIEVED=1
SECONDS_PASSED=0
while [ "$FILES_TO_BE_RETRIEVED" -gt 0 ]
do
#  FILES_TO_BE_RETRIEVED=`/usr/bin/xargs -a $TEST_FILE_LIST -L 1 -P 10 -I {} /usr/bin/eos ls -y {} | /usr/bin/grep -v '^d1::t1' -c`
  FILES_TO_BE_RETRIEVED=`/usr/bin/eos --batch $EOS_MGM_URL $TEST_FILE_LIST.eosh | /usr/bin/grep '^d0::t1' -c`
  CURRENT_TIME=`/usr/bin/date "+%Y-%m-%d %H:%M:%S"`
  echo "$CURRENT_TIME Scanned $EOS_DIR using file list $TEST_FILE_LIST - files to be retrieved: $FILES_TO_BE_RETRIEVED"
  sleep $SLEEP
  let SECONDS_PASSED=$SECONDS_PASSED+$SLEEP
  if [ "$SECONDS_PASSED" -gt "$TIMEOUT" ]
  then
    echo "$0: timing out after $TIMEOUT seconds, stopping the test."
    exit -1
  fi
done

# Cleanup
/usr/bin/date "+%Y-%m-%d %H:%M:%S"
# /usr/bin/rm -f $TEST_FILE_NAME $TEST_FILE_LIST $TEST_FILE_LIST.eosh $EOS_KEYTAB

exit 0
