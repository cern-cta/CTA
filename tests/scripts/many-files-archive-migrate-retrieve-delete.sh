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

FILES_COUNT=1000
EOS_DIR=/eos/dev/test/`whoami`/`/usr/bin/date +%Y%m%d%H%M`
EOS_MGM_URL=root://eosctatape.cern.ch
TEST_FILE_NAME=/var/tmp/cta-test-temporary-file-$$
TEST_FILE_LIST=/var/tmp/cta-test-temporary-list-$$
TEMPORARY_PID_FILE=/var/tmp/cta-test-temporary-pid-file
EOS_KEYTAB=/var/tmp/cta-test-temporary-kt-$$
TIMEOUT=7200
SLEEP=15

export XRD_STREAMTIMEOUT=3600     # increased from 60s
export XRD_TIMEOUTRESOLUTION=3600 # increased from 15s

# Usage
if ! [[ "0" -lt "$1" ]]
then
  echo "Usage: $0 <filesize in bytes>"
  exit -1
fi

TEST_FILE_SIZE=$1

umask 077

USER_ID=`/usr/bin/id -u`
GROUP_ID=`/usr/bin/id -g`

# Generate random file (make smaller (< 1 GB) files ASCII hence compressible (size not accurate though))
#/usr/bin/rm -f $TEST_FILE_NAME
#if [ "$TEST_FILE_SIZE" -lt "1000000000" ]
#then
#  /usr/bin/openssl rand $TEST_FILE_SIZE -base64 -out $TEST_FILE_NAME
#else
  /usr/bin/openssl rand $TEST_FILE_SIZE -out $TEST_FILE_NAME
#fi

#echo "EOS directory: $EOS_DIR, File size: $TEST_FILE_SIZE, Files count: $FILES_COUNT, User ID: $USER_ID, Group ID: $GROUP_ID, Test file name: $TEST_FILE_NAME, Test file list: $TEST_FILE_LIST"

# Generate file list
/usr/bin/date "+%Y-%m-%d %H:%M:%S"
/usr/bin/echo -e "***\n*** Creating file list $TEST_FILE_LIST for $FILES_COUNT files in $EOS_DIR directory ...\n***"
/usr/bin/rm -f $TEST_FILE_LIST
for i in `seq 1 $FILES_COUNT`
do
  echo "$EOS_DIR/testfile-$i" >> $TEST_FILE_LIST
done
/usr/bin/sed -e 's/^/ls -y /' $TEST_FILE_LIST > $TEST_FILE_LIST.eosh # Prepare the file for EOS batch mode

# Hard coded KEYTAB - this is BAD!
echo "0 u:cta g:cta n:cta-taped N:6425591835858577167 c:1496074683 e:0 f:0 k:b7825f9dd4d72952e429e342dd687aa8735411c29587ddd052613e33c0792e0b" > $EOS_KEYTAB
chmod 400 $EOS_KEYTAB

# Copy the test file to EOS (many times; in parallel) for archival
/usr/bin/date "+%Y-%m-%d %H:%M:%S"
/usr/bin/echo -e "***\n*** Copying $FILES_COUNT files from $TEST_FILE_LIST file list to EOS into $EOS_DIR directory ...\n***"
/usr/bin/xargs -a $TEST_FILE_LIST -L 1 -P 10 -I {} /usr/bin/xrdcp --nopbar $TEST_FILE_NAME 'root://eosctatape//{}'

# Wait $TIMEOUT for files until they are migrated
/usr/bin/date "+%Y-%m-%d %H:%M:%S"
/usr/bin/echo -e "***\n*** Waiting until the files are archived on tape (expecting d0::t1 in front of each file entry using the EOS ls -y command), timeout after $TIMEOUT seconds ...\n***"
FILES_TO_BE_ARCHIVED=1
SECONDS_PASSED=0
while [ "$FILES_TO_BE_ARCHIVED" -gt 0 ]
do
#  FILES_TO_BE_ARCHIVED=`/usr/bin/xargs -a $TEST_FILE_LIST -L 1 -P 10 -I {} /usr/bin/eos ls -y {} | /usr/bin/grep -v '^d0::t1' -c`
  FILES_TO_BE_ARCHIVED=`/usr/bin/eos --batch $EOS_MGM_URL $TEST_FILE_LIST.eosh | /usr/bin/grep -v '^d0::t1' -c`
  CURRENT_TIME=`/usr/bin/date "+%Y-%m-%d %H:%M:%S"`
  echo "$CURRENT_TIME Scanned $EOS_DIR using file list $TEST_FILE_LIST - files to be archived: $FILES_TO_BE_ARCHIVED"
  sleep $SLEEP
  let SECONDS_PASSED=$SECONDS_PASSED+$SLEEP
  if [ "$SECONDS_PASSED" -gt "$TIMEOUT" ]
  then
    echo "$0: timing out after $TIMEOUT seconds, stopping the test."
    exit -1
  fi
done

# Retrieve files from tape (using here hard coded CTA keytab - THIS IS NOT SAFE !!!)
/usr/bin/date "+%Y-%m-%d %H:%M:%S"
/usr/bin/echo -e "***\n*** Retrieving tape replicas for $FILES_COUNT files in the $EOS_DIR directory from $TEST_FILE_LIST file list onto EOS disk ...\n***"
/usr/bin/xargs -a $TEST_FILE_LIST -I {} echo "{}?eos.ruid=$USER_ID&eos.rgid=$GROUP_ID" | XrdSecPROTOCOL=sss XrdSecSSSKT=$EOS_KEYTAB /usr/bin/xargs -n 5 -P50 /usr/bin/xrdfs eosctatape prepare -s

# Wait $TIMEOUT for files until they are migrated
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

# Removing files from EOS
#/usr/bin/date "+%Y-%m-%d %H:%M:%S"
#/usr/bin/echo -e "***\n*** Removing ALL files from EOS $EOS_DIR directory ...\n***"
#/usr/bin/eos $EOS_MGM_URL rm -r $EOS_DIR

# Cleanup
/usr/bin/date "+%Y-%m-%d %H:%M:%S"
# /usr/bin/rm -f $TEST_FILE_NAME $TEST_FILE_LIST $TEST_FILE_LIST.eosh $EOS_KEYTAB

exit 0
