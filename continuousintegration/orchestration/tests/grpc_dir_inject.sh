#!/bin/sh

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

# Migration tools parameters
#EOSINSTANCE=ctaeos
#EOS_CMD="/usr/bin/eos root://${EOSINSTANCE}"
EOS_CMD="/usr/bin/eos"
EOS_TEST_DIR_INJECT=/usr/bin/eos-test-dir-inject
CONFIG_FILE=/etc/cta/castor-migration.conf
TMPFILE=/tmp/eos-test-inject-sh.$$

# Colours
NC='\033[0m' # No colour
RED='\033[0;31m'
LT_RED='\033[1;31m'
GREEN='\033[0;32m'
LT_GREEN='\033[1;32m'
ORANGE='\033[0;33m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
LT_BLUE='\033[1;34m'

error()
{
  echo -e "${RED}$*${NC}" >&2
  if [ -r ${TMPFILE} ]
  then
    json-pretty-print.sh ${TMPFILE}
  fi
  exit 1
}

echoc()
{
  COLOUR=$1
  shift
  echo -e "${COLOUR}$*${NC}"
}

[ -x ${EOS_TEST_DIR_INJECT} ] || error "Can't find executable ${EOS_TEST_DIR_INJECT}"
[ -r ${CONFIG_FILE} ] || error "Can't find configuration file ${CONFIG_FILE}"
CASTOR_PREFIX=$(awk '/^castor.prefix[ 	]/ { print $2 }' ${CONFIG_FILE})
EOS_PREFIX=$(awk '/^eos.prefix[ 	]/ { print $2 }' ${CONFIG_FILE})

echoc $LT_BLUE "gRPC configuration:"
cat ${CONFIG_FILE}

# Ping the gRPC interface
${EOS_TEST_DIR_INJECT} ping || error "gRPC ping failed"

# Create the top-level directory.
#
# Note: GNU coreutils "mkdir -p" does not return an error if the directory already exists;
#       but "eos mkdir -p" does return an error.
echo Creating ${EOS_PREFIX}...
${EOS_CMD} mkdir -p ${EOS_PREFIX}

# Create directory with system-assigned file id -- should succeed
echoc $LT_BLUE "Creating directory with auto-assigned file id"
${EOS_TEST_DIR_INJECT} --path ${CASTOR_PREFIX}/test_dir1 >${TMPFILE}
[ $? -eq 0 ] || error "Creating directory with auto-assigned file id failed"
json-pretty-print.sh ${TMPFILE}
rm ${TMPFILE}
${EOS_CMD} ls -l ${EOS_PREFIX}
${EOS_CMD} fileinfo ${EOS_PREFIX}/test_dir1
${EOS_CMD} attr ls ${EOS_PREFIX}/test_dir1
/usr/bin/eos root://${EOSINSTANCE} rmdir ${EOS_PREFIX}/test_dir1

# Create directory with self-assigned file id -- should succeed
TEST_FILE_ID=123456789
echoc $LT_BLUE "Creating directory with self-assigned file id"
${EOS_TEST_DIR_INJECT} --fileid ${TEST_FILE_ID} --path ${CASTOR_PREFIX}/test_dir2 >${TMPFILE}
[ $? -eq 0 ] || error "Creating directory with self-assigned file id failed"
json-pretty-print.sh ${TMPFILE}
rm ${TMPFILE}
${EOS_CMD} fileinfo ${EOS_PREFIX}/test_dir2

# Try again -- should fail
echoc $LT_GREEN "Creating directory with the same path (should fail)"
${EOS_TEST_DIR_INJECT} --path ${CASTOR_PREFIX}/test_dir2 >/dev/null
[ $? -ne 0 ] || error "Creating directory with self-assigned file id succeeded when it should have failed"

# Try again -- should fail
echoc $LT_GREEN "Creating directory with the same file id (should fail)"
${EOS_TEST_DIR_INJECT} --fileid ${TEST_FILE_ID} --path ${CASTOR_PREFIX}/test_dir3 >/dev/null
[ $? -ne 0 ] || error "Creating directory with self-assigned file id succeeded when it should have failed"

# Remove and try again -- should succeed
echoc $LT_GREEN "Remove the directory and tombstone"
${EOS_CMD} rmdir ${EOS_PREFIX}/test_dir2
${EOS_CMD} ns cache drop-single-container ${TEST_FILE_ID}

echoc $LT_BLUE "Recreate the directory with self-assigned file id (should succeed this time)"
${EOS_TEST_DIR_INJECT} --fileid ${TEST_FILE_ID} --path ${CASTOR_PREFIX}/test_dir2 >/dev/null
[ $? -eq 0 ] || error "Creating directory with self-assigned file id failed with error $?"
${EOS_CMD} fileinfo ${EOS_PREFIX}/test_dir2
${EOS_CMD} rmdir ${EOS_PREFIX}/test_dir2
