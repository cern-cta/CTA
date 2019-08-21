#!/bin/sh

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
  exit 1
}

echoc()
{
  COLOUR=$1
  shift
  echo -e "${COLOUR}$*${NC}"
}

EOS_TEST_DIR_INJECT=/usr/bin/eos-test-dir-inject
[ -x ${EOS_TEST_DIR_INJECT} ] || error "Can't find executable ${EOS_TEST_DIR_INJECT}"

CONFIG_FILE=/etc/cta/castor-migration.conf
[ -r ${CONFIG_FILE} ] || error "Can't find configuration file ${CONFIG_FILE}"

TMPFILE=/tmp/eos-test-inject-sh.$$

CASTOR_PREFIX=$(awk '/^castor.prefix[ 	]/ { print $2 }' ${CONFIG_FILE})
EOS_PREFIX=$(awk '/^eos.prefix[ 	]/ { print $2 }' ${CONFIG_FILE})

# Ping the gRPC interface
${EOS_TEST_DIR_INJECT} ping || error "gRPC ping failed"

# Create the top-level directory. GNU coreutils "mkdir -p" does not return an error if the directory
# already exists; "eos mkdir -p" does return an error, which we explicitly ignore.
eos mkdir -p ${EOS_PREFIX} 2>/dev/null

# Create directory with system-assigned file id -- should succeed
echoc $LT_BLUE "Creating directory with auto-assigned file id"
${EOS_TEST_DIR_INJECT} --path ${CASTOR_PREFIX}/my_test_dir >${TMPFILE}
[ $? -eq 0 ] || error "Creating directory with auto-assigned file id failed"
json-pretty-print.sh ${TMPFILE}
rm ${TMPFILE}
eos ls -l ${EOS_PREFIX}
eos fileinfo ${EOS_PREFIX}/my_test_dir
eos rmdir ${EOS_PREFIX}/my_test_dir

# Create directory with self-assigned file id -- should succeed
echoc $LT_BLUE "Creating directory with self-assigned file id"
${EOS_TEST_DIR_INJECT} --fileid 9876543210 --path ${CASTOR_PREFIX}/my_test_dir >${TMPFILE}
[ $? -eq 0 ] || error "Creating directory with self-assigned file id failed"
json-pretty-print.sh ${TMPFILE}
rm ${TMPFILE}
eos fileinfo ${EOS_PREFIX}/my_test_dir

# Try again -- should fail
echoc $LT_GREEN "Creating directory with the same file id (should fail)"
${EOS_TEST_DIR_INJECT} --fileid 9876543210 --path ${CASTOR_PREFIX}/my_test_dir2 >/dev/null
[ $? -ne 0 ] || error "Creating directory with self-assigned file id succeeded when it should have failed"

# Remove and try again -- should succeed after restarting EOS
echoc $LT_GREEN "Remove the directory and restart EOS to remove the tombstone"
eos rmdir ${EOS_PREFIX}/my_test_dir
echoc $LT_BLUE "Recreate the directory with self-assigned file id (should succeed this time)"
${EOS_TEST_DIR_INJECT} --fileid 9876543210 --path ${CASTOR_PREFIX}/my_test_dir >/dev/null
[ $? -eq 0 ] || error "Creating directory with self-assigned file id failed with error $?"
eos fileinfo ${EOS_PREFIX}/my_test_dir

echoc $LT_GREEN "Cleaning up: removing tombstones and removing injected directories"
eos rmdir ${EOS_PREFIX}/my_test_dir  2>/dev/null
eos rmdir ${EOS_PREFIX}/my_test_dir2 2>/dev/null
