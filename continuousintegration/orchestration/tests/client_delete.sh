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


# Get list of files currently on tape.
INITIALFILESONTAPE=$((${NB_FILES}*${NB_DIRS}))

# We can now delete the files
echo "$(date +%s): Waiting for files to be removed from EOS and tapes"
admin_kdestroy &>/dev/null
admin_kinit &>/dev/null
if $(admin_cta admin ls &>/dev/null); then
  echo "$(date +%s): Got cta admin privileges, can proceed with the workflow"
else
  # displays what failed and fail
  admin_cta admin ls
  die "$(date +%s): Could not launch cta-admin command."
fi

echo "$(date +%s): Before starting deletion there are ${INITIALFILESONTAPE} files on tape."
#XrdSecPROTOCOL=sss eos -r 0 0 root://${EOSINSTANCE} rm -Fr ${EOS_DIR} &
KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos root://${EOSINSTANCE} rm -Fr ${EOS_DIR} &
EOSRMPID=$!
# wait a bit in case eos prematurely fails...
sleep 1
if test ! -d /proc/${EOSRMPID}; then
  # eos rm process died, get its status
  wait ${EOSRMPID}
  test $? -ne 0 && die "Could not launch eos rm"
fi

# Now we can start to do something...
# deleted files are the ones that made it on tape minus the ones that are still on tapes...
echo "$(date +%s): Waiting for files to be deleted:"
SECONDS_PASSED=0
WAIT_FOR_DELETED_FILE_TIMEOUT=$((5+${NB_FILES}/9))
FILESONTAPE=${INITIALFILESONTAPE}

while test 0 != ${FILESONTAPE}; do
  echo "$(date +%s): Waiting for files to be deleted from tape: Seconds passed = ${SECONDS_PASSED}"
  sleep 1
  let SECONDS_PASSED=SECONDS_PASSED+1

  if test ${SECONDS_PASSED} == ${WAIT_FOR_DELETED_FILE_TIMEOUT}; then
    echo "Timed out after ${WAIT_FOR_DELETED_FILE_TIMEOUT} seconds waiting for file to be deleted from tape"
    break
  fi

  FILESONTAPE=0
  for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
      FILESONTAPE=$((${FILESONTAPE} + $(eos root://${EOSINSTANCE} ls -y ${EOS_DIR}/${subdir} | egrep '^d[0-9]+::t1' | wc -l)))
  done

  DELETED=$((${INITIALFILESONTAPE} - ${FILESONTAPE}))

  echo "${DELETED}/${INITIALFILESONTAPE} deleted"
done


# kill eos rm command that may run in the background
kill ${EOSRMPID} &> /dev/null

# As we deleted the directory we may have deleted more files than the ones we retrieved
# therefore we need to take the smallest of the 2 values to decide if the system test was
# successful or not
LASTCOUNT=0
if [[ ${INITIALFILESONTAPE} -gt ${DELETED} ]]; then
  LASTCOUNT=${DELETED}
  echo "Some files have not been deleted:"
else
  echo "All files have been deleted"
  LASTCOUNT=${INITIALFILESONTAPE}
fi

if [ ${LASTCOUNT} -ne $((${NB_FILES} * ${NB_DIRS})) ]; then
  echo "ERROR there were some lost files during the archive/retrieve test with ${NB_FILES} files" >> /tmp/RC
  echo "ERROR there were some lost files during the archive/retrieve test with ${NB_FILES} files"
fi
