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


# Provide an EOS directory and return the list of tapes containing files under that directory
nsls_tapes()
{
  EOS_DIR=${1:-${EOS_BASEDIR}}

  # 1. Query EOS namespace to get a list of file IDs
  # 2. Pipe to "tape ls" to get the list of tapes where those files are archived
  eos root://${EOSINSTANCE} find --fid ${EOS_DIR} |\
    admin_cta --json tape ls --fxidfile /dev/stdin |\
    jq '.[] | .vid' | sed 's/"//g'
}


# Provide a list of tapes and list the filenames of the files stored on those tapes
tapefile_ls()
{
  for vid in $*
  do
    admin_cta --json tapefile ls --lookupnamespace --vid ${vid} |\
    jq '.[] | .df.path'
  done
}

# Get list of files currently on tape.
tmp_file=$(mktemp)
initial_files_on_tape=$(mktemp)
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
    eos root://${EOSINSTANCE} ls -y ${EOS_DIR}/${subdir} | egrep '^d[0-9][0-9]*::t1' | awk '{print $10}' > tmp_file
    cat $tmp_file | xargs -iFILE_NAME echo ${subdir}/FILE_NAME >> $initial_files_on_tape
done

# We can now delete the files
echo "Waiting for files to be removed from EOS and tapes"
admin_kdestroy &>/dev/null
admin_kinit &>/dev/null
if $(admin_cta admin ls &>/dev/null); then
  echo "Got cta admin privileges, can proceed with the workflow"
else
  # displays what failed and fail
  admin_cta admin ls
  die "Could not launch cta-admin command."
fi
# recount the files on tape as the workflows may have gone further...
VIDLIST=$(nsls_tapes ${EOS_DIR})
INITIALFILESONTAPE=$(tapefile_ls ${VIDLIST} | wc -l)
echo "Before starting deletion there are ${INITIALFILESONTAPE} files on tape."
#XrdSecPROTOCOL=sss eos -r 0 0 root://${EOSINSTANCE} rm -Fr ${EOS_DIR} &
KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 gfal-rm -r ${GFAL2_PROTOCOL}://${EOSINSTANCE}/${EOS_DIR} 1>/dev/null &

# wait a bit in case eos prematurely fails...
sleep 0.1i
if test ! -d /proc/${EOSRMPID}; then
  # eos rm process died, get its status
  wait ${EOSRMPID}
  test $? -ne 0 && die "Could not launch eos rm"
fi

# Now we can start to do something...
# deleted files are the ones that made it on tape minus the ones that are still on tapes...
echo "Waiting for files to be deleted:"
SECONDS_PASSED=0
WAIT_FOR_DELETED_FILE_TIMEOUT=$((5+${NB_FILES}/9))
FILESONTAPE=${INITIALFILESONTAPE}

while test 0 != ${FILESONTAPE}; do
  echo "Waiting for files to be deleted from tape: Seconds passed = ${SECONDS_PASSED}"
  sleep 1
  let SECONDS_PASSED=SECONDS_PASSED+1

  if test ${SECONDS_PASSED} == ${WAIT_FOR_DELETED_FILE_TIMEOUT}; then
    echo "Timed out after ${WAIT_FOR_DELETED_FILE_TIMEOUT} seconds waiting for file to be deleted from tape"
    break
  fi

  FILESONTAPE=$(tapefile_ls ${VIDLIST} > >(wc -l) 2> >(cat > /tmp/ctaerr))

  if [[ $(cat /tmp/ctaerr | wc -l) -gt 0 ]]; then
    echo "cta-admin COMMAND FAILED!!"
    echo "ERROR CTA ERROR MESSAGE:"
    cat /tmp/ctaerr
    break
  fi

  DELETED=$((${INITIALFILESONTAPE} - ${FILESONTAPE}))

  echo "${DELETED}/${INITIALFILESONTAPE} deleted"
done


# kill eos rm command that may run in the background
kill ${EOSRMPID} &> /dev/null


LASTCOUNT=0

if [[ ${RETRIEVED} -gt ${DELETED} ]]; then
  LASTCOUNT=${DELETED}
  echo "Some files have not been deleted:"
  tapefile_ls ${VIDLIST}
else
  echo "All files have been deleted"
  LASTCOUNT=${RETRIEVED}
  db_begin_transaction
  db_update_col "deleted" "+" "1"
  db_commit_transaction
fi

if [ ${LASTCOUNT} -ne $((${NB_FILES} * ${NB_DIRS})) ]; then
  #((RC++))
  echo "ERROR there were some lost files during the archive/retrieve test with ${NB_FILES} files" >> /tmp/RC
  echo "ERROR there were some lost files during the archive/retrieve test with ${NB_FILES} files (first 10):"
  grep -v retrieved ${STATUS_FILE} | sed -e "s;^;${EOS_DIR}/;" | head -10
fi
