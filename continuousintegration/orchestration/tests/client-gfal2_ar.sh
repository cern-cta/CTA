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


. client_setup.sh "@"

# Immutable file test.

. client_immutable_file.sh

# Archiving Test
if [[ $DONOTARCHIVE == 0 ]]; then
    . client_archive.sh
fi

if [[ $ARCHIVEONLY == 1 ]]; then
  echo "Archiveonly mode: exiting"
  test -z $TAILPID || kill ${TAILPID} &> /dev/null
  exit 0
fi

# Retrieve Test
. client-gfal2_retrieve.sh

# Evict Test
. client-gfal2_evict.sh

# Abort prepare test.
RESTAGEDFILES=0
. client_abortPrepare.sh

# Delete Test
if [[ $REMOVE == 1 ]]; then
  . client_delete.sh
fi

echo "###"
echo "$(date +%s): Results:"
echo "REMOVED/EVICTED/RETRIEVED/ARCHIVED/RESTAGEDFILES/NB_FILES"
echo "${DELETED}/${EVICTED}/${RETRIEVED}/${ARCHIVED}/${RESTAGEDFILES}/$((${NB_FILES} * ${NB_DIRS}))"
echo "###"

test -z ${COMMENT} || annotate "test ${TESTID} FINISHED" "Summary:</br>NB_FILES: $((${NB_FILES} * ${NB_DIRS}))</br>ARCHIVED: ${ARCHIVED}<br/>RETRIEVED: ${RETRIEVED}<br/>STAGERRMED: ${STAGERRMED}</br>DELETED: ${DELETED}" 'test,end'


# stop tail
test -z $TAILPID || kill ${TAILPID} &> /dev/null

RC=0
if [ ${LASTCOUNT} -ne $((${NB_FILES} * ${NB_DIRS})) ]; then
  ((RC++))
  echo "ERROR there were some lost files during the archive/retrieve test with ${NB_FILES} files (first 10):"
  grep -v retrieved ${STATUS_FILE} | sed -e "s;^;${EOS_DIR}/;" | head -10
fi

if [ $(cat ${LOGDIR}/prepare_sys.retrieve.req_id_*.log | grep -v value= | wc -l) -ne 0 ]; then
  # THIS IS NOT YET AN ERROR: UNCOMMENT THE FOLLOWING LINE WHEN https://gitlab.cern.ch/cta/CTA/issues/606 is fixed
  # ((RC++))
  echo "ERROR $(cat ${LOGDIR}/prepare_sys.retrieve.req_id_*.log | grep -v value= | wc -l) files out of $(cat ${LOGDIR}/prepare_sys.retrieve.req_id_*.log | wc -l) prepared files have no sys.retrieve.req_id extended attribute set"
fi


if [ ${RESTAGEDFILES} -ne "0" ]; then
  ((RC++))
  echo "ERROR some files were retrieved in spite of retrieve cancellation."
fi

# This one does not change the return code
# WARNING if everything else was OK
# ERROR otherwise as these xrootd failures could be the reason of the failure
if [ $(ls ${LOGDIR}/xrd_errors | wc -l) -ne 0 ]; then
  # ((RC++)) # do not change RC
  if [ ${RC} -eq 0 ]; then
    echo "WARNING several xrootd failures occured during this run, please check client dumps in ${LOGDIR}/xrd_errors."
  else
    echo "ERROR several xrootd failures occured during this run, please check client dumps in ${LOGDIR}/xrd_errors."
  fi
fi

exit ${RC}
