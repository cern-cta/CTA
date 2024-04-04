#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2024 CERN
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


echo "$(date +%s): Trigerring EOS evict workflow as poweruser1:powerusers (12001:1200)"

TO_EVICT=$((${NB_FILES}*${NB_DIRS}))
echo "$(date +%s): $TO_EVICT files to be evicted from EOS using 'xrdfs prepare -e'"
# We need the -e as we are evicting the files from disk cache (see xrootd prepare definition)
for (( subdir=0; subdir < ${NB_DIRS}; subdir++ )); do
  command_str=$(eval echo "${evict}")
  seq -w 0 $((${NB_FILES} - 1)) | xargs --max-procs=100 -n 100 -iTEST_FILE_NAME bash -c "$command_str"
done

sleep 1

LEFTOVER=0
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  LEFTOVER=$(( ${LEFTOVER} + $(eos root://${EOSINSTANCE} ls -y ${EOS_DIR}/${subdir} | egrep '^d[1-9][0-9]*::t1' | wc -l) ))
done

if [[ ${LEFTOVER} -ne 0 ]]; then
  echo "$LEFTOVER files not evicted, trying a second time."
  for (( subdir=0; subdir < ${NB_DIRS}; subdir++ )); do
    for file in $(eos root://${EOSINSTANCE} ls -y ${EOS_DIR}/${subdir} | egrep '^d1::t1' | awk -F' ' '{print $10}'); do
      echo $file | xargs --max-procs=1 -n1 -iTEST_FILE_NAME bash -c "$command_str"
    done
  done
fi

EVICTED=$((${TO_EVICT}-${LEFTOVER}))
echo "$(date +%s): $EVICTED/$TO_EVICT files evicted from EOS ${evict}"

if [[ ${EVICTED} -ne ${TO_EVICT} ]]; then
  echo "ERROR: some files were not evicted"
  exit 1
fi
