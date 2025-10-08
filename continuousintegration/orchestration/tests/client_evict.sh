#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later



echo "$(date +%s): Trigerring EOS evict workflow as poweruser1:powerusers (12001:1200)"

TO_EVICT=$((${NB_FILES}*${NB_DIRS}))
echo "$(date +%s): $TO_EVICT files to be evicted from EOS using 'xrdfs prepare -e'"
# We need the -e as we are evicting the files from disk cache (see xrootd prepare definition)
for (( subdir=0; subdir < ${NB_DIRS}; subdir++ )); do
  command_str=$(eval echo "${evict}")
  prefix_eval=$(eval echo "${evict_prefix}")
  seq -w 0 $((${NB_FILES} - 1)) | sed "s~^~${prefix_eval}~" | xargs -n $evict_count echo | xargs --max-procs=10 -IFILE_LIST bash -c "${command_str}"
done

sleep 1

LEFTOVER=0
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  LEFTOVER=$(( ${LEFTOVER} + $(eos root://${EOS_MGM_HOST} ls -y ${EOS_DIR}/${subdir} | grep -E '^d[1-9][0-9]*::t1' | wc -l) ))
done

EVICTED=$((${TO_EVICT}-${LEFTOVER}))
echo "$(date +%s): $EVICTED/$TO_EVICT files evicted from EOS ${evict}"

if [[ ${EVICTED} -ne ${TO_EVICT} ]]; then
  echo "ERROR: some files were not evicted"
  exit 1
fi
