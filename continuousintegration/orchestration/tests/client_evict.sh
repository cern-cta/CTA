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


echo "$(date +%s): Trigerring EOS evict workflow as poweruser1:powerusers (12001:1200)"

# Build the list of files with more than 1 disk copy that have been archived before (ie d>=1::t1)
TMP_FILE=$(mktemp)
TO_EVICT=0
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  touch "${TMP_FILE}${subdir}"
  eos root://${EOSINSTANCE} ls -y ${EOS_DIR}/${subdir} | egrep 'd[1-9][0-9]*::t1' | sed -e "s%\s\+% %g;s%.* \([^ ]\+\)$%${subdir}/\1%" >> "${TMP_FILE}${subdir}"
  TO_EVICT=$(( ${TO_EVICT} + $(cat ${TMP_FILE}${subdir} | wc -l ) ))
done
rm -f ${TMP_FILE}


# Get base evict value an new one.
current_evict_val=0
NEW_EVICT_VAL=$(( ${current_evict_val} + 1 ))

echo "$(date +%s): $TO_EVICT files to be evicted from EOS using 'xrdfs prepare -e'"
# We need the -e as we are evicting the files from disk cache (see xrootd prepare definition)
for (( subdir=0; subdir < ${NB_DIRS}; subdir++ )); do
  cat "${TMP_FILE}${subdir}" | sed -e "s%^%${EOS_DIR}/%" | XrdSecPROTOCOL=krb5 KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 xargs --max-procs=10 -n 40 xrdfs ${EOSINSTANCE} prepare -e > /dev/null
done


db_begin_transaction
LEFTOVER=0
TMP_FILE=$(mktemp)
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  LEFTOVER=$(( ${LEFTOVER} + $(eos root://${EOSINSTANCE} ls -y ${EOS_DIR}/${subdir} | egrep '^d[1-9][0-9]*::t1'  | wc -l) ))

  eos root://${EOSINSTANCE} ls -y ${EOS_DIR}/${subdir} | egrep '^d[0][0-9]*::t1' | awk '{print $10}' > ${TMP_FILE}

  cat ${TMP_FILE} | xargs -iTEST_FILE_NAME bash -c "db_update 'evicted' ${subdir}/TEST_FILE_NAME ${NEW_EVICT_VAL} '='"
done
rm -f ${TMP_FILE}
db_commit_transaction

EVICTED=$((${TO_EVICT}-${LEFTOVER}))
echo "$(date +%s): $EVICTED/$TO_EVICT files evicted from EOS 'xrdfs prepare -e'"
