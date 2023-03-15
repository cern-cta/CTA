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
rm -f ${STATUS_FILE}
rm -f ${SURLs}
touch ${STATUS_FILE}
touch ${SURLs}
TMP_FILE=$(mktemp)
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  eos root://${EOSINSTANCE} ls -y ${EOS_DIR}/${subdir} | egrep 'd[1-9][0-9]*::t1' > ${TMP_FILE}
  cat ${TMP_FILE} | sed -e "s%\s\+% %g;s%.* \([^ ]\+\)$%${subdir}/\1%" >> ${STATUS_FILE}
  cat ${TMP_FILE} | sed -e "s%\s\+% %g;s%.* \([^ ]\+\)$%${GFAL2_PROTOCOL}://${EOSINSTANCE}/${EOS_DIR}/${subdir}/\1%" >> ${SURLs}
done
rm -f ${TMP_FILE}

TO_EVICT=$(cat ${STATUS_FILE} | wc -l)

NEW_STAGE_VAL=1

echo "$(date +%s): $TO_EVICT files to be evicted from EOS using 'gfal-evict SURL'"
cat ${SURLs} | xargs --max-procs=${NB_PROCS}  -iTEST_FILE_NAME bash -c "XrdSecPROTOCOL=krb5 KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 xargs --max-procs=10 -n 40 gfal-evict TEST_FILE_NAME"

LEFTOVER=0
status=$(mktemp)
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  LEFTOVER=$(( ${LEFTOVER} + $(eos root://${EOSINSTANCE} ls -y ${EOS_DIR}/${subdir} | egrep '^d[1-9][0-9]*::t1' | wc -l) ))

  eos root://${EOSINSTANCE} ls -y ${EOS_DIR}/${subdir} | egrep '^d[0][0-9]*::t1' | awk '{print $10}' > $status
  cat $status | xargs -iTEST_FILE_NAME bash -c "db_update 'evicted' ${subdir}/TEST_FILE_NAME ${NEW_EVICT_VAL} '='"
done

EVICTED=$((${TO_EVICT}-${LEFTOVER}))
echo "$(date +%s): $EVICTED/$TO_EVICT files evicted from EOS 'gfal-evict SURL'"

LASTCOUNT=${EVICTED}
