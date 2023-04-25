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



RC=0

TOTAL_FILES=$((${NB_FILES} * ${NB_DIRS}))
IFS='|' read -a results_array <<< $(db_results)

ARCHIVED=${results_array[0]}
RETRIEVED=${results_array[1]}
EVICTED=${results_array[2]}
ABORTED=${results_array[3]}
DELETED=${results_array[4]}

names_arr=("ARCHIVED" "RETRIEVED" "EVICTED" "ABORTED" "DELETED")

echo "###"
echo "$(date +%s): Results:"
echo "NB FILES / ARCHIVED / RETRIEVED / EVICTED / ABORTED / DELETED"
echo "${TOTAL_FILES} / ${ARCHIVED} / ${RETRIEVED} / ${EVICTED} / ${ABORTED} / ${DELETED}"
echo "###"

test -z ${COMMENT} || annotate "test ${TESTID} FINISHED" "Summary:</br>NB_FILES: $((${NB_FILES} * ${NB_DIRS}))</br>ARCHIVED: ${ARCHIVED}<br/>RETRIEVED: ${RETRIEVED}</br>EVICTED: ${EVICTED}<br/>DELETED: ${DELETED}" 'test,end'

TOTAL_FILES=$(( ${NB_FILES} * ${NB_DIRS} ))
for i in "${!resuls_array[@]}"; do
  if [[ ${TOTAL_FILES} -ne ${results_array[$i]} ]]; then
    echo "ERROR: ${names_arr[${i}]} count value ${results_arr[${i}]} does not match the expected value ${TOTAL_FILES}."
  fi
done


if [ $(cat ${LOGDIR}/prepare_sys.retrieve.req_id_*.log | grep -v value= | wc -l) -ne 0 ]; then
  # THIS IS NOT YET AN ERROR: UNCOMMENT THE FOLLOWING LINE WHEN https://gitlab.cern.ch/cta/CTA/issues/606 is fixed
  # ((RC++))
  echo "ERROR $(cat ${LOGDIR}/prepare_sys.retrieve.req_id_*.log | grep -v value= | wc -l) files out of $(cat ${LOGDIR}/prepare_sys.retrieve.req_id_*.log | wc -l) prepared files have no sys.retrieve.req_id extended attribute set"
fi


# This one does not change the return code
# WARNING if everything else was OK
# ERROR otherwise as these xrootd failures could be the reason of the failure
if [ $(ls ${LOGDIR}/xrd_errors | wc -l) -ne 0 ]; then
  # ((RC++)) # do not change RC
  #if [ ${RC} -eq 0 ]; then
  if [[ $(cat /tmp/RC | wc -l) -eq 0 ]]; then
    echo "WARNING several xrootd failures occured during this run, please check client dumps in ${LOGDIR}/xrd_errors."
  else
    echo "ERROR several xrootd failures occured during this run, please check client dumps in ${LOGDIR}/xrd_errors."
  fi
fi

exit $(cat /tmp/RC | wc -l)
