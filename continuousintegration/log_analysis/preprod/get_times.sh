#!/bin/sh

error()
{
  echo "$*" >&2
  exit 1
}

get_filename()
{
  awk -vFILENAME=${1} -vFILENAME_PREFIX=${FILENAME_PREFIX} 'BEGIN {
    FILENAME="00000000" FILENAME
    print FILENAME_PREFIX substr(FILENAME, length(FILENAME)-7)
  }'
}

get_ids()
{
  IDS=$(sed "$1q;d" filenameToLogIds.txt)
  FILENAME=$(echo $IDS | cut -d' ' -f1)
  FILE_ID=$(echo $IDS | cut -d' ' -f2)
  ARCHIVE_ID=$(echo $IDS | cut -d' ' -f3)
}

check_results()
{
  NUM_EXPECTED=$1
  NUM_RECEIVED=$(cut -d' ' -f2 | sort | uniq | wc -l)
  [ $NUM_EXPECTED -eq $NUM_RECEIVED ] && return 0
  return 1
}

get_mgm_times()
{
  TMP_RESULTS=$(grep -m6 "^$1 " xrdmgm-events.log)

  echo -e "${TMP_RESULTS}" | check_results 6
  [ $? -ne 0 ] && TMP_RESULTS=$(grep "^$1 " xrdmgm-events.log)

  TIME_MGM_CREATE=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 CRE" | cut -d' ' -f3)
  TIME_MGM_ACOMMIT=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 ACO" | cut -d' ' -f3)
  TIME_MGM_CLOSEW=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 CLO" | cut -d' ' -f3)
  TIME_MGM_ARCHIVED=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 ARC" | cut -d' ' -f3)
  TIME_MGM_PREPARE=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 PRE" | cut -d' ' -f3)
  TIME_MGM_RCOMMIT=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 RCO" | cut -d' ' -f3)
  #TIME_MGM_RETRIEVED=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 RWC" | cut -d' ' -f3)
}

get_frontend_times()
{
  TMP_RESULTS=$(grep -m3 "^$1 " cta-frontend-events.log)

  echo -e "${TMP_RESULTS}" | check_results 3
  [ $? -ne 0 ] && TMP_RESULTS=$(grep "^$1 " cta-frontend-events.log)

  TIME_FE_CREATE=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 CRE" | cut -d' ' -f3)
  TIME_FE_CLOSEW=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 CLW" | cut -d' ' -f3)
  TIME_FE_PREPARE=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 PRE" | cut -d' ' -f3)
}

get_taped_times()
{
  # 1 archive copy: 5+1 messages
  # 2 archive copies: 5+5+1 messages
  # retrieve: 7 messages
  TMP_RESULTS=$(grep -m13 "^$1 " cta-taped-events.log)

  echo -e "${TMP_RESULTS}" | check_results 13
  [ $? -ne 0 ] && TMP_RESULTS=$(grep "^$1 " cta-taped-events.log)

  TIME_TS_ARCHIVE_POP=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 A_POP" | cut -d' ' -f3)
  TIME_TS_ARCHIVE_TASKS=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 A_TSS" | cut -d' ' -f3)
  TIME_TS_ARCHIVE_OPEN=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 A_OPN" | cut -d' ' -f3)
  TIME_TS_ARCHIVE_READ=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 A_RED" | cut -d' ' -f3)
  TIME_TS_ARCHIVE_DONE=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 A_DNE" | cut -d' ' -f3)
  TIME_TS_ARCHIVE_REPORT=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 A_RPT" | cut -d' ' -f3)
  TIME_TS_RETRIEVE_POP=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 R_POP" | cut -d' ' -f3)
  TIME_TS_RETRIEVE_TASK=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 R_TSK" | cut -d' ' -f3)
  TIME_TS_RETRIEVE_TASKS=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 R_RCL" | cut -d' ' -f3)
  TIME_TS_RETRIEVE_POSITION=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 R_POS" | cut -d' ' -f3)
  TIME_TS_RETRIEVE_READ=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 R_RED" | cut -d' ' -f3)
  TIME_TS_RETRIEVE_OPEN=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 R_OPN" | cut -d' ' -f3)
  TIME_TS_RETRIEVE_DONE=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 R_TRF" | cut -d' ' -f3)
}

get_offset_time()
{
  echo "$2-$1" | bc -l
}

get_times()
{
  get_ids $1
  get_mgm_times $FILENAME
  get_frontend_times $ARCHIVE_ID
  get_taped_times $ARCHIVE_ID

  echo -n "$ARCHIVE_ID $FILE_ID "

# For debugging
#  echo $TIME_MGM_CREATE $TIME_FE_CREATE $TIME_MGM_ACOMMIT $TIME_MGM_CLOSEW $TIME_FE_CLOSEW $TIME_TS_ARCHIVE_POP \
#       $TIME_TS_ARCHIVE_TASKS $TIME_TS_ARCHIVE_OPEN $TIME_TS_ARCHIVE_READ $TIME_TS_ARCHIVE_DONE $TIME_TS_ARCHIVE_REPORT \
#       $TIME_MGM_ARCHIVED
#  echo $TIME_MGM_PREPARE $TIME_FE_PREPARE $TIME_TS_RETRIEVE_POP $TIME_TS_RETRIEVE_TASK $TIME_TS_RETRIEVE_TASKS \
#       $TIME_TS_RETRIEVE_POSITION $TIME_TS_RETRIEVE_READ $TIME_TS_RETRIEVE_OPEN $TIME_TS_RETRIEVE_DONE \
#       $TIME_MGM_RCOMMIT $TIME_MGM_RETRIEVED

  # MGM events have second precision. CTA events have microsecond precision

  echo -n "$(get_offset_time $TIME_MGM_CREATE $TIME_FE_CREATE) "
  echo -n "$(get_offset_time $TIME_MGM_CREATE $TIME_MGM_ACOMMIT) "
  echo -n "$(get_offset_time $TIME_MGM_ACOMMIT $TIME_MGM_CLOSEW) "
  echo -n "$(get_offset_time $TIME_FE_CREATE $TIME_FE_CLOSEW) "
  echo -n "$(get_offset_time $TIME_FE_CLOSEW $TIME_TS_ARCHIVE_POP) "
  echo -n "$(get_offset_time $TIME_TS_ARCHIVE_POP $TIME_TS_ARCHIVE_TASKS) "
  echo -n "$(get_offset_time $TIME_TS_ARCHIVE_TASKS $TIME_TS_ARCHIVE_OPEN) "
  echo -n "$(get_offset_time $TIME_TS_ARCHIVE_OPEN $TIME_TS_ARCHIVE_READ) "
  echo -n "$(get_offset_time $TIME_TS_ARCHIVE_READ $TIME_TS_ARCHIVE_DONE) "
  echo -n "$(get_offset_time $TIME_TS_ARCHIVE_DONE $TIME_TS_ARCHIVE_REPORT) "
  echo -n "$(get_offset_time $TIME_TS_ARCHIVE_REPORT $TIME_MGM_ARCHIVED) "

  echo -n "$(get_offset_time $TIME_MGM_PREPARE $TIME_FE_PREPARE) "
  echo -n "$(get_offset_time $TIME_FE_PREPARE $TIME_TS_RETRIEVE_POP) "
  echo -n "$(get_offset_time $TIME_TS_RETRIEVE_POP $TIME_TS_RETRIEVE_TASK) "
  echo -n "$(get_offset_time $TIME_TS_RETRIEVE_TASK $TIME_TS_RETRIEVE_TASKS) "
  echo -n "$(get_offset_time $TIME_TS_RETRIEVE_TASKS $TIME_TS_RETRIEVE_POSITION) "
  echo -n "$(get_offset_time $TIME_TS_RETRIEVE_POSITION $TIME_TS_RETRIEVE_READ) "
  echo -n "$(get_offset_time $TIME_TS_RETRIEVE_READ $TIME_TS_RETRIEVE_OPEN) "
  echo -n "$(get_offset_time $TIME_TS_RETRIEVE_OPEN $TIME_TS_RETRIEVE_DONE) "
  echo    "$(get_offset_time $TIME_TS_RETRIEVE_DONE $TIME_MGM_RCOMMIT);"
#  echo -n "$(get_offset_time $TIME_TS_RETRIEVE_DONE $TIME_MGM_RCOMMIT) "
#  echo    "$(get_offset_time $TIME_MGM_RCOMMIT $TIME_MGM_RETRIEVED);"
}

FILE_NUM_START=${1:-1}
FILE_NUM_END=${2:-1000000}

FILENAME_PREFIX=

[ -r filenameToLogIds.txt ] || error "Can't open filenameToLogIds.txt"
[ -r xrdmgm-events.log ] || error "Can't open xrdmgm-events.log"
[ -r cta-frontend-events.log ] || error "Can't open cta-frontend-events.log"
[ -r cta-taped-events.log ] || error "Can't open cta-taped-events.log"

echo "cta_times = ["

FILE_NUM=${FILE_NUM_START}
while :
do
  get_times ${FILE_NUM}
  FILE_NUM=$((${FILE_NUM}+1))
  [ $FILE_NUM -le $FILE_NUM_END ] || break
done

echo "];"

