#!/bin/bash

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
  echo -ne "${COLOUR}$*${NC}" >&2
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
  TMP_RESULTS=$(grep -m7 "^$1 " xrdmgm-events.log)

  echo -e "${TMP_RESULTS}" | check_results 6
  [ $? -ne 0 ] && TMP_RESULTS=$(grep "^$1 " xrdmgm-events.log)

  TIME_MGM_CREATE=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 CRE" | cut -d' ' -f3)
  TIME_MGM_ACOMMIT=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 ACO" | cut -d' ' -f3)
#  TIME_MGM_CLOSEW=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 CLO" | cut -d' ' -f3)
  TIME_MGM_ARCHIVED=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 ARC" | cut -d' ' -f3)
  TIME_MGM_PREPARE=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 PRE" | cut -d' ' -f3)
  TIME_MGM_RCOMMIT=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 RCO" | cut -d' ' -f3)
  TIME_MGM_RETRIEVED=$(echo -e "${TMP_RESULTS}" | grep -m1 "^$1 RWC" | cut -d' ' -f3)
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
  get_mgm_times ${FILENAME}
  get_frontend_times ${ARCHIVE_ID}
  get_taped_times ${ARCHIVE_ID}

  echo -n "${ARCHIVE_ID},"
  echo -n "${FILE_ID},"

  # Note: MGM events have second precision. CTA events have microsecond precision

  echo -n "${TIME_MGM_CREATE},"
  echo -n "${TIME_FE_CREATE},"
  echo -n "${TIME_MGM_ACOMMIT},"
#  echo -n "${TIME_MGM_CLOSEW},"
  echo -n "${TIME_FE_CLOSEW},"
  echo -n "${TIME_TS_ARCHIVE_POP},"
  echo -n "${TIME_TS_ARCHIVE_TASKS},"
  echo -n "${TIME_TS_ARCHIVE_OPEN},"
  echo -n "${TIME_TS_ARCHIVE_READ},"
  echo -n "${TIME_TS_ARCHIVE_DONE},"
  echo -n "${TIME_TS_ARCHIVE_REPORT},"
  echo -n "${TIME_MGM_ARCHIVED},"

  echo -n "${TIME_MGM_PREPARE},"
  echo -n "${TIME_FE_PREPARE},"
  echo -n "${TIME_TS_RETRIEVE_POP},"
  echo -n "${TIME_TS_RETRIEVE_TASK},"
  echo -n "${TIME_TS_RETRIEVE_TASKS},"
  echo -n "${TIME_TS_RETRIEVE_POSITION},"
  echo -n "${TIME_TS_RETRIEVE_READ},"
  echo -n "${TIME_TS_RETRIEVE_OPEN},"
  echo -n "${TIME_TS_RETRIEVE_DONE},"
  echo -n "${TIME_MGM_RCOMMIT},"
  echo    "${TIME_MGM_RETRIEVED}"
}

FILE_NUM_START=1
FILE_NUM_END=$(wc -l filenameToLogIds.txt | cut -d' ' -f1)

[ -r filenameToLogIds.txt ] || error "Can't open filenameToLogIds.txt"
[ -r xrdmgm-events.log ] || error "Can't open xrdmgm-events.log"
[ -r cta-frontend-events.log ] || error "Can't open cta-frontend-events.log"
[ -r cta-taped-events.log ] || error "Can't open cta-taped-events.log"

echoc $LT_BLUE "Creating CSV from events..."

# Print CSV header
echo -n "Archive File ID,Disk File ID,MGM CREATE,FE CREATE,MGM commit,FE CLOSEW,"
echo -n "TS Archive Pop,TS Archive Tasks,TS Archive Open,TS Archive Read,TS Archive Done,"
echo -n "TS Archive Report,MGM ARCHIVED,MGM PREPARE,FE PREPARE,TS Retrieve Pop,TS Retrieve Task,"
echo -n "TS Retrieve Tasks,TS Retrieve Position,TS Retrieve Read,TS Retrieve Open,TS Retrieve Done,"
echo    "MGM commit,MGM RETRIEVED"

# Print CSV data
FILE_NUM=${FILE_NUM_START}
while :
do
  [ "$FILE_NUM" == "$((${FILE_NUM}/100))00" ] && echoc $LT_BLUE "${FILE_NUM}..."
  get_times ${FILE_NUM}
  FILE_NUM=$((${FILE_NUM}+1))
  [ $FILE_NUM -le $FILE_NUM_END ] || break
done

echoc $LT_BLUE "done.\n"

