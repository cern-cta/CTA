#!/bin/bash
#
# Preprocess logfiles for more efficient data extraction

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

check_file_exists()
{
  [ -f ${1} ] || error "${1} does not exist or is not a regular file"
  [ -r ${1} ] || error "Can't open ${1} for reading"
  [ -s ${1} ] || error "${1} is zero-length"
}

convert_abs_rel()
{
  awk 'BEGIN { FS=","; SKIPHEADER=1; }
    function rel(arg1, arg2) { return arg2-arg1; }
    SKIPHEADER==1 {
      printf "Archive File ID,Disk File ID,FE CREATE,MGM commit,MGM CLOSEW,TS Archive Pop,TS Archive Tasks,"
      printf "TS Archive Open,TS Archive Read,TS Archive Done,TS Archive Report,MGM ARCHIVED,FE PREPARE,"
      printf "TS Retrieve Pop,TS Retrieve Task,TS Retrieve Tasks,TS Retrieve Position,TS Retrieve Read,"
      printf "TS Retrieve Open,TS Retrieve Done,MGM commit,MGM RETRIEVED\n"
      SKIPHEADER=0; next;
    }
    {
      ARCHIVE_ID=$1
      FILE_ID=$2
      TIME_MGM_CREATE=$3
      TIME_FE_CREATE=$4
      TIME_MGM_ACOMMIT=$5
      TIME_MGM_CLOSEW=$6
      TIME_FE_CLOSEW=$7
      TIME_TS_ARCHIVE_POP=$8
      TIME_TS_ARCHIVE_TASKS=$9
      TIME_TS_ARCHIVE_OPEN=$10
      TIME_TS_ARCHIVE_READ=$11
      TIME_TS_ARCHIVE_DONE=$12
      TIME_TS_ARCHIVE_REPORT=$13
      TIME_MGM_ARCHIVED=$14
      TIME_MGM_PREPARE=$15
      TIME_FE_PREPARE=$16
      TIME_TS_RETRIEVE_POP=$17
      TIME_TS_RETRIEVE_TASK=$18
      TIME_TS_RETRIEVE_TASKS=$19
      TIME_TS_RETRIEVE_POSITION=$20
      TIME_TS_RETRIEVE_READ=$21
      TIME_TS_RETRIEVE_OPEN=$22
      TIME_TS_RETRIEVE_DONE=$23
      TIME_MGM_RCOMMIT=$24
      TIME_MGM_RETRIEVED=$25

      printf "%d,%d,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f\n",
        ARCHIVE_ID,FILE_ID,
        rel(TIME_MGM_CREATE,TIME_FE_CREATE),
        rel(TIME_MGM_CREATE,TIME_MGM_ACOMMIT),
        rel(TIME_MGM_ACOMMIT,TIME_MGM_CLOSEW),
        rel(TIME_MGM_CLOSEW,TIME_FE_CLOSEW),
        rel(TIME_FE_CLOSEW,TIME_TS_ARCHIVE_POP),
        rel(TIME_TS_ARCHIVE_POP,TIME_TS_ARCHIVE_TASKS),
        rel(TIME_TS_ARCHIVE_TASKS,TIME_TS_ARCHIVE_OPEN),
        rel(TIME_TS_ARCHIVE_OPEN,TIME_TS_ARCHIVE_READ),
        rel(TIME_TS_ARCHIVE_READ,TIME_TS_ARCHIVE_DONE),
        rel(TIME_TS_ARCHIVE_DONE,TIME_TS_ARCHIVE_REPORT),
        rel(TIME_TS_ARCHIVE_REPORT,TIME_MGM_ARCHIVED),
        rel(TIME_MGM_PREPARE,TIME_FE_PREPARE),
        rel(TIME_FE_PREPARE,TIME_TS_RETRIEVE_POP),
        rel(TIME_TS_RETRIEVE_POP,TIME_TS_RETRIEVE_TASK),
        rel(TIME_TS_RETRIEVE_TASK,TIME_TS_RETRIEVE_TASKS),
        rel(TIME_TS_RETRIEVE_TASKS,TIME_TS_RETRIEVE_POSITION),
        rel(TIME_TS_RETRIEVE_POSITION,TIME_TS_RETRIEVE_READ),
        rel(TIME_TS_RETRIEVE_READ,TIME_TS_RETRIEVE_OPEN),
        rel(TIME_TS_RETRIEVE_OPEN,TIME_TS_RETRIEVE_DONE),
        rel(TIME_TS_RETRIEVE_DONE,TIME_MGM_RCOMMIT),
        rel(TIME_MGM_RCOMMIT,TIME_MGM_RETRIEVED)
    }' $1
}

[ $# -eq 1 ] || error "Usage: postprocess <file.csv>"
check_file_exists $1
echoc ${LT_BLUE} "Converting from absolute to relative times..."
convert_abs_rel $1
echoc ${LT_BLUE} "done.\n"
