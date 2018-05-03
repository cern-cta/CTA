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
  echo -ne "${COLOUR}$*${NC}"
}

check_file_exists()
{
  [ -f ${1} ] || error "${1} does not exist or is not a regular file"
  [ -r ${1} ] || error "Can't open ${1} for reading"
  [ -s ${1} ] || error "${1} is zero-length"
}

process_mgm_log()
{
  echoc $LT_BLUE "Extracting log messages from ${MGM_LOG}..."

  awk -vDATE_OFFSET="$DATE_OFFSET" -vTEST_RUN=${TEST_RUN} 'BEGIN { COMMIT_EVENT="ACO" }
    function processLine(pattern, event, logline) {
      gsub("time=.*" pattern, "", logline)
      split(logline,logarray)
      DAYS=logarray[1]-DATE_OFFSET
      HOURS=substr(logarray[2],1,2)
      MINS=substr(logarray[2],4,2)
      SECS=substr(logarray[2],7,2)
      timesecs=((DAYS*24 + HOURS)*60 + MINS)*60 + SECS
      gsub("^.*" TEST_RUN "/", "", logarray[3])
      print logarray[3],event,timesecs
    }

    $0 !~ TEST_RUN { next }

    / default ARCHIVED /        { processLine("ARCHIVED", "ARC", $0) }
    / default CLOSEW /          { processLine("CLOSEW", "CLO", $0) }
    / default SYNC::CREATE /    { processLine("CREATE", "CRE", $0) }
    / default SYNC::PREPARE /   { processLine("PREPARE", "PRE", $0); COMMIT_EVENT="RCO" }
    / retrieve_written CLOSEW / { processLine("CLOSEW", "RWC", $0) }
    / subcmd=commit /           { processLine("subcmd=commit path=", COMMIT_EVENT, $0) }
  ' ${MGM_LOG} >xrdmgm.log.$$

  echoc $LT_BLUE "done.\nSorting..."
  sort -n xrdmgm.log.$$ >xrdmgm-events.log
  echoc $LT_BLUE "done.\n"
  rm -f xrdmgm.log.$$
}

process_frontend_log()
{
  echoc $LT_BLUE "Extracting archive log messages from cta-frontend..."

  awk -vDATE_OFFSET="$DATE_OFFSET" '
    function processLine(event, logline) {
      sub(" .* fileId=\"", " ", logline)
      sub("\".*$", "", logline)
      sub("^20", "", logline)
      sub("T", " ", logline)
      gsub("-", "", logline)
      gsub("+[0-9:]* ", " ", logline)
      split(logline,logarray)
      DAYS=logarray[1]-DATE_OFFSET
      HOURS=substr(logarray[2],1,2)
      MINS=substr(logarray[2],4,2)
      SECS=substr(logarray[2],7,2)
      MSECS=substr(logarray[2],10)
      timesecs=((DAYS*24 + HOURS)*60 + MINS)*60 + SECS
      print logarray[3],event,timesecs "." MSECS
    }

    /CREATE/ { processLine("CRE", $0) }
    /CLOSEW/ { processLine("CLW", $0) }
    /PREPARE/ { processLine("PRE", $0) }
  ' ${FRONTEND_LOG} >cta-frontend.log.$$

  echoc $LT_BLUE "done.\nSorting..."
  sort -n cta-frontend.log.$$ >cta-frontend-events.log
  echoc $LT_BLUE "done.\n"
  rm -f cta-frontend.log.$$
}

process_file_ids()
{
  echoc $LT_BLUE "Extracting disk IDs and archive IDs from cta-frontend..."
  grep ${TEST_RUN} ${FRONTEND_LOG} |\
  grep processCREATE |\
  sed 's/^.*diskFileId="//' |\
  sed 's/" diskFilePath="/ /' |\
  sed 's/" fileId="/ /' |\
  sed 's/".*$//' |\
  awk -vTEST_RUN=${TEST_RUN} '{
    gsub("^.*" TEST_RUN "/", "", $2)
    print $2 " " $1 " " $3
  }' | uniq >filenameToLogIds.txt
  echoc $LT_BLUE "done.\n"
}

process_taped_log()
{
  echoc $LT_BLUE "Extracting archive log messages from cta-taped..."

  awk -vDATE_OFFSET="$DATE_OFFSET" '
    function processLine(event, logline) {
      sub(" .* fileId=\"", " ", logline)
      sub("\".*$", "", logline)
      sub("^20", "", logline)
      sub("T", " ", logline)
      gsub("-", "", logline)
      gsub("+[0-9:]* ", " ", logline)
      split(logline,logarray)
      DAYS=logarray[1]-DATE_OFFSET
      HOURS=substr(logarray[2],1,2)
      MINS=substr(logarray[2],4,2)
      SECS=substr(logarray[2],7,2)
      MSECS=substr(logarray[2],10)
      timesecs=((DAYS*24 + HOURS)*60 + MINS)*60 + SECS
      print logarray[3],event,timesecs "." MSECS
    }

    /In ArchiveMount::getNextJobBatch\(\): popped one job/    { processLine("A_POP", $0) }
    /Created tasks for migrating a file/                      { processLine("A_TSS", $0) }
    /Opened disk file for read/                               { processLine("A_OPN", $0) }
    /File successfully read from disk/                        { processLine("A_RED", $0) }
    /File successfully transmitted to drive/                  { processLine("A_DNE", $0) }
    /Reported to the client a full file archival/             { processLine("A_RPT", $0) }
    /In RetrieveMount::getNextJobBatch\(\): popped one job/   { processLine("R_POP", $0) }
    /Recall task created/                                     { processLine("R_TSK", $0) }
    /Created tasks for recalling a file/                      { processLine("R_RCL", $0) }
    /Successfully positioned for reading/                     { processLine("R_POS", $0) }
    /File successfully read from tape/                        { processLine("R_RED", $0) }
    /Opened disk file for writing/                            { processLine("R_OPN", $0) }
    /File successfully transfered to disk/                    { processLine("R_TRF", $0) }
  ' ${TAPED_LOG} >cta-taped.log.$$

  echoc $LT_BLUE "done.\nSorting..."
  sort -n cta-taped.log.$$ >cta-taped-events.log
  echoc $LT_BLUE "done.\n"
  rm -f cta-taped.log.$$
}

# Log filenames
TEST_RUN=${1}
MGM_LOG=${2-xrdlog.mgm}
FRONTEND_LOG=${3-cta-frontend.log}
TAPED_LOG=${4-cta-taped.log}

# Check files are readable
check_file_exists ${MGM_LOG}
check_file_exists ${FRONTEND_LOG}
check_file_exists ${TAPED_LOG}

# Get date offset value
DATE_OFFSET=$(head -1 ${MGM_LOG} | awk '{ print $1 }')

# Process log data

process_mgm_log
process_frontend_log
process_file_ids
process_taped_log
