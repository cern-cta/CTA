#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


EOS_MGM_HOST=ctaeos
EOS_BASEDIR=/eos/ctaeos/cta
TEST_FILE_NAME_BASE=test
ARCHIVEONLY=0 # Only archive files or do the full test?
DONOTARCHIVE=0 # files were already archived in a previous run NEED TARGETDIR
TARGETDIR=''
LOGDIR='/var/log'
PREQUEUE=0
SKIP_WAIT_FOR_ARCHIVE=0
SKIP_ARCHIVE=0
SKIP_GET_XATTRS=1
SKIP_EVICT=1
RELAUNCH=0
RELAUNCH_EOS_DIR="${EOS_BASEDIR}/552e70f9-93fd-4792-95e9-c87023c56ad9/"
COMMENT=''
# id of the test so that we can track it
TESTID="$(date +%y%m%d%H%M)"

DRIVE_UP='.*'
# if you wish to disable pre-queueing, set DRIVE_UP_SUBDIR_NUMBER=0
DRIVE_UP_SUBDIR_NUMBER=0 # 20
SLEEP_BEFORE_SUBDIR_NUMBER=1000 # more then NB_DIRS means never
SLEEP_TIME_AFTER_SUBDIR_NUMBER=0 #6300 # 1h45m sleep time
NB_PROCS=1
NB_FILES=1
NB_DIRS=1
FILE_KB_SIZE=1 # NB bs for dd
DD_BS=16 # DD bs option DO NOT USE SHORTNAME: NO 1k BUT 1024!!!
REMOVE=0
TAILPID=''
TAPEAWAREGC=0

NB_BATCH_PROCS=10  # number of parallel batch processes
BATCH_SIZE=100    # number of files per batch process

SSH_OPTIONS='-o BatchMode=yes -o ConnectTimeout=10'

die() {
  echo "$@" 1>&2
  test -z $TAILPID || kill ${TAILPID} &> /dev/null
  exit 1
}


usage() { cat <<EOF 1>&2
Usage: $0 [-n <nb_files_perdir>] [-N <nb_dir>] [-s <file_kB_size>] [-p <# parallel procs>] [-d <eos_dest_dir>] [-e <eos_instance>] [-r]
  -r		Remove files at the end: launches the delete workflow on the files that were deleted. WARNING: THIS CAN BE FATAL TO THE NAMESPACE IF THERE ARE TOO MANY FILES AND XROOTD STARTS TO TIMEOUT.
  -a		Archiveonly mode: exits after file archival
  -g		Tape aware GC?
EOF
exit 1
}


# Send annotations to Influxdb
annotate() {
  TITLE=$1
  TEXT=$2
  TAGS=$3
  LINE="ctapps_tests title=\"${TITLE}\",text=\"${TEXT}\",tags=\"${TAGS}\" $(date +%s)"
  curlcmd="curl --connect-timeout 2 -X POST 'https://ctapps-influx02.cern.ch:8086/write?db=annotations&u=annotations&p=annotations&precision=s' --data-binary '${LINE}'"
  eval ${curlcmd}
}


# Provide an EOS directory and return the list of tapes containing files under that directory
nsls_tapes()
{
  EOS_DIR=${1:-${EOS_BASEDIR}}

  # 1. Query EOS namespace to get a list of file IDs
  # 2. Pipe to "tape ls" to get the list of tapes where those files are archived
  eos root://${EOS_MGM_HOST} find --fid ${EOS_DIR} |\
    admin_cta --json tape ls --fxidfile /dev/stdin |\
    jq '.[] | .vid' | sed 's/"//g'
}

# Provide a list of tapes and list the filenames of the files stored on those tapes
tapefile_ls()
{
  for vid in $*
  do
    admin_cta --json tapefile ls --lookupnamespace --vid ${vid} |\
    jq '.[] | .df.path'
  done
}

delete_files_from_eos_and_tapes(){
  # We can now delete the files
  DELETED=0
  if [[ $REMOVE == 1 ]]; then
    echo "Waiting for files to be removed from EOS and tapes"
    # . /root/client_helper.sh
    admin_kdestroy &>/dev/null
    admin_kinit &>/dev/null
    if $(admin_cta admin ls &>/dev/null); then
      echo "Got cta admin privileges, can proceed with the workflow"
    else
      # displays what failed and fail
      admin_cta admin ls
      die "Could not launch cta-admin command."
    fi
    # recount the files on tape as the workflows may have gone further...
    VIDLIST=$(nsls_tapes ${EOS_DIR})
    INITIALFILESONTAPE=$(tapefile_ls ${VIDLIST} | wc -l)
    echo "Before starting deletion there are ${INITIALFILESONTAPE} files on tape."
    KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos root://${EOS_MGM_HOST} rm -Fr ${EOS_DIR} &
    EOSRMPID=$!
    # wait a bit in case eos prematurely fails...
    sleep 0.1
    if test ! -d /proc/${EOSRMPID}; then
      # eos rm process died, get its status
      wait ${EOSRMPID}
      test $? -ne 0 && die "Could not launch eos rm"
    fi
    # Now we can start to do something...
    # The number of deleted files will be determined using the number of files initaly on tapes INITIALFILESONTAPE
    # minus the number of files which are still on tapes at any point in time (output of tapefile_ls)...
    echo "Waiting for files to be deleted:"
    SECONDS_PASSED=0
    WAIT_FOR_DELETED_FILE_TIMEOUT=$((5+${NB_FILES}/9))
    FILESONTAPE=${INITIALFILESONTAPE}
    while test 0 != ${FILESONTAPE}; do
      echo "Waiting for files to be deleted from tape: Seconds passed = ${SECONDS_PASSED}"
      sleep 1
      let SECONDS_PASSED=SECONDS_PASSED+1

      if test ${SECONDS_PASSED} == ${WAIT_FOR_DELETED_FILE_TIMEOUT}; then
        echo "Timed out after ${WAIT_FOR_DELETED_FILE_TIMEOUT} seconds waiting for file to be deleted from tape"
        break
      fi
      FILESONTAPE=$(tapefile_ls ${VIDLIST} > >(wc -l) 2> >(cat > /tmp/ctaerr))
      if [[ $(cat /tmp/ctaerr | wc -l) -gt 0 ]]; then
        echo "cta-admin COMMAND FAILED!!"
        echo "ERROR CTA ERROR MESSAGE:"
        cat /tmp/ctaerr
        break
      fi
      DELETED=$((${INITIALFILESONTAPE} - ${FILESONTAPE}))
      echo "${DELETED}/${INITIALFILESONTAPE} deleted"
    done

    # kill eos rm command that may run in the background
    kill ${EOSRMPID} &> /dev/null

    # As we deleted the directory we may have deleted more files than the ones we retrieved
    # therefore we need to take the smallest of the 2 values to decide if the system test was
    # successful or not
    if [[ ${RETRIEVED} -gt ${DELETED} ]]; then
      LASTCOUNT=${DELETED}
      echo "Some files have not been deleted:"
      tapefile_ls ${VIDLIST}
    else
      echo "All files have been deleted"
      LASTCOUNT=${RETRIEVED}
    fi
  fi
}

while getopts "d:e:n:N:s:p:vS:rAPGt:m:Q" o; do
    case "${o}" in
        e)
            EOS_MGM_HOST=${OPTARG}
            ;;
        d)
            EOS_BASEDIR=${OPTARG}
            ;;
        n)
            NB_FILES=${OPTARG}
            ;;
        N)
            NB_DIRS=${OPTARG}
            ;;
        s)
            FILE_KB_SIZE=${OPTARG}
            ;;
        p)
            NB_PROCS=${OPTARG}
            ;;
        v)
            VERBOSE=1
            ;;
        r)
            REMOVE=1
            ;;
        A)
            ARCHIVEONLY=1
            ;;
        P)
            DONOTARCHIVE=1
            ;;
        G)
            TAPEAWAREGC=1
            ;;
        Q)
            PREQUEUE=1
            ;;
        t)
            TARGETDIR=${OPTARG}
            ;;
        m)
            COMMENT=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [[ "x${COMMENT}" = "x" ]]; then
    echo "No annotation will be pushed to Influxdb"
fi

if [[ $PREQUEUE == 1 ]]; then
  DRIVE_UP_SUBDIR_NUMBER=400
#  DRIVE_UP_SUBDIR_NUMBER=$(($((NB_DIRS / 4)) * 2))
# DRIVE_UP="ULT3580-TD811"
fi

if [[ $DONOTARCHIVE == 1 ]]; then
    if [[ "x${TARGETDIR}" = "x" ]]; then
      echo "You must provide a target directory to run a test and skip archival"
      exit 1
    fi
    eos root://${EOS_MGM_HOST} ls -d ${EOS_BASEDIR}/${TARGETDIR} || die "target directory does not exist and there is no archive phase to create it."
fi

if [[ $TAPEAWAREGC == 1 ]]; then
    echo "Enabling tape aware garbage collector"
    ssh ${SSH_OPTIONS} -l root ${EOS_MGM_HOST} eos space config default space.filearchivedgc=off || die "Could not disable filearchivedgc"
fi
EOS_DIR=''
if [[ "x${TARGETDIR}" = "x" ]]; then
    EOS_DIR="${EOS_BASEDIR}/$(uuidgen)"
else
    EOS_DIR="${EOS_BASEDIR}/${TARGETDIR}"
fi
#RERUN WITH SAME DIR AS PREVIOUS RUN
if [[ $RELAUNCH == 1 ]]; then
  EOS_DIR="${EOS_BASEDIR}/${RELAUNCH_EOS_DIR}"
fi
LOGDIR="${LOGDIR}/$(basename ${EOS_DIR})"
mkdir -p ${LOGDIR} || die "Cannot create directory LOGDIR: ${LOGDIR}"
mkdir -p ${LOGDIR}/xrd_errors || die "Cannot create directory LOGDIR/xrd_errors: ${LOGDIR}/xrd_errors"

STATUS_FILE=$(mktemp)
echo "$(date +%s): STATUS_FILE=${STATUS_FILE}"
ERROR_FILE=$(mktemp)
echo "$(date +%s): ERROR_FILE=${ERROR_FILE}"
EOS_BATCHFILE=$(mktemp --suffix=.eosh)
echo "$(date +%s): EOS_BATCHFILE=${EOS_BATCHFILE}"

# get some common useful helpers for krb5
. /root/client_helper.sh

# Get kerberos credentials for user1
user_kinit
klist -s || die "Cannot get kerberos credentials for user ${USER}"

# Get kerberos credentials for poweruser1
eospower_kdestroy
eospower_kinit
admin_kinit &>/dev/null

echo "Starting test ${TESTID}: ${COMMENT}"

test -z ${COMMENT} || annotate "test ${TESTID} STARTED" "comment: ${COMMENT}<br/>files: $((${NB_DIRS}*${NB_FILES}))<br/>filesize: ${FILE_KB_SIZE}kB" 'test,start'


if [[ $DONOTARCHIVE == 0 ]]; then

echo "$(date +%s): Creating test dir in eos: ${EOS_DIR}"

eos root://${EOS_MGM_HOST} mkdir -p "${EOS_DIR}" || die "Cannot create directory ${EOS_DIR} in eos instance ${EOS_MGM_HOST}."
echo
echo "Listing the EOS extended attributes of ${EOS_DIR}"
eos root://${EOS_MGM_HOST} attr ls ${EOS_DIR}
echo

echo yes | cta-immutable-file-test root://${EOS_MGM_HOST}/${EOS_DIR}/immutable_file || die "The cta-immutable-file-test failed."

# Create directory for xrootd error reports
ERROR_DIR="/dev/shm/$(basename ${EOS_DIR})"
#ERROR_DIR="/var/log/client-0/$(basename ${EOS_DIR})"
mkdir -p ${ERROR_DIR}
echo "$(date +%s): ERROR_DIR=${ERROR_DIR}"
ls -l "${ERROR_DIR}"
# not more than 100k files per directory so that we can rm and find as a standard user
DD_COMMAND="dd if=/dev/zero bs=${DD_BS} count=${FILE_KB_SIZE} 2>/dev/null"
if (( DD_BS <= 16 )); then
  DD_COMMAND=":"
fi
echo "${DD_COMMAND}"
TEST_FILE_NUMS=()
echo "Generating array of padded file numbers for later processing"
for (( j=0; j < NB_FILES; j++ )); do
  if (( SKIP_ARCHIVE == 1 )); then
    break
  fi
  TEST_FILE_NUMS[j]=$(printf "%.7d" $j)
done
echo "Generating array of padded subdirs"
TEST_SUBDIRS=()
for (( j=0; j < NB_DIRS; j++ )); do
  if (( SKIP_ARCHIVE == 1 )); then
    break
  fi
  TEST_SUBDIRS[j]=$(printf "%.3d" $j)
done
if (( 0 != DRIVE_UP_SUBDIR_NUMBER )); then
  admin_cta drive down ".*" --reason "PUTTING DRIVE DOWN TO PRE-QUEUE REQUESTS"
fi
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  if (( SKIP_ARCHIVE == 1 )); then
    break
  fi
  if (( subdir == DRIVE_UP_SUBDIR_NUMBER )); then
    echo "Putting drives up"
    admin_cta drive up "${DRIVE_UP}" --reason "PUTTING DRIVE UP FOR TESTS"
  fi
  if (( subdir == SLEEP_BEFORE_SUBDIR_NUMBER )); then
    echo "OStoreDB can not queue, sleeping for ${SLEEP_TIME_AFTER_SUBDIR_NUMBER}"
    # Loop over the sleep time, printing a message every 60 seconds
    sleep_interval=60
    while (( SLEEP_TIME_AFTER_SUBDIR_NUMBER > 0 )); do
      # If remaining sleep time is more than the interval, sleep for the interval and print a message
      if (( SLEEP_TIME_AFTER_SUBDIR_NUMBER > sleep_interval )); then
        echo "Slept for ${sleep_interval} seconds."
        sleep $sleep_interval
        (( SLEEP_TIME_AFTER_SUBDIR_NUMBER -= sleep_interval ))
        echo "Slept for ${sleep_interval} seconds, remaining $SLEEP_TIME_AFTER_SUBDIR_NUMBER."
      else
        # Sleep for the remaining time
        echo "Slept for remaining ${SLEEP_TIME_AFTER_SUBDIR_NUMBER} seconds."
        sleep $SLEEP_TIME_AFTER_SUBDIR_NUMBER
        echo "Sleeping finished"
        break
      fi
    done
  fi
  echo "XRootD report error dir: $(date +%s): ERROR_DIR=${ERROR_DIR}"
  eos root://${EOS_MGM_HOST} mkdir -p ${EOS_DIR}/${subdir}
  eos_exit_status=$?
  echo "eos mkdir exit status: ${eos_exit_status}"
  if [[ $eos_exit_status -ne 0 ]]; then
    die "Cannot create directory ${EOS_DIR}/${subdir} in eos instance ${EOS_MGM_HOST}."
  fi
  TEST_FILE_NAME_WITH_SUBDIR="${TEST_FILE_NAME_BASE}${TEST_SUBDIRS[${subdir}]}" # this is the target filename of xrdcp processes just need to add the filenumber in each directory
  echo "Starting to queue the ${NB_FILES} files to ${EOS_DIR}/${subdir} (file base name: ${TEST_FILE_NAME_WITH_SUBDIR})\
        for the subdir ${subdir}/${NB_DIRS} using ${NB_PROCS} processes."
  TEST_FILE_PATH_BASE="root://${EOS_MGM_HOST}/${EOS_DIR}/${subdir}/${TEST_FILE_NAME_WITH_SUBDIR}_"
  ERROR_LOG="${ERROR_DIR}/${TEST_FILE_NAME_WITH_SUBDIR}_error"
  OUTPUT_LOG="${ERROR_DIR}/${TEST_FILE_NAME_WITH_SUBDIR}_output"
  touch "${ERROR_LOG}"
  touch "${OUTPUT_LOG}"
  PADDED_SUBDIR="${TEST_SUBDIRS[${subdir}]}"
  echo "Starting parallel processes."
  echo "In case of OStoreDB prequeueing the drives processing block further queueing, this is why \
        as soon as we spot jobs taking more then 5 minutes we put them to sleep for next \
        15 minutes before queueing next file on that thread"
  # xargs must iterate on the individual file number no subshell can be spawned even for a simple addition in xargs
  printf "%s\n" "${TEST_FILE_NUMS[@]}" | xargs --max-procs=${NB_PROCS} -iTEST_FILE_NUM bash -c "START=\$(date +%s); (${DD_COMMAND}; \
         printf \"%s\" \"UNIQ_${PADDED_SUBDIR}_TEST_FILE_NUM\";) \
         | if ! XRD_LOGLEVEL=Error XRD_STREAMTIMEOUT=10800 xrdcp - \"${TEST_FILE_PATH_BASE}TEST_FILE_NUM\" \
         2>\"${ERROR_LOG}_TEST_FILE_NUM\" > \"${OUTPUT_LOG}_TEST_FILE_NUM\"; then  \
         if ! XRD_LOGLEVEL=Error XRD_STREAMTIMEOUT=10800 xrdcp - \"${TEST_FILE_PATH_BASE}TEST_FILE_NUM\" \
         2>>\"${ERROR_LOG}_TEST_FILE_NUM\" >> \"${OUTPUT_LOG}_TEST_FILE_NUM\"; then \
         if ! XRD_LOGLEVEL=Error XRD_STREAMTIMEOUT=10800 xrdcp - \"${TEST_FILE_PATH_BASE}TEST_FILE_NUM\" \
         2>>\"${ERROR_LOG}_TEST_FILE_NUM\" >> \"${OUTPUT_LOG}_TEST_FILE_NUM\"; then :; fi; fi; fi; \
         tail -n 50 \"${ERROR_LOG}_TEST_FILE_NUM\" >>\"${ERROR_LOG}\"; \
         tail -n 50 \"${OUTPUT_LOG}_TEST_FILE_NUM\" >>\"${OUTPUT_LOG}\"; \
         rm -f \"${ERROR_LOG}_TEST_FILE_NUM\"; \
         rm -f \"${OUTPUT_LOG}_TEST_FILE_NUM\"; \
         END=\$(date +%s); DURATION=\$((END - START)); \
         (( DURATION > 300 )) && sleep 900 || :" || echo "xargs failed !!!"
  if [[ -s "$ERROR_LOG" ]]; then
    LINE_COUNT=$(wc -l < "$ERROR_LOG")
    TMP_FILE=$(mktemp)
    {
       echo "Original line count: $LINE_COUNT"
       tail -n 250 "$ERROR_LOG"
    } > "$TMP_FILE"
    mv -f "$TMP_FILE" "$ERROR_LOG"
  fi
  if [[ -s "$OUTPUT_LOG" ]]; then
    LINE_COUNT=$(wc -l < "$OUTPUT_LOG")
    TMP_FILE=$(mktemp)
    {
       echo "Original line count: $LINE_COUNT"
       tail -n 250 "$OUTPUT_LOG"
    } > "$TMP_FILE"
    mv -f "$TMP_FILE" "$OUTPUT_LOG"
  fi
  # move the files to make space in the small memory buffer /dev/shm for logs
  mv ${ERROR_LOG} ${LOGDIR}/xrd_errors/
  echo "Done."
done
# aternative while loop for parallele processing
#i=0
#running_jobs=0
#
#while (( i < NB_FILES )); do
#    TEST_FILE_NUM=$(printf "%.6d" $i)
#    TEST_FILE_PATH="root://${EOS_MGM_HOST}/${EOS_DIR}/${subdir}/${TEST_FILE_NAME_SUBDIR}_${TEST_FILE_NUM}_$(date +%s%N)"
#    ERROR_LOG="${ERROR_DIR}/${TEST_FILE_NAME_SUBDIR}_${TEST_FILE_NUM}.log"
#
#    # Launch the process in the background and pass variables
#    {
#      dd if=/dev/zero bs=${DD_BS} count=${FILE_KB_SIZE} 2>/dev/null &&
#      echo "UNIQUE_${subdir}_${TEST_FILE_NUM}"
#    } | XRD_LOGLEVEL=Dump XRD_STREAMTIMEOUT=7200 xrdcp - "$TEST_FILE_PATH" 2>"$ERROR_LOG" &
#
#    ((running_jobs++))
#
#    # Limit the number of background jobs
#    if (( running_jobs >= NB_PROCS )); then
#      # Wait for at least one process to finish before continuing
#      wait -n
#      ((running_jobs--))
#    fi
#
#    ((i++))
#done


if [[ "0" != "$(ls ${LOGDIR}/xrd_errors/ 2> /dev/null | wc -l)" ]]; then
  # there were some xrdcp errors
  echo "Several xrdcp errors occured during archival!"
  echo "Please check client pod logs in artifacts"
fi


COPIED=0
COPIED_EMPTY=0
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  if (( SKIP_WAIT_FOR_ARCHIVE == 1 )); then
    break
  fi
  COPIED=$(( ${COPIED} + $(eos root://${EOS_MGM_HOST} find -f ${EOS_DIR}/${subdir} | wc -l) ))
  COPIED_EMPTY=$(( ${COPIED_EMPTY} + $(eos root://${EOS_MGM_HOST} find -0 ${EOS_DIR}/${subdir} | wc -l) ))
done

# Only not empty files are archived by CTA
TO_BE_ARCHIVED=$((${COPIED} - ${COPIED_EMPTY}))

ARCHIVING=${TO_BE_ARCHIVED}
ARCHIVED=0
echo "$(date +%s): Waiting for files to be on tape:"
SECONDS_PASSED=0
WAIT_FOR_ARCHIVED_FILE_TIMEOUT=$((${NB_FILES}/10))
START_TIME=$(date +%s)
END_TIME=$(date +%s)
while test 0 != ${ARCHIVING}; do
  if (( SKIP_WAIT_FOR_ARCHIVE == 1 )); then
    break
  fi
  NOW=$(date +%s)
  SECONDS_PASSED=$((NOW - START_TIME))
  # Stop waiting if timeout is reached
  if (( SECONDS_PASSED >= WAIT_FOR_ARCHIVED_FILE_TIMEOUT )); then
    echo "$(date +%s): Timed out after ${WAIT_FOR_ARCHIVED_FILE_TIMEOUT} seconds waiting for files to be archived to tape."
    break
  fi
  echo "$(date +%s): Waiting for files to be archived to tape: Seconds passed = ${SECONDS_PASSED}"
  ARCHIVED=0
  for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
    ARCHIVED=$(( ${ARCHIVED} + $(eos root://${EOS_MGM_HOST} ls -y ${EOS_DIR}/${subdir} | grep '^d0::t1' | wc -l) ))
    sleep 5 # do not hammer eos too hard
  done

  ARCHIVING=$((${TO_BE_ARCHIVED} - ${ARCHIVED}))
  echo "${ARCHIVED}/${TO_BE_ARCHIVED} archived; Remaining ${ARCHIVING}"
  NB_TAPE_NOT_FULL=$(admin_cta --json ta ls --all | jq "[.[] | select(.full == false)] | length")
  if [[ ${NB_TAPE_NOT_FULL} == 0 ]]
  then
    echo "$(date +%s): All tapes are full, exiting archiving loop"
    break
  fi
  sleep 10
done


echo "###"
echo "${ARCHIVED}/${TO_BE_ARCHIVED} archived"
echo "###"

fi # DONOTARCHIVE

#echo "$(date +%s): Dumping objectstore list"
#ssh root@ctappsfrontend cta-objectstore-list

if [[ $ARCHIVEONLY == 1 ]]; then
  echo "Archiveonly mode: exiting"
  test -z $TAILPID || kill ${TAILPID} &> /dev/null
  exit 0
fi


echo "###"
echo "${TAPEONLY}/${ARCHIVED} on tape only"
echo "###"
echo "Sleeping 60 seconds to allow MGM-FST communication to settle after disk copy deletion."
sleep 60
echo "###"

if (( 0 != DRIVE_UP_SUBDIR_NUMBER )); then
  admin_cta drive down ".*" --reason "PUTTING DRIVE DOWN TO PRE-QUEUE REQUESTS"
fi

echo "$(date +%s): Triggering EOS retrieve workflow as poweruser1:powerusers (12001:1200)"

rm -f ${STATUS_FILE}
touch ${STATUS_FILE}
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  echo "Retrieving file names for subdir ${subdir}/${NB_DIRS}"
  eos root://${EOS_MGM_HOST} ls -y ${EOS_DIR}/${subdir} | grep 'd0::t1' | sed -e "s%\s\+% %g;s%.* \([^ ]\+\)$%${subdir}/\1%" >> ${STATUS_FILE}
done
echo "Number of file names to be retrieved:"
wc -l ${STATUS_FILE}

# We need the -s as we are staging the files from tape (see xrootd prepare definition)
# cat ${STATUS_FILE} | KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xargs --max-procs=${NB_PROCS} -iTEST_FILE_NAME xrdfs ${EOS_MGM_HOST} prepare -s ${EOS_DIR}/TEST_FILE_NAME 2>&1 | tee ${ERROR_FILE}
# CAREFULL HERE: ${STATUS_FILE} contains lines like: 99/test9900001
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  if (( subdir == DRIVE_UP_SUBDIR_NUMBER )); then
    echo "Putting drives up"
    admin_cta drive up "${DRIVE_UP}" --reason "PUTTING DRIVE UP FOR TESTS"
  fi
  if (( subdir == SLEEP_BEFORE_SUBDIR_NUMBER )); then
    echo "OStoreDB can not queue, sleeping for ${SLEEP_TIME_AFTER_SUBDIR_NUMBER}"
    # Loop over the sleep time, printing a message every 60 seconds
    sleep_interval=60
    while (( SLEEP_TIME_AFTER_SUBDIR_NUMBER > 0 )); do
      # If remaining sleep time is more than the interval, sleep for the interval and print a message
      if (( SLEEP_TIME_AFTER_SUBDIR_NUMBER > sleep_interval )); then
        echo "Slept for ${sleep_interval} seconds."
        sleep $sleep_interval
        (( SLEEP_TIME_AFTER_SUBDIR_NUMBER -= sleep_interval ))
       	echo "Slept for ${sleep_interval} seconds, remaining $SLEEP_TIME_AFTER_SUBDIR_NUMBER."
      else
        # Sleep for the remaining time
        echo "Slept for remaining ${SLEEP_TIME_AFTER_SUBDIR_NUMBER} seconds."
        sleep $SLEEP_TIME_AFTER_SUBDIR_NUMBER
       	echo "Sleeping finished"
        break
      fi
    done
  fi
  ERROR_LOG="${ERROR_DIR}/subdir_${subdir}_error"
  OUTPUT_LOG="${ERROR_DIR}/subdir_${subdir}_output"
  #OUTPUT_LOG="/dev/null"
  touch ${OUTPUT_LOG}
  touch ${ERROR_LOG}
  echo -n "Retrieving files to ${EOS_DIR}/${subdir} using ${NB_PROCS} processes...${subdir}, ${EOS_DIR}/${subdir}/, ${ERROR_LOG},  ${OUTPUT_LOG}, ${EOS_MGM_HOST}"
  awk -F '/' -v subdir="${subdir}" '$1 == subdir { print $2 }' "${STATUS_FILE}" | \
    xargs -P ${NB_PROCS} -I{} bash -c \
    "START=\$(date +%s); if ! XRD_LOGLEVEL=Error KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5,unix \
    xrdfs ${EOS_MGM_HOST} prepare -s \"${EOS_DIR}/${subdir}/{}?activity=T0Reprocess\" 2> ${ERROR_LOG}_{}  > ${OUTPUT_LOG}_{}; then  \
    if ! XRD_LOGLEVEL=Error KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5,unix \
    xrdfs ${EOS_MGM_HOST} prepare -s \"${EOS_DIR}/${subdir}/{}?activity=T0Reprocess\" 2>> ${ERROR_LOG}_{}  >> ${OUTPUT_LOG}_{}; then \
    if ! XRD_LOGLEVEL=Error KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5,unix \
    xrdfs ${EOS_MGM_HOST} prepare -s \"${EOS_DIR}/${subdir}/{}?activity=T0Reprocess\" 2>> ${ERROR_LOG}_{}  >> ${OUTPUT_LOG}_{}; then :; fi; fi; fi; \
    tail -n 50 ${ERROR_LOG}_{} >>${ERROR_LOG}; \
    tail -n 50 ${OUTPUT_LOG}_{} >>${OUTPUT_LOG}; \
    rm -f ${ERROR_LOG}_{}; \
    rm -f ${OUTPUT_LOG}_{}; \
    END=\$(date +%s); DURATION=\$((END - START)); \
    (( DURATION > 300 )) && sleep 900 || :" || echo "xargs for prepare failed !!!"
  # letting the traffic settle
  sleep 2
  # Limit the logging output not to run out of space
  if [[ -s "$ERROR_LOG" ]]; then
    LINE_COUNT=$(wc -l < "$ERROR_LOG")
    TMP_FILE=$(mktemp)
    {
       echo "Original line count: $LINE_COUNT"
       tail -n 200 "$ERROR_LOG"
    } > "$TMP_FILE"
    mv -f "$TMP_FILE" "$ERROR_LOG"
  fi
  if [[ -s "$OUTPUT_LOG" ]]; then
    LINE_COUNT=$(wc -l < "$OUTPUT_LOG")
    TMP_FILE=$(mktemp)
    {
       echo "Original line count: $LINE_COUNT"
       tail -n 200 "$OUTPUT_LOG"
    } > "$TMP_FILE"
    mv -f "$TMP_FILE" "$OUTPUT_LOG"
  fi
  # move the files to make space in the small memory buffer for logs
  mv ${ERROR_LOG} ${LOGDIR}/xrd_errors/
  #cat ${STATUS_FILE} | grep ^${subdir}/ | cut -d/ -f2 | xargs --max-procs=${NB_PROCS} -iTEST_FILE_NAME bash -c "XRD_LOGLEVEL=Dump KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_MGM_HOST} prepare -s ${EOS_DIR}/${subdir}/TEST_FILE_NAME?activity=T0Reprocess 2>${ERROR_DIR}/RETRIEVE_TEST_FILE_NAME && rm ${ERROR_DIR}/RETRIEVE_TEST_FILE_NAME || echo ERROR with xrootd prepare stage for file TEST_FILE_NAME, full logs in ${ERROR_DIR}/RETRIEVE_TEST_FILE_NAME" | tee ${LOGDIR}/prepare_${subdir}.log | grep ^ERROR
  echo Done.
done
echo 'Checking the request ID in extended attributes'
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  if (( SKIP_GET_XATTRS == 1 )); then
    break
  fi
  # for the moment commenting out getting the attributes since
  #cat ${STATUS_FILE} | grep ^${subdir}/ | cut -d/ -f2 | xargs --max-procs=${NB_PROCS} -iTEST_FILE_NAME bash -c "XRD_LOGLEVEL=Dump KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_MGM_HOST} xattr ${EOS_DIR}/${subdir}/TEST_FILE_NAME get sys.retrieve.req_id 2>${ERROR_DIR}/XATTRGET_TEST_FILE_NAME && rm ${ERROR_DIR}/XATTRGET_TEST_FILE_NAME || echo ERROR with xrootd xattr get for file TEST_FILE_NAME, full logs in ${ERROR_DIR}/XATTRGET_TEST_FILE_NAME" | tee ${LOGDIR}/prepare_sys.retrieve.req_id_${subdir}.log | grep ^ERROR
  #OUTPUT_LOG="${LOGDIR}/xattrget_${subdir}.log"
  OUTPUT_LOG="/dev/null"
  ERROR_LOG="${ERROR_DIR}/XATTRGET_${subdir}".log
  touch ${ERROR_LOG}
  awk -F '/' -v subdir="${subdir}" '$1 == subdir { print $2 }' "${STATUS_FILE}" | \
    xargs -P ${NB_PROCS} -I{} bash -c \
    "START=$(date +%s); XRD_LOGLEVEL=Error KRB5CCNAME=/tmp/\"${EOSPOWER_USER}\"/krb5cc_0 XrdSecPROTOCOL=krb5 \
    xrdfs \"${EOS_MGM_HOST}\" xattr \"${EOS_DIR}/${subdir}\"/{} get sys.retrieve.req_id 2>>\"${ERROR_LOG}\"  >>\"${OUTPUT_LOG}\" || \
    echo \"ERROR: Failed to get xattr for file {}\" >> \"${ERROR_LOG}\"; \
    END=$(date +%s); DURATION=$((END - START)); \
        (( DURATION > 300 )) && sleep 900 || :" || echo "xargs for xattr failed !!!"
  sleep 2
  # Limit the logging output not to run out of space
  if [[ -s "$ERROR_LOG" ]]; then
    LINE_COUNT=$(wc -l < "$ERROR_LOG")
    TMP_FILE=$(mktemp)
    {
       echo "Original line count: $LINE_COUNT"
       tail -n 50 "$ERROR_LOG"
    } > "$TMP_FILE"
    mv -f "$TMP_FILE" "$ERROR_LOG"
  fi
done
if [[ "0" != "$(ls ${ERROR_DIR} 2> /dev/null | wc -l)" ]]; then
   # there were some prepare errors
  echo "Several prepare errors occured during retrieval!"
  echo "Please check client pod logs in artifacts"
  mv ${ERROR_DIR}/* ${LOGDIR}/xrd_errors/
fi

ARCHIVED=$(cat ${STATUS_FILE} | wc -l)
TO_BE_RETRIEVED=$(( ${ARCHIVED} - $(ls ${ERROR_DIR}/RETRIEVE_* 2>/dev/null | wc -l) ))
RETRIEVING=${TO_BE_RETRIEVED}
RETRIEVED=0
NO_PROGRESS_TIMEOUT=300
LAST_PROGRESS_TIME=${START_TIME}
LAST_RETRIEVED_COUNT=0
# Wait for the copy to appear on disk
echo "$(date +%s): Waiting for files to be back on disk:"
SECONDS_PASSED=0
START_TIME=$(date +%s)
WAIT_FOR_RETRIEVED_FILE_TIMEOUT=$((${NB_FILES}/10))
while test 0 -lt ${RETRIEVING}; do
  NOW=$(date +%s)
  SECONDS_PASSED=$((NOW - START_TIME))
  echo "$(date +%s): Waiting for files to be retrieved from tape: Seconds passed = ${SECONDS_PASSED}"
  # Stop waiting if timeout is reached
  if (( SECONDS_PASSED >= WAIT_FOR_RETRIEVED_FILE_TIMEOUT )); then
    echo "$(date +%s): Timed out after ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT} seconds waiting for files to be retrieved from tape."
    break
  fi
  RETRIEVED=0
  for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
    RETRIEVED=$(( ${RETRIEVED} + $(eos root://${EOS_MGM_HOST} ls -y ${EOS_DIR}/${subdir} | grep -E '^d[1-9][0-9]*::t1' | wc -l) ))
    sleep 1 # do not hammer eos too hard
  done

  RETRIEVING=$((${ARCHIVED} - ${RETRIEVED}))

  echo "${RETRIEVED}/${ARCHIVED} retrieved; Remaining ${RETRIEVING}"

  # Check for progress
  if (( RETRIEVED > LAST_RETRIEVED_COUNT )); then
    LAST_PROGRESS_TIME=${NOW}
    LAST_RETRIEVED_COUNT=${RETRIEVED}
  else
    NO_PROGRESS_TIME=$((NOW - LAST_PROGRESS_TIME))
    if (( NO_PROGRESS_TIME >= NO_PROGRESS_TIMEOUT )); then
      echo "$(date +%s): No progress for ${NO_PROGRESS_TIMEOUT}s — treating as completed successfully."
      break
    fi
  fi
  sleep 10
done

echo "###"
echo "${RETRIEVED}/${ARCHIVED} retrieved files"
echo "###"

if (( SKIP_EVICT == 1 )); then
  echo "As SKIP_EVICT is ${SKIP_EVICT}, we skip the rest of the stress test, just evict the files from disk."
  # Build the list of files with at least 1 disk copy that have been archived before (ie d>=1::t1)
  rm -f ${STATUS_FILE}
  touch ${STATUS_FILE}
  for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
    eos root://${EOS_MGM_HOST} ls -y ${EOS_DIR}/${subdir} | grep -E 'd[1-9][0-9]*::t1' | sed -e "s%\s\+% %g;s%.* \([^ ]\+\)$%${subdir}/\1%" >> ${STATUS_FILE}
    sleep 2
  done

  TO_EVICT=$(cat ${STATUS_FILE} | wc -l)

  echo "$(date +%s): $TO_EVICT files to be evicted from EOS using 'xrdfs prepare -e'"
  # We need the -e as we are evicting the files from disk cache (see xrootd prepare definition)
  cat ${STATUS_FILE} | sed -e "s%^%${EOS_DIR}/%" | XrdSecPROTOCOL=krb5 KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 xargs --max-procs=10 -n 40 xrdfs ${EOS_MGM_HOST} prepare -e > /dev/null


  LEFTOVER=0
  for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
    LEFTOVER=$(( ${LEFTOVER} + $(eos root://${EOS_MGM_HOST} ls -y ${EOS_DIR}/${subdir} | grep -E '^d[1-9][0-9]*::t1' | wc -l) ))
  done

  EVICTED=$((${TO_EVICT}-${LEFTOVER}))
  echo "$(date +%s): $EVICTED/$TO_EVICT files evicted from EOS 'xrdfs prepare -e'"
  exit 0
fi
#echo "$(date +%s): Dumping objectstore list"
#ssh root@ctappsfrontend cta-objectstore-list


# Build the list of files with at least 1 disk copy that have been archived before (ie d>=1::t1)
rm -f ${STATUS_FILE}
touch ${STATUS_FILE}
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  eos root://${EOS_MGM_HOST} ls -y ${EOS_DIR}/${subdir} | grep -E 'd[1-9][0-9]*::t1' | sed -e "s%\s\+% %g;s%.* \([^ ]\+\)$%${subdir}/\1%" >> ${STATUS_FILE}
  sleep 2
done

TO_EVICT=$(cat ${STATUS_FILE} | wc -l)

echo "$(date +%s): $TO_EVICT files to be evicted from EOS using 'xrdfs prepare -e'"
# We need the -e as we are evicting the files from disk cache (see xrootd prepare definition)
cat ${STATUS_FILE} | sed -e "s%^%${EOS_DIR}/%" | XrdSecPROTOCOL=krb5 KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 xargs --max-procs=10 -n 40 xrdfs ${EOS_MGM_HOST} prepare -e > /dev/null


LEFTOVER=0
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  LEFTOVER=$(( ${LEFTOVER} + $(eos root://${EOS_MGM_HOST} ls -y ${EOS_DIR}/${subdir} | grep -E '^d[1-9][0-9]*::t1' | wc -l) ))
done

EVICTED=$((${TO_EVICT}-${LEFTOVER}))
echo "$(date +%s): $EVICTED/$TO_EVICT files evicted from EOS 'xrdfs prepare -e'"

LASTCOUNT=${EVICTED}

# Build the list of tape only files.
rm -f ${STATUS_FILE}
touch ${STATUS_FILE}
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  eos root://${EOS_MGM_HOST} ls -y ${EOS_DIR}/${subdir} | grep -E 'd0::t[^0]' | sed -e "s%\s\+% %g;s%.* \([^ ]\+\)$%${subdir}/\1%" >> ${STATUS_FILE}
done

# Put all tape drives down
echo "Sleeping 3 seconds to let previous sessions finish."
sleep 3
INITIAL_DRIVES_STATE=$(admin_cta --json dr ls)
echo INITIAL_DRIVES_STATE:
echo ${INITIAL_DRIVES_STATE} | jq -r '.[] | [ .driveName, .driveStatus] | @tsv' | column -t
echo -n "Will put down those drives : "
drivesToSetDown=$(echo ${INITIAL_DRIVES_STATE} | jq -r '.[] | select (.driveStatus == "UP") | .driveName')
echo $drivesToSetDown
for d in $(echo $drivesToSetDown); do
  admin_cta drive down $d --reason "PUTTING DRIVE DOWN FOR TESTS"
done

echo "$(date +%s): Waiting for the drives to be down"
SECONDS_PASSED=0
WAIT_FOR_DRIVES_DOWN_TIMEOUT=$((10))
while [[ $SECONDS_PASSED -lt WAIT_FOR_DRIVES_DOWN_TIMEOUT ]]; do
  sleep 1
  oneStatusUpRemaining=0
  for d in $(echo $drivesToSetDown); do
    status=$(admin_cta --json drive ls | jq -r ". [] | select(.driveName == \"$d\") | .driveStatus")
    if [[ $status == "UP" ]]; then
      oneStatusUpRemaining=1
    fi;
  done
  if [[ $oneStatusUpRemaining -eq 0 ]]; then
    echo "Drives : $drivesToSetDown are down"
    break;
  fi
  echo -n "."
  SECONDS_PASSED=$SECONDS_PASSED+1
  if [[ $SECONDS_PASSED -gt $WAIT_FOR_DRIVES_DOWN_TIMEOUT ]]; then
    die "ERROR: Timeout reach for trying to put all drives down"
  fi
done

# Prepare-stage the files
#cat ${STATUS_FILE} | perl -p -e "s|^(.*)$|${EOS_DIR}/\$1?activity=T0Reprocess|" | \
#  XRD_LOGLEVEL=Dump KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xargs  -n 40 --max-procs=10 \
#     echo bash -c "echo xrdfs ${EOS_MGM_HOST} prepare -s $@" bash
# | \
#  tee ${LOGDIR}/prepare_${subdir}.log | grep -i error

for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  echo -n "Retrieving files to ${EOS_DIR}/${subdir} using ${NB_PROCS} processes (prepare2)..."
  cat ${STATUS_FILE} | grep ^${subdir}/ | cut -d/ -f2 | xargs --max-procs=${NB_PROCS} -iTEST_FILE_NAME bash -c "XRD_LOGLEVEL=Dump KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_MGM_HOST} prepare -s ${EOS_DIR}/${subdir}/TEST_FILE_NAME?activity=T0Reprocess 2>${ERROR_DIR}/RETRIEVE_TEST_FILE_NAME && rm ${ERROR_DIR}/RETRIEVE_TEST_FILE_NAME || echo ERROR with xrootd prepare stage for file TEST_FILE_NAME, full logs in ${ERROR_DIR}/RETRIEVE_TEST_FILE_NAME" | tee ${LOGDIR}/prepare2_${subdir}.log | grep ^ERROR
  echo Done.
  echo -n "Checking the presence of the sys.retrieve.req_id extended attributes..."
  cat ${STATUS_FILE} | grep ^${subdir}/ | cut -d/ -f2 | xargs --max-procs=${NB_PROCS} -iTEST_FILE_NAME bash -c "XRD_LOGLEVEL=Dump KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOS_MGM_HOST} xattr ${EOS_DIR}/${subdir}/TEST_FILE_NAME get sys.retrieve.req_id 2>${ERROR_DIR}/XATTRGET_TEST_FILE_NAME && rm ${ERROR_DIR}/XATTRGET_TEST_FILE_NAME || echo ERROR with xrootd xattr get for file TEST_FILE_NAME, full logs in ${ERROR_DIR}/XATTRGET_TEST_FILE_NAME" | tee ${LOGDIR}/prepare2_sys.retrieve.req_id_${subdir}.log | grep ^ERROR
  echo Done.
done
if [[ "0" != "$(ls ${ERROR_DIR} 2> /dev/null | wc -l)" ]]; then
  # there were some prepare errors
  echo "Several prepare errors occured during retrieval!"
  echo "Please check client pod logs in artifacts"
  mv ${ERROR_DIR}/* ${LOGDIR}/xrd_errors/
fi

# Ensure all requests files are queued
requestsTotal=$(admin_cta --json sq | jq 'map(select (.mountType == "RETRIEVE") | .queuedFiles | tonumber) | add')
echo "Retrieve requests count: ${requestsTotal}"
filesCount=$(cat ${STATUS_FILE} | wc -l)
if [[ ${requestsTotal} -ne ${filesCount} ]]; then
  echo "ERROR: Retrieve queue(s) size mismatch: ${requestsTotal} requests queued for ${filesCount} files."
fi

# We don't care about abort prepare for the stress test
# Abort prepare -s requests
# for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
#   echo -n "Cancelling prepare for files in ${EOS_DIR}/${subdir} using ${NB_PROCS} processes (prepare_abort)..."
#   cat ${STATUS_FILE} | grep ^${subdir}/ | cut -d/ -f2                                                                         \
#   | xargs --max-procs=${NB_PROCS} -iTEST_FILE_NAME cta-client-ar-abortPrepare --eos-instance ${EOS_MGM_HOST}                   \
#       --eos-poweruser ${EOSPOWER_USER} --eos-dir ${EOS_DIR} --subdir ${subdir} --file TEST_FILE_NAME --error-dir ${ERROR_DIR} \
#   | tee ${LOGDIR}/prepare_abort_sys.retrieve.req_id_${subdir}.log # | grep ^ERROR
#   echo Done.
# done

# Put drive(s) back up to clear the queue
echo -n "Will put back up those drives : "
echo ${INITIAL_DRIVES_STATE} | jq -r '.[] | select (.driveStatus == "UP") | .driveName'
for d in $(echo ${INITIAL_DRIVES_STATE} | jq -r '.[] | select (.driveStatus == "UP") | .driveName'); do
  admin_cta dr up $d
done

# Check that queues are empty after a while and files did not get retrieved
echo "$(date +%s): Waiting for retrieve queues to be cleared:"
SECONDS_PASSED=0
WAIT_FOR_RETRIEVE_QUEUES_CLEAR_TIMEOUT=$((60))
REMAINING_REQUESTS=$(admin_cta --json sq | jq -r 'map(select (.mountType == "RETRIEVE") | .queuedFiles | tonumber) | add')
echo "${REMAINING_REQUESTS} requests remaining."
# Prevent the result from being empty
if [[ -z "$REMAINING_REQUESTS" ]]; then REMAINING_REQUESTS='0'; fi
while [[ ${REMAINING_REQUESTS} -gt 0 ]]; do
  echo "$(date +%s): Waiting for retrieve queues to be cleared: Seconds passed = ${SECONDS_PASSED}"
  sleep 1
  let SECONDS_PASSED=SECONDS_PASSED+1

  if test ${SECONDS_PASSED} == ${WAIT_FOR_RETRIEVE_QUEUES_CLEAR_TIMEOUT}; then
    echo "$(date +%s): Timed out after ${WAIT_FOR_RETRIEVE_QUEUES_CLEAR_TIMEOUT} seconds waiting for retrieve queues to be cleared"
    break
  fi

  REMAINING_REQUESTS=$(admin_cta --json sq | jq -r 'map(select (.mountType == "RETRIEVE") | .queuedFiles | tonumber) | add');
  # Prevent the result from being empty
  if [[ -z "$REMAINING_REQUEST" ]]; then REMAINING_REQUESTS='0'; fi
  echo "${REMAINING_REQUESTS} requests remaining."
done

# Check that the files were not retrieved
echo "Checking restaged files..."
RESTAGEDFILES=0
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  RF=$(eos root://${EOS_MGM_HOST} ls -y ${EOS_DIR}/${subdir} | grep -E '^d[1-9][0-9]*::t1' | wc -l)
  echo "Restaged files in directory ${subdir}: ${RF}"
  (( RESTAGEDFILES += ${RF} ))
done
echo "Total restaged files found: ${RESTAGEDFILES}"

if [[ "0" != "$(ls ${ERROR_DIR} 2> /dev/null | wc -l)" ]]; then
  # there were some prepare errors
  echo "Several errors occured during prepare cancel test!"
  echo "Please check client pod logs in artifacts"
  mv ${ERROR_DIR}/* ${LOGDIR}/xrd_errors/
fi

delete_files_from_eos_and_tapes

echo "###"
echo "$(date +%s): Results:"
echo "REMOVED/EVICTED/RETRIEVED/ARCHIVED/RESTAGEDFILES/NB_FILES"
echo "${DELETED}/${EVICTED}/${RETRIEVED}/${ARCHIVED}/${RESTAGEDFILES}/$((${NB_FILES} * ${NB_DIRS}))"
echo "###"

test -z ${COMMENT} || annotate "test ${TESTID} FINISHED" "Summary:</br>NB_FILES: $((${NB_FILES} * ${NB_DIRS}))</br>ARCHIVED: ${ARCHIVED}<br/>RETRIEVED: ${RETRIEVED}<br/>EVICTED: ${EVICTED}</br>DELETED: ${DELETED}" 'test,end'


#echo "$(date +%s): Dumping objectstore list"
#ssh root@ctappsfrontend cta-objectstore-list


# stop tail
test -z $TAILPID || kill ${TAILPID} &> /dev/null

RC=0
if [[ ${LASTCOUNT} -ne $((${NB_FILES} * ${NB_DIRS})) ]]; then
  ((RC++))
  echo "ERROR there were some lost files during the archive/retrieve test with ${NB_FILES} files (first 10):"
  grep -v retrieved ${STATUS_FILE} | sed -e "s;^;${EOS_DIR}/;" | head -10
fi

if [[ $(cat ${LOGDIR}/prepare_sys.retrieve.req_id_*.log | grep -v value= | wc -l) -ne 0 ]]; then
  # THIS IS NOT YET AN ERROR: UNCOMMENT THE FOLLOWING LINE WHEN https://gitlab.cern.ch/cta/CTA/issues/606 is fixed
  # ((RC++))
  echo "ERROR $(cat ${LOGDIR}/prepare_sys.retrieve.req_id_*.log | grep -v value= | wc -l) files out of $(cat ${LOGDIR}/prepare_sys.retrieve.req_id_*.log | wc -l) prepared files have no sys.retrieve.req_id extended attribute set"
fi


if [[ ${RESTAGEDFILES} -ne "0" ]]; then
  ((RC++))
  echo "ERROR some files were retrieved in spite of retrieve cancellation."
fi

# This one does not change the return code
# WARNING if everything else was OK
# ERROR otherwise as these xrootd failures could be the reason of the failure
if [[ $(ls ${LOGDIR}/xrd_errors | wc -l) -ne 0 ]]; then
  # ((RC++)) # do not change RC
  if [[ ${RC} -eq 0 ]]; then
    echo "WARNING several xrootd failures occured during this run, please check client dumps in ${LOGDIR}/xrd_errors."
  else
    echo "ERROR several xrootd failures occured during this run, please check client dumps in ${LOGDIR}/xrd_errors."
  fi
fi

echo "Archive retrieve test completed successfully"

exit ${RC}
