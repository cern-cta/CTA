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


EOSINSTANCE=ctaeos
EOS_BASEDIR=/eos/ctaeos/cta
TEST_FILE_NAME_BASE=test
DATA_SOURCE=/dev/urandom
ARCHIVEONLY=0 # Only archive files or do the full test?
DONOTARCHIVE=0 # files were already archived in a previous run NEED TARGETDIR
TARGETDIR=''
LOGDIR='/var/log'

COMMENT=''
# id of the test so that we can track it
TESTID="$(date +%y%m%d%H%M)"

NB_PROCS=1
NB_FILES=1
NB_DIRS=1
FILE_KB_SIZE=1
VERBOSE=0
REMOVE=0
TAILPID=''
TAPEAWAREGC=0

NB_BATCH_PROCS=500  # number of parallel batch processes
BATCH_SIZE=20    # number of files per batch process

SSH_OPTIONS='-o BatchMode=yes -o ConnectTimeout=10'

die() {
  echo "$@" 1>&2
  test -z $TAILPID || kill ${TAILPID} &> /dev/null
  exit 1
}


usage() { cat <<EOF 1>&2
Usage: $0 [-n <nb_files_perdir>] [-N <nb_dir>] [-s <file_kB_size>] [-p <# parallel procs>] [-v] [-d <eos_dest_dir>] [-e <eos_instance>] [-S <data_source_file>] [-r]
  -v		Verbose mode: displays live logs of rmcd to see tapes being mounted/dismounted in real time
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
  eos root://${EOSINSTANCE} find --fid ${EOS_DIR} |\
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


while getopts "d:e:n:N:s:p:vS:rAPGt:m:" o; do
    case "${o}" in
        e)
            EOSINSTANCE=${OPTARG}
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
        S)
            DATA_SOURCE=${OPTARG}
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

if [ ! -z "${error}" ]; then
    echo -e "ERROR:\n${error}"
    exit 1
fi

if [ "x${COMMENT}" = "x" ]; then
    echo "No annotation will be pushed to Influxdb"
fi

if [[ $DONOTARCHIVE == 1 ]]; then
    if [[ "x${TARGETDIR}" = "x" ]]; then
      echo "You must provide a target directory to run a test and skip archival"
      exit 1
    fi
    eos root://${EOSINSTANCE} ls -d ${EOS_BASEDIR}/${TARGETDIR} || die "target directory does not exist and there is no archive phase to create it."
fi

if [[ $TAPEAWAREGC == 1 ]]; then
    echo "Enabling tape aware garbage collector"
    ssh ${SSH_OPTIONS} -l root ${EOSINSTANCE} eos space config default space.filearchivedgc=off || die "Could not disable filearchivedgc"
fi

EOS_DIR=''
if [[ "x${TARGETDIR}" = "x" ]]; then
    EOS_DIR="${EOS_BASEDIR}/$(uuidgen)"
else
    EOS_DIR="${EOS_BASEDIR}/${TARGETDIR}"
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

# As we are skipping n bytes per file we need a bit more than the file size to accomodate dd to read ${FILE_KB_SIZE} skipping the n first bytes
dd if=/dev/urandom of=/tmp/testfile bs=1k count=$((${FILE_KB_SIZE} + ${NB_FILES}*${NB_DIRS}/1024 + 1)) || exit 1

if [[ $VERBOSE == 1 ]]; then
  tail -v -f /mnt/logs/tpsrv0*/rmcd/cta/cta-rmcd.log &
  TAILPID=$!
fi

# get some common useful helpers for krb5
. /root/client_helper.sh

# Get kerberos credentials for user1
user_kinit
klist -s || die "Cannot get kerberos credentials for user ${USER}"

# Get kerberos credentials for poweruser1
eospower_kdestroy
eospower_kinit

echo "Starting test ${TESTID}: ${COMMENT}"

#echo "$(date +%s): Dumping objectstore list"
#ssh root@ctappsfrontend cta-objectstore-list

test -z ${COMMENT} || annotate "test ${TESTID} STARTED" "comment: ${COMMENT}<br/>files: $((${NB_DIRS}*${NB_FILES}))<br/>filesize: ${FILE_KB_SIZE}kB" 'test,start'


if [[ $DONOTARCHIVE == 0 ]]; then

echo "$(date +%s): Creating test dir in eos: ${EOS_DIR}"
# uuid should be unique no need to remove dir before...
# XrdSecPROTOCOL=sss eos -r 0 0 root://${EOSINSTANCE} rm -Fr ${EOS_DIR}


eos root://${EOSINSTANCE} mkdir -p ${EOS_DIR} || die "Cannot create directory ${EOS_DIR} in eos instance ${EOSINSTANCE}."

echo
echo "Listing the EOS extended attributes of ${EOS_DIR}"
eos root://${EOSINSTANCE} attr ls ${EOS_DIR}
echo

echo yes | cta-immutable-file-test root://${EOSINSTANCE}/${EOS_DIR}/immutable_file || die "The cta-immutable-file-test failed."

# Create directory for xrootd error reports
ERROR_DIR="/dev/shm/$(basename ${EOS_DIR})"
mkdir ${ERROR_DIR}
echo "$(date +%s): ERROR_DIR=${ERROR_DIR}"
# not more than 100k files per directory so that we can rm and find as a standard user
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  eos root://${EOSINSTANCE} mkdir -p ${EOS_DIR}/${subdir} || die "Cannot create directory ${EOS_DIR}/{subdir} in eos instance ${EOSINSTANCE}."
  echo -n "Copying files to ${EOS_DIR}/${subdir} using ${NB_PROCS} processes..."
  TEST_FILE_NAME_SUBDIR=${TEST_FILE_NAME_BASE}$(printf %.2d ${subdir}) # this is the target filename of xrdcp processes just need to add the filenumber in each directory
  # xargs must iterate on the individual file number no subshell can be spawned even for a simple addition in xargs
  for ((i=0;i<${NB_FILES};i++)); do
    echo $(printf %.6d $i)
done | xargs --max-procs=${NB_PROCS} -iTEST_FILE_NUM bash -c "dd if=/tmp/testfile bs=1k 2>/dev/null | (dd bs=$((${subdir}*${NB_FILES})) count=1 of=/dev/null 2>/dev/null; dd bs=TEST_FILE_NUM count=1 of=/dev/null 2>/dev/null; dd bs=1k count=${FILE_KB_SIZE} 2>/dev/null) | XRD_LOGLEVEL=Dump xrdcp - root://${EOSINSTANCE}/${EOS_DIR}/${subdir}/${TEST_FILE_NAME_SUBDIR}TEST_FILE_NUM 2>${ERROR_DIR}/${TEST_FILE_NAME_SUBDIR}TEST_FILE_NUM && rm ${ERROR_DIR}/${TEST_FILE_NAME_SUBDIR}TEST_FILE_NUM || echo ERROR with xrootd transfer for file ${TEST_FILE_NAME_SUBDIR}TEST_FILE_NUM, full logs in ${ERROR_DIR}/${TEST_FILE_NAME_SUBDIR}TEST_FILE_NUM"
  #done | xargs --max-procs=${NB_PROCS} -iTEST_FILE_NAME xrdcp --silent /tmp/testfile root://${EOSINSTANCE}/${EOS_DIR}/${subdir}/TEST_FILE_NAME
  #  done | xargs -n ${BATCH_SIZE} --max-procs=${NB_BATCH_PROCS} ./batch_xrdcp /tmp/testfile root://${EOSINSTANCE}/${EOS_DIR}/${subdir}
  echo Done.
done
if [ "0" != "$(ls ${ERROR_DIR} 2> /dev/null | wc -l)" ]; then
  # there were some xrdcp errors
  echo "Several xrdcp errors occured during archival!"
  echo "Please check client pod logs in artifacts"
  mv ${ERROR_DIR}/* ${LOGDIR}/xrd_errors/
fi

COPIED=0
COPIED_EMPTY=0
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  COPIED=$(( ${COPIED} + $(eos root://${EOSINSTANCE} find -f ${EOS_DIR}/${subdir} | wc -l) ))
  COPIED_EMPTY=$(( ${COPIED_EMPTY} + $(eos root://${EOSINSTANCE} find -0 ${EOS_DIR}/${subdir} | wc -l) ))
done

# Only not empty files are archived by CTA
TO_BE_ARCHIVED=$((${COPIED} - ${COPIED_EMPTY}))

ARCHIVING=${TO_BE_ARCHIVED}
ARCHIVED=0
echo "$(date +%s): Waiting for files to be on tape:"
SECONDS_PASSED=0
WAIT_FOR_ARCHIVED_FILE_TIMEOUT=$((40+${NB_FILES}/5))
while test 0 != ${ARCHIVING}; do
  echo "$(date +%s): Waiting for files to be archived to tape: Seconds passed = ${SECONDS_PASSED}"
  sleep 3
  let SECONDS_PASSED=SECONDS_PASSED+1

  if test ${SECONDS_PASSED} == ${WAIT_FOR_ARCHIVED_FILE_TIMEOUT}; then
    echo "$(date +%s): Timed out after ${WAIT_FOR_ARCHIVED_FILE_TIMEOUT} seconds waiting for file to be archived to tape"
    break
  fi

  ARCHIVED=0
  for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
    ARCHIVED=$(( ${ARCHIVED} + $(eos root://${EOSINSTANCE} ls -y ${EOS_DIR}/${subdir} | grep '^d0::t1' | wc -l) ))
    sleep 1 # do not hammer eos too hard
  done

  echo "${ARCHIVED}/${TO_BE_ARCHIVED} archived"

  ARCHIVING=$((${TO_BE_ARCHIVED} - ${ARCHIVED}))
  NB_TAPE_NOT_FULL=`admin_cta --json ta ls --all | jq "[.[] | select(.full == false)] | length"`
  if [[ ${NB_TAPE_NOT_FULL} == 0 ]]
  then
    echo "$(date +%s): All tapes are full, exiting archiving loop"
    break
  fi
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
echo "Sleeping 10 seconds to allow MGM-FST communication to settle after disk copy deletion."
sleep 10
echo "###"


echo "$(date +%s): Trigerring EOS retrieve workflow as poweruser1:powerusers (12001:1200)"

rm -f ${STATUS_FILE}
touch ${STATUS_FILE}
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  eos root://${EOSINSTANCE} ls -y ${EOS_DIR}/${subdir} | grep 'd0::t1' | sed -e "s%\s\+% %g;s%.* \([^ ]\+\)$%${subdir}/\1%" >> ${STATUS_FILE}
  # sleep 3 # do not hammer eos too hard
done

# We need the -s as we are staging the files from tape (see xrootd prepare definition)
# cat ${STATUS_FILE} | KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xargs --max-procs=${NB_PROCS} -iTEST_FILE_NAME xrdfs ${EOSINSTANCE} prepare -s ${EOS_DIR}/TEST_FILE_NAME 2>&1 | tee ${ERROR_FILE}
# CAREFULL HERE: ${STATUS_FILE} contains lines like: 99/test9900001
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  echo -n "Retrieving files to ${EOS_DIR}/${subdir} using ${NB_PROCS} processes..."
  cat ${STATUS_FILE} | grep ^${subdir}/ | cut -d/ -f2 | xargs --max-procs=${NB_PROCS} -iTEST_FILE_NAME bash -c "XRD_LOGLEVEL=Dump KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOSINSTANCE} prepare -s ${EOS_DIR}/${subdir}/TEST_FILE_NAME?activity=T0Reprocess 2>${ERROR_DIR}/RETRIEVE_TEST_FILE_NAME && rm ${ERROR_DIR}/RETRIEVE_TEST_FILE_NAME || echo ERROR with xrootd prepare stage for file TEST_FILE_NAME, full logs in ${ERROR_DIR}/RETRIEVE_TEST_FILE_NAME" | tee ${LOGDIR}/prepare_${subdir}.log | grep ^ERROR
  echo Done.
  cat ${STATUS_FILE} | grep ^${subdir}/ | cut -d/ -f2 | xargs --max-procs=${NB_PROCS} -iTEST_FILE_NAME bash -c "XRD_LOGLEVEL=Dump KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOSINSTANCE} query opaquefile ${EOS_DIR}/${subdir}/TEST_FILE_NAME?mgm.pcmd=xattr\&mgm.subcmd=get\&mgm.xattrname=sys.retrieve.req_id 2>${ERROR_DIR}/XATTRGET_TEST_FILE_NAME && rm ${ERROR_DIR}/XATTRGET_TEST_FILE_NAME || echo ERROR with xrootd xattr get for file TEST_FILE_NAME, full logs in ${ERROR_DIR}/XATTRGET_TEST_FILE_NAME" | tee ${LOGDIR}/prepare_sys.retrieve.req_id_${subdir}.log | grep ^ERROR
done
if [ "0" != "$(ls ${ERROR_DIR} 2> /dev/null | wc -l)" ]; then
  # there were some prepare errors
  echo "Several prepare errors occured during retrieval!"
  echo "Please check client pod logs in artifacts"
  mv ${ERROR_DIR}/* ${LOGDIR}/xrd_errors/
fi

ARCHIVED=$(cat ${STATUS_FILE} | wc -l)
TO_BE_RETRIEVED=$(( ${ARCHIVED} - $(ls ${ERROR_DIR}/RETRIEVE_* 2>/dev/null | wc -l) ))
RETRIEVING=${TO_BE_RETRIEVED}
RETRIEVED=0
# Wait for the copy to appear on disk
echo "$(date +%s): Waiting for files to be back on disk:"
SECONDS_PASSED=0
WAIT_FOR_RETRIEVED_FILE_TIMEOUT=$((40+${NB_FILES}/5))
while test 0 -lt ${RETRIEVING}; do
  echo "$(date +%s): Waiting for files to be retrieved from tape: Seconds passed = ${SECONDS_PASSED}"
  sleep 3
  let SECONDS_PASSED=SECONDS_PASSED+1

  if test ${SECONDS_PASSED} == ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT}; then
    echo "$(date +%s): Timed out after ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT} seconds waiting for file to be retrieved tape"
    break
  fi

  RETRIEVED=0
  for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
    RETRIEVED=$(( ${RETRIEVED} + $(eos root://${EOSINSTANCE} ls -y ${EOS_DIR}/${subdir} | egrep '^d[1-9][0-9]*::t1' | wc -l) ))
    sleep 1 # do not hammer eos too hard
  done

  RETRIEVING=$((${TO_BE_RETRIEVED} - ${RETRIEVED}))

  echo "${RETRIEVED}/${TO_BE_RETRIEVED} retrieved"
done

echo "###"
echo "${RETRIEVED}/${TO_BE_RETRIEVED} retrieved files"
echo "###"


#echo "$(date +%s): Dumping objectstore list"
#ssh root@ctappsfrontend cta-objectstore-list


# Build the list of files with more than 1 disk copy that have been archived before (ie d>=1::t1)
rm -f ${STATUS_FILE}
touch ${STATUS_FILE}
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  eos root://${EOSINSTANCE} ls -y ${EOS_DIR}/${subdir} | egrep 'd[1-9][0-9]*::t1' | sed -e "s%\s\+% %g;s%.* \([^ ]\+\)$%${subdir}/\1%" >> ${STATUS_FILE}
done

TO_EVICT=$(cat ${STATUS_FILE} | wc -l)

echo "$(date +%s): $TO_EVICT files to be evicted from EOS using 'xrdfs prepare -e'"
# We need the -e as we are evicting the files from disk cache (see xrootd prepare definition)
cat ${STATUS_FILE} | sed -e "s%^%${EOS_DIR}/%" | XrdSecPROTOCOL=krb5 KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 xargs --max-procs=10 -n 40 xrdfs ${EOSINSTANCE} prepare -e > /dev/null


LEFTOVER=0
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  LEFTOVER=$(( ${LEFTOVER} + $(eos root://${EOSINSTANCE} ls -y ${EOS_DIR}/${subdir} | egrep '^d[1-9][0-9]*::t1' | wc -l) ))
done

EVICTED=$((${TO_EVICT}-${LEFTOVER}))
echo "$(date +%s): $EVICTED/$TO_EVICT files evicted from EOS 'xrdfs prepare -e'"

LASTCOUNT=${EVICTED}

# Build the list of tape only files.
rm -f ${STATUS_FILE}
touch ${STATUS_FILE}
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  eos root://${EOSINSTANCE} ls -y ${EOS_DIR}/${subdir} | egrep 'd0::t[^0]' | sed -e "s%\s\+% %g;s%.* \([^ ]\+\)$%${subdir}/\1%" >> ${STATUS_FILE}
done

# Put all tape drives down
echo "Sleeping 3 seconds to let previous sessions finish."
sleep 3
admin_kdestroy &>/dev/null
admin_kinit &>/dev/null
INITIAL_DRIVES_STATE=`admin_cta --json dr ls`
echo INITIAL_DRIVES_STATE:
echo ${INITIAL_DRIVES_STATE} | jq -r '.[] | [ .driveName, .driveStatus] | @tsv' | column -t
echo -n "Will put down those drives : "
drivesToSetDown=`echo ${INITIAL_DRIVES_STATE} | jq -r '.[] | select (.driveStatus == "UP") | .driveName'`
echo $drivesToSetDown
for d in `echo $drivesToSetDown`; do
  admin_cta drive down $d --reason "PUTTING DRIVE DOWN FOR TESTS"
done

echo "$(date +%s): Waiting for the drives to be down"
SECONDS_PASSED=0
WAIT_FOR_DRIVES_DOWN_TIMEOUT=$((10))
while [[ $SECONDS_PASSED < WAIT_FOR_DRIVES_DOWN_TIMEOUT ]]; do
  sleep 1
  oneStatusUpRemaining=0
  for d in `echo $drivesToSetDown`; do
    status=`admin_cta --json drive ls | jq -r ". [] | select(.driveName == \"$d\") | .driveStatus"`
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
#     echo bash -c "echo xrdfs ${EOSINSTANCE} prepare -s $@" bash
# | \
#  tee ${LOGDIR}/prepare_${subdir}.log | grep -i error

for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  echo -n "Retrieving files to ${EOS_DIR}/${subdir} using ${NB_PROCS} processes (prepare2)..."
  cat ${STATUS_FILE} | grep ^${subdir}/ | cut -d/ -f2 | xargs --max-procs=${NB_PROCS} -iTEST_FILE_NAME bash -c "XRD_LOGLEVEL=Dump KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOSINSTANCE} prepare -s ${EOS_DIR}/${subdir}/TEST_FILE_NAME?activity=T0Reprocess 2>${ERROR_DIR}/RETRIEVE_TEST_FILE_NAME && rm ${ERROR_DIR}/RETRIEVE_TEST_FILE_NAME || echo ERROR with xrootd prepare stage for file TEST_FILE_NAME, full logs in ${ERROR_DIR}/RETRIEVE_TEST_FILE_NAME" | tee ${LOGDIR}/prepare2_${subdir}.log | grep ^ERROR
  echo Done.
  echo -n "Checking the presence of the sys.retrieve.req_id extended attributes..."
  cat ${STATUS_FILE} | grep ^${subdir}/ | cut -d/ -f2 | xargs --max-procs=${NB_PROCS} -iTEST_FILE_NAME bash -c "XRD_LOGLEVEL=Dump KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOSINSTANCE} query opaquefile ${EOS_DIR}/${subdir}/TEST_FILE_NAME?mgm.pcmd=xattr\&mgm.subcmd=get\&mgm.xattrname=sys.retrieve.req_id 2>${ERROR_DIR}/XATTRGET_TEST_FILE_NAME && rm ${ERROR_DIR}/XATTRGET_TEST_FILE_NAME || echo ERROR with xrootd xattr get for file TEST_FILE_NAME, full logs in ${ERROR_DIR}/XATTRGET_TEST_FILE_NAME" | tee ${LOGDIR}/prepare2_sys.retrieve.req_id_${subdir}.log | grep ^ERROR
  echo Done.
done
if [ "0" != "$(ls ${ERROR_DIR} 2> /dev/null | wc -l)" ]; then
  # there were some prepare errors
  echo "Several prepare errors occured during retrieval!"
  echo "Please check client pod logs in artifacts"
  mv ${ERROR_DIR}/* ${LOGDIR}/xrd_errors/
fi

# Ensure all requests files are queued
requestsTotal=`admin_cta --json sq | jq 'map(select (.mountType == "RETRIEVE") | .queuedFiles | tonumber) | add'`
echo "Retrieve requests count: ${requestsTotal}"
filesCount=`cat ${STATUS_FILE} | wc -l`
if [ ${requestsTotal} -ne ${filesCount} ]; then
  echo "ERROR: Retrieve queue(s) size mismatch: ${requestsTotal} requests queued for ${filesCount} files."
fi

# Abort prepare -s requests
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  echo -n "Cancelling prepare for files in ${EOS_DIR}/${subdir} using ${NB_PROCS} processes (prepare_abort)..."
  cat ${STATUS_FILE} | grep ^${subdir}/ | cut -d/ -f2                                                                         \
  | xargs --max-procs=${NB_PROCS} -iTEST_FILE_NAME cta-client-ar-abortPrepare --eos-instance ${EOSINSTANCE}                   \
      --eos-poweruser ${EOSPOWER_USER} --eos-dir ${EOS_DIR} --subdir ${subdir} --file TEST_FILE_NAME --error-dir ${ERROR_DIR} \
  | tee ${LOGDIR}/prepare_abort_sys.retrieve.req_id_${subdir}.log # | grep ^ERROR
  echo Done.
done

# Put drive(s) back up to clear the queue
echo -n "Will put back up those drives : "
echo ${INITIAL_DRIVES_STATE} | jq -r '.[] | select (.driveStatus == "UP") | .driveName'
for d in `echo ${INITIAL_DRIVES_STATE} | jq -r '.[] | select (.driveStatus == "UP") | .driveName'`; do
  admin_cta dr up $d
done

# Check that queues are empty after a while and files did not get retrieved
echo "$(date +%s): Waiting for retrieve queues to be cleared:"
SECONDS_PASSED=0
WAIT_FOR_RETRIEVE_QUEUES_CLEAR_TIMEOUT=$((60))
REMAINING_REQUESTS=`admin_cta --json sq | jq -r 'map(select (.mountType == "RETRIEVE") | .queuedFiles | tonumber) | add'`
echo "${REMAINING_REQUESTS} requests remaining."
# Prevent the result from being empty
if [ -z "$REMAINING_REQUESTS" ]; then REMAINING_REQUESTS='0'; fi
while [[ ${REMAINING_REQUESTS} > 0 ]]; do
  echo "$(date +%s): Waiting for retrieve queues to be cleared: Seconds passed = ${SECONDS_PASSED}"
  sleep 1
  let SECONDS_PASSED=SECONDS_PASSED+1

  if test ${SECONDS_PASSED} == ${WAIT_FOR_RETRIEVE_QUEUES_CLEAR_TIMEOUT}; then
    echo "$(date +%s): Timed out after ${WAIT_FOR_RETRIEVE_QUEUES_CLEAR_TIMEOUT} seconds waiting for retrieve queues to be cleared"
    break
  fi

  REMAINING_REQUESTS=`admin_cta --json sq | jq -r 'map(select (.mountType == "RETRIEVE") | .queuedFiles | tonumber) | add'`;
  # Prevent the result from being empty
  if [ -z "$REMAINING_REQUEST" ]; then REMAINING_REQUESTS='0'; fi
  echo "${REMAINING_REQUESTS} requests remaining."
done

# Check that the files were not retrieved
echo "Checking restaged files..."
RESTAGEDFILES=0
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  RF=$(eos root://${EOSINSTANCE} ls -y ${EOS_DIR}/${subdir} | egrep '^d[1-9][0-9]*::t1' | wc -l)
  echo "Restaged files in directory ${subdir}: ${RF}"
  (( RESTAGEDFILES += ${RF} ))
done
echo "Total restaged files found: ${RESTAGEDFILES}"

if [ "0" != "$(ls ${ERROR_DIR} 2> /dev/null | wc -l)" ]; then
  # there were some prepare errors
  echo "Several errors occured during prepare cancel test!"
  echo "Please check client pod logs in artifacts"
  mv ${ERROR_DIR}/* ${LOGDIR}/xrd_errors/
fi

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
  #XrdSecPROTOCOL=sss eos -r 0 0 root://${EOSINSTANCE} rm -Fr ${EOS_DIR} &
  KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos root://${EOSINSTANCE} rm -Fr ${EOS_DIR} &
  EOSRMPID=$!
  # wait a bit in case eos prematurely fails...
  sleep 0.1
  if test ! -d /proc/${EOSRMPID}; then
    # eos rm process died, get its status
    wait ${EOSRMPID}
    test $? -ne 0 && die "Could not launch eos rm"
  fi
  # Now we can start to do something...
  # deleted files are the ones that made it on tape minus the ones that are still on tapes...
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


echo "###"
echo "$(date +%s): Results:"
echo "REMOVED/EVICTED/RETRIEVED/ARCHIVED/RESTAGEDFILES/NB_FILES"
echo "${DELETED}/${EVICTED}/${RETRIEVED}/${ARCHIVED}/${RESTAGEDFILES}/$((${NB_FILES} * ${NB_DIRS}))"
echo "###"

test -z ${COMMENT} || annotate "test ${TESTID} FINISHED" "Summary:</br>NB_FILES: $((${NB_FILES} * ${NB_DIRS}))</br>ARCHIVED: ${ARCHIVED}<br/>RETRIEVED: ${RETRIEVED}<br/>STAGERRMED: ${STAGERRMED}</br>DELETED: ${DELETED}" 'test,end'


#echo "$(date +%s): Dumping objectstore list"
#ssh root@ctappsfrontend cta-objectstore-list


# stop tail
test -z $TAILPID || kill ${TAILPID} &> /dev/null

RC=0
if [ ${LASTCOUNT} -ne $((${NB_FILES} * ${NB_DIRS})) ]; then
  ((RC++))
  echo "ERROR there were some lost files during the archive/retrieve test with ${NB_FILES} files (first 10):"
  grep -v retrieved ${STATUS_FILE} | sed -e "s;^;${EOS_DIR}/;" | head -10
fi

if [ $(cat ${LOGDIR}/prepare_sys.retrieve.req_id_*.log | grep -v value= | wc -l) -ne 0 ]; then
  # THIS IS NOT YET AN ERROR: UNCOMMENT THE FOLLOWING LINE WHEN https://gitlab.cern.ch/cta/CTA/issues/606 is fixed
  # ((RC++))
  echo "ERROR $(cat ${LOGDIR}/prepare_sys.retrieve.req_id_*.log | grep -v value= | wc -l) files out of $(cat ${LOGDIR}/prepare_sys.retrieve.req_id_*.log | wc -l) prepared files have no sys.retrieve.req_id extended attribute set"
fi


if [ ${RESTAGEDFILES} -ne "0" ]; then
  ((RC++))
  echo "ERROR some files were retrieved in spite of retrieve cancellation."
fi

# This one does not change the return code
# WARNING if everything else was OK
# ERROR otherwise as these xrootd failures could be the reason of the failure
if [ $(ls ${LOGDIR}/xrd_errors | wc -l) -ne 0 ]; then
  # ((RC++)) # do not change RC
  if [ ${RC} -eq 0 ]; then
    echo "WARNING several xrootd failures occured during this run, please check client dumps in ${LOGDIR}/xrd_errors."
  else
    echo "ERROR several xrootd failures occured during this run, please check client dumps in ${LOGDIR}/xrd_errors."
  fi
fi

exit ${RC}
