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


EOS_MGM_HOST=ctaeos
EOS_BASEDIR=/eos/ctaeos/cta
TEST_FILE_NAME_BASE=test
DATA_SOURCE=/dev/urandom
# id of the test so that we can track it
TESTID="$(date +%y%m%d%H%M)"
TARGETDIR=''
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
NB_BATCH_PROCS=10  # number of parallel batch processes
BATCH_SIZE=100    # number of files per batch process
COMMENT=''
LOGDIR='/var/log'
NAMESPACE=''

SSH_OPTIONS='-o BatchMode=yes -o ConnectTimeout=10'

die() {
  echo "$@" 1>&2
  test -z $TAILPID || kill ${TAILPID} &> /dev/null
  exit 1
}

usage() { cat <<EOF 1>&2
Usage: $0 [-n <nb_files_perdir>] [-N <nb_dir>] [-s <file_kB_size>] [-p <# parallel procs>] [-d <eos_dest_dir>] [-e <eos_instance>]
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

BASE_REPORT_DIRECTORY=/var/log

while getopts "f:d:e:n:N:s:p:v:Gt:m:Q" o; do
    case "${o}" in
        f)
            NAMESPACE=${OPTARG}
            ;;
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
        S)
            DATA_SOURCE=${OPTARG}
            ;;
        r)
            REMOVE=1
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
CLIENT_POD="cta-client-0"
CTA_CLI_POD="cta-cli-0"
EOS_MGM_POD="eos-mgm-0"

echo "Preparing namespace for the tests"
./prepare_tests.sh -n ${NAMESPACE}

kubectl -n ${NAMESPACE} cp client_helper.sh ${CLIENT_POD}:/root/client_helper.sh -c client
kubectl -n ${NAMESPACE} cp client_prepare_file.sh ${CLIENT_POD}:/root/client_prepare_file.sh -c client

removeRepackRequest() {
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin repack rm --vid $1
}

modifyTapeState() {
  reason="${3:-Testing}"
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tape ch --state $2 --reason "$reason" --vid $1
}

modifyTapeStateAndWait() {
  WAIT_FOR_EMPTY_QUEUE_TIMEOUT=60
  SECONDS_PASSED=0
  modifyTapeState $1 $2 $3
  echo "Waiting for tape $1 to complete transitioning to $2"
  while test 0 == `kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json tape ls --state $2 --vid $1  | jq -r ". [] | select(.vid == \"$1\")" | wc -l`; do
    sleep 1
    printf "."
    let SECONDS_PASSED=SECONDS_PASSED+1
    if test ${SECONDS_PASSED} == ${WAIT_FOR_EMPTY_QUEUE_TIMEOUT}; then
      echo "Timed out after ${WAIT_FOR_EMPTY_QUEUE_TIMEOUT} seconds waiting for tape $1 to transition to state $2. Test failed."
      exit 1
    fi
  done
}

archiveFiles() {
  NB_DIRS=$1
  NB_FILES=$2
  FILE_KB_SIZE=$3
  NB_PROCS=$4

  EOS_DIR=''
  if [[ "x${TARGETDIR}" = "x" ]]; then
      EOS_DIR="${EOS_BASEDIR}/$(uuidgen)"
  else
      EOS_DIR="${EOS_BASEDIR}/${TARGETDIR}"
  fi
  LOGDIR="${LOGDIR}/$(basename ${EOS_DIR})"
  mkdir -p ${LOGDIR} || die "Cannot create directory LOGDIR: ${LOGDIR}"
  mkdir -p ${LOGDIR}/xrd_errors || die "Cannot create directory LOGDIR/xrd_errors: ${LOGDIR}/xrd_errors"

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
    TEST_FILE_NUMS[j]=$(printf "%.7d" $j)
  done
  echo "Generating array of padded subdirs"
  TEST_SUBDIRS=()
  for (( j=0; j < NB_DIRS; j++ )); do
    TEST_SUBDIRS[j]=$(printf "%.3d" $j)
  done
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  echo "XRootD report error dir: $(date +%s): ERROR_DIR=${ERROR_DIR}"
  eos root://${EOS_MGM_HOST} mkdir -p ${EOS_DIR}/${subdir}
  eos_exit_status=$?
  echo "eos mkdir exit status: ${eos_exit_status}"
  if [ $eos_exit_status -ne 0 ]; then
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
  #echo "Launching archiving on client pod"
  #echo " Archiving ${NB_FILES} files of ${FILE_SIZE_KB}kB each"
  #echo " Archiving files: xrdcp as user1"
  #kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c "/root/client_setup.sh -n ${NB_FILES} -s ${FILE_SIZE_KB} -p 100 -d /eos/ctaeos/preprod -A" || exit 1
  #kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c ". /root/client_env && /root/client_archive.sh" || exit 1
}

echo
kubectl -n ${NAMESPACE} cp . ${CLIENT_POD}:/root/ -c client

REPACK_BUFFER_URL=/eos/ctaeos/repack
echo "Creating the repack buffer URL directory (${REPACK_BUFFER_URL})"
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos mkdir ${REPACK_BUFFER_URL}
kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos chmod 1777 ${REPACK_BUFFER_URL}

source ./repack_helper.sh
kubectl -n ${NAMESPACE} cp repack_systemtest.sh ${CLIENT_POD}:/root/repack_systemtest.sh -c client

roundTripRepack() {
  echo
  echo "***********************************************************"
  echo "STEP $1. Launching a round trip repack \"just move\" request"
  echo "***********************************************************"

  VID_TO_REPACK=$(getFirstVidContainingFiles)
  if [ "$VID_TO_REPACK" != "null" ]
  then
  echo
    echo "Marking the tape ${VID_TO_REPACK} as REPACKING"
    modifyTapeStateAndWait ${VID_TO_REPACK} REPACKING
    echo "Launching the repack \"just move\" test on VID ${VID_TO_REPACK} (with backpressure)"
    kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/repack_systemtest.sh -v ${VID_TO_REPACK} -b ${REPACK_BUFFER_URL} -m -r ${BASE_REPORT_DIRECTORY}/Step1-RoundTripRepack -p -n repack_ctasystest || exit 1
  else
    echo "No vid found to repack"
    exit 1
  fi

  echo "Removing repack request for ${VID_TO_REPACK}"
  removeRepackRequest ${VID_TO_REPACK}
  echo "Setting the tape ${VID_TO_REPACK} back to ACTIVE"
  modifyTapeState ${VID_TO_REPACK} ACTIVE
  echo "Reclaiming tape ${VID_TO_REPACK}"
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tape reclaim --vid ${VID_TO_REPACK}

  VID_TO_REPACK=$(getFirstVidContainingFiles)
  if [ "$VID_TO_REPACK" != "null" ]
  then
  echo
    echo "Marking the tape ${VID_TO_REPACK} as REPACKING"
    modifyTapeStateAndWait ${VID_TO_REPACK} REPACKING
    echo "Launching the repack \"just move\" test on VID ${VID_TO_REPACK}"
    kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/repack_systemtest.sh -v ${VID_TO_REPACK} -b ${REPACK_BUFFER_URL} -m -r ${BASE_REPORT_DIRECTORY}/Step$1-RoundTripRepack -n repack_ctasystest  || exit 1
  else
    echo "No vid found to repack"
    exit 1
  fi

  removeRepackRequest ${VID_TO_REPACK}
  echo "Setting the tape ${VID_TO_REPACK} back to ACTIVE"
  modifyTapeState ${VID_TO_REPACK} ACTIVE
  echo "Reclaiming tape ${VID_TO_REPACK}"
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tape reclaim --vid ${VID_TO_REPACK}

  echo
  echo "*******************************************************************"
  echo "STEP $1. Launching a round trip repack \"just move\" request TEST OK"
  echo "*******************************************************************"
}

repackNonRepackingTape() {
  echo
  echo "***************************************************************************************"
  echo "STEP $1. Launching a Repack Request on a disabled/broken/exported/active/repacking tape"
  echo "***************************************************************************************"

  VID_TO_REPACK=$(getFirstVidContainingFiles)

  if [ "$VID_TO_REPACK" != "null" ]
  then
    echo "Marking the tape ${VID_TO_REPACK} as DISABLED"
    modifyTapeState ${VID_TO_REPACK} DISABLED "Repack disabled tape test"
    echo "Launching the repack request test on VID ${VID_TO_REPACK} with DISABLED state"
    kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/repack_systemtest.sh -v ${VID_TO_REPACK} -b ${REPACK_BUFFER_URL} -r ${BASE_REPORT_DIRECTORY}/Step$1-RepackDisabledTape -n repack_ctasystest && echo "The repack command should have failed as the tape is DISABLED" && exit 1 || echo "The repack submission has failed, test OK"

    echo "Marking the tape ${VID_TO_REPACK} as BROKEN"
    modifyTapeStateAndWait ${VID_TO_REPACK} BROKEN "Repack broken tape test"
    echo "Launching the repack request test on VID ${VID_TO_REPACK} with BROKEN state"
    kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/repack_systemtest.sh -v ${VID_TO_REPACK} -b ${REPACK_BUFFER_URL} -r ${BASE_REPORT_DIRECTORY}/Step$1-RepackDisabledTape -n repack_ctasystest && echo "The repack command should have failed as the tape is BROKEN" && exit 1 || echo "The repack submission has failed, test OK"

    echo "Marking the tape ${VID_TO_REPACK} as EXPORTED"
    modifyTapeStateAndWait ${VID_TO_REPACK} EXPORTED "Repack exported tape test"
    echo "Launching the repack request test on VID ${VID_TO_REPACK} with EXPORTED state"
    kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/repack_systemtest.sh -v ${VID_TO_REPACK} -b ${REPACK_BUFFER_URL} -r ${BASE_REPORT_DIRECTORY}/Step$1-RepackDisabledTape -n repack_ctasystest && echo "The repack command should have failed as the tape is EXPORTED" && exit 1 || echo "The repack submission has failed, test OK"

    echo "Marking the tape ${VID_TO_REPACK} as ACTIVE"
    modifyTapeState ${VID_TO_REPACK} ACTIVE "Repack active tape test"
    echo "Launching the repack request test on VID ${VID_TO_REPACK} with ACTIVE state"
    kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/repack_systemtest.sh -v ${VID_TO_REPACK} -b ${REPACK_BUFFER_URL} -r ${BASE_REPORT_DIRECTORY}/Step$1-RepackDisabledTape -n repack_ctasystest && echo "The repack command should have failed as the tape is ACTIVE" && exit 1 || echo "The repack submission has failed, test OK"
  else
    echo "No vid found to repack"
    exit 1
  fi;

  echo
  echo "Marking the tape ${VID_TO_REPACK} as REPACKING"
  modifyTapeStateAndWait ${VID_TO_REPACK} REPACKING "Repack repacking tape test"
  echo "Launching the repack request test on VID ${VID_TO_REPACK} with REPACKING state"
  kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/repack_systemtest.sh -v ${VID_TO_REPACK} -b ${REPACK_BUFFER_URL} -r ${BASE_REPORT_DIRECTORY}/Step$1-RepackDisabledTape -n repack_ctasystest  || exit 1

  removeRepackRequest ${VID_TO_REPACK}
  echo "Setting the tape ${VID_TO_REPACK} back to ACTIVE"
  modifyTapeState ${VID_TO_REPACK} ACTIVE
  echo "Reclaiming tape ${VID_TO_REPACK}"
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tape reclaim --vid ${VID_TO_REPACK}

  echo
  echo "***********************************************************************************************"
  echo "STEP $1. Launching a Repack Request on a disabled/broken/exported/active/repacking tape TEST OK"
  echo "***********************************************************************************************"
}

repackJustMove() {
  echo
  echo "*********************************************"
  echo "STEP $1. Testing Repack \"Just move\" workflow"
  echo "*********************************************"

  VID_TO_REPACK=$(getFirstVidContainingFiles)
  if [ "$VID_TO_REPACK" != "null" ]
  then
    echo
    echo "Marking the tape ${VID_TO_REPACK} as REPACKING"
    modifyTapeStateAndWait ${VID_TO_REPACK} REPACKING
    echo "Launching the repack test \"just move\" on VID ${VID_TO_REPACK}"
    kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/repack_systemtest.sh -v ${VID_TO_REPACK} -b ${REPACK_BUFFER_URL} -m -r ${BASE_REPORT_DIRECTORY}/Step$1-RepackJustMove -n repack_ctasystest  || exit 1
  else
    echo "No vid found to repack"
    exit 1
  fi

  removeRepackRequest ${VID_TO_REPACK}
  echo "Setting the tape ${VID_TO_REPACK} back to ACTIVE"
  modifyTapeState ${VID_TO_REPACK} ACTIVE
  echo "Reclaiming tape ${VID_TO_REPACK}"
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tape reclaim --vid ${VID_TO_REPACK}

  echo
  echo "*****************************************************"
  echo "STEP $1. Testing Repack \"Just move\" workflow TEST OK"
  echo "*****************************************************"
}

repackJustMoveWithMaxFiles() {
  echo
  echo "*****************************************************************"
  echo "STEP $1. Testing Repack \"Just move\" workflow with limited files"
  echo "*****************************************************************"

  VID_TO_REPACK=$(getFirstVidContainingFiles)

  if [ "$VID_TO_REPACK" = "null" ]
  then
    echo "No vid found to repack"
    exit 1
  fi

  TOTAL_FILES_ON_TAPE=$(getNumberOfFilesOnTape $VID_TO_REPACK)
  NUMBER_OF_FILES_TO_REPACK=$(($TOTAL_FILES_ON_TAPE/2)) # Number that covers only some of the files on tape (half)

  echo
  echo "Marking the tape ${VID_TO_REPACK} as REPACKING"
  modifyTapeStateAndWait ${VID_TO_REPACK} REPACKING
  echo "Launching the repack test \"just move\", with only ${NUMBER_OF_FILES_TO_REPACK}/${TOTAL_FILES_ON_TAPE} files, on VID ${VID_TO_REPACK}"
  kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/repack_systemtest.sh -v ${VID_TO_REPACK} -b ${REPACK_BUFFER_URL} -m -r ${BASE_REPORT_DIRECTORY}/Step$1-RepackJustMoveWithMaxFiles -n repack_ctasystest -f ${NUMBER_OF_FILES_TO_REPACK} || exit 1
  removeRepackRequest ${VID_TO_REPACK}
  echo "Setting the tape ${VID_TO_REPACK} back to ACTIVE"
  modifyTapeState ${VID_TO_REPACK} ACTIVE
  echo "Reclaiming tape ${VID_TO_REPACK}, should fail because tape should still have files"
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tape reclaim --vid ${VID_TO_REPACK}
  if [ $? -eq 0 ]; then
    echo "Reclaim should have failed"
    exit 1
  else
    echo "Reclaim failed as expected"
  fi

  TOTAL_FILES_ON_TAPE=$(getNumberOfFilesOnTape $VID_TO_REPACK)

  echo
  echo "Marking the tape ${VID_TO_REPACK} as REPACKING"
  modifyTapeStateAndWait ${VID_TO_REPACK} REPACKING
  echo "Launching the repack test \"just move\", for all its ${TOTAL_FILES_ON_TAPE} files, on VID ${VID_TO_REPACK}"
  kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/repack_systemtest.sh -v ${VID_TO_REPACK} -b ${REPACK_BUFFER_URL} -m -r ${BASE_REPORT_DIRECTORY}/Step$1-RepackJustMoveWithMaxFiles -n repack_ctasystest -f ${TOTAL_FILES_ON_TAPE} || exit 1
  removeRepackRequest ${VID_TO_REPACK}
  echo "Setting the tape ${VID_TO_REPACK} back to ACTIVE"
  modifyTapeState ${VID_TO_REPACK} ACTIVE
  echo "Reclaiming tape ${VID_TO_REPACK}, should succeed because tape no longer has files"
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tape reclaim --vid ${VID_TO_REPACK}
  if [ $? -eq 0 ]; then
    echo "Reclaim succeeded as expected"
  else
    echo "Reclaim should have succeeded"
    exit 1
  fi

  echo
  echo "*************************************************************************"
  echo "STEP $1. Testing Repack \"Just move\" workflow with limited files TEST OK"
  echo "*************************************************************************"
}

repackJustAddCopies() {
  echo
  echo "**************************************************************************"
  echo "STEP $1. Testing Repack \"Just Add copies\" workflow with all copies on CTA"
  echo "**************************************************************************"

  VID_TO_REPACK=$(getFirstVidContainingFiles)
  if [ "$VID_TO_REPACK" != "null" ]
  then
    echo "Marking the tape ${VID_TO_REPACK} as REPACKING"
    modifyTapeStateAndWait ${VID_TO_REPACK} REPACKING
    echo "Launching the repack \"just add copies\" test on VID ${VID_TO_REPACK} with all copies already on CTA"
    kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/repack_systemtest.sh -v ${VID_TO_REPACK} -b ${REPACK_BUFFER_URL} -a -r ${BASE_REPORT_DIRECTORY}/Step$1-JustAddCopiesAllCopiesInCTA -n repack_ctasystest
  else
    echo "No vid found to repack"
    exit 1
  fi

  repackJustAddCopiesResult=`kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json re ls | jq -r ". [] | select (.vid == \"${VID_TO_REPACK}\")"`

  nbRetrievedFiles=`echo ${repackJustAddCopiesResult} | jq -r ".retrievedFiles"`
  nbArchivedFiles=`echo ${repackJustAddCopiesResult} | jq -r ".archivedFiles"`

  if [ $nbArchivedFiles == 0 ] && [ $nbRetrievedFiles == 0 ]
  then
    echo "Nb retrieved files = 0 and nb archived files = 0. Test OK"
  else
    echo "Repack \"just add copies\" on VID ${VID_TO_REPACK} failed : nbRetrievedFiles = $nbRetrievedFiles, nbArchivedFiles = $nbArchivedFiles"
    exit 1
  fi

  removeRepackRequest ${VID_TO_REPACK}
  echo "Setting the tape ${VID_TO_REPACK} back to ACTIVE"
  modifyTapeState ${VID_TO_REPACK} ACTIVE

  echo
  echo "**********************************************************************************"
  echo "STEP $1. Testing Repack \"Just Add copies\" workflow with all copies on CTA TEST OK"
  echo "**********************************************************************************"
}

repackCancellation() {
  echo
  echo "***********************************"
  echo "STEP $1. Testing Repack cancellation"
  echo "***********************************"

  echo "Putting all drives down"
  echo 'kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin drive down .* --reason "Putting drive down for repack test"'
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin drive down '.*' --reason "Putting drive down for repack test"

  VID_TO_REPACK=$(getFirstVidContainingFiles)
  if [ "$VID_TO_REPACK" != "null" ]
  then
  echo
    echo "Marking the tape ${VID_TO_REPACK} as REPACKING"
    modifyTapeStateAndWait ${VID_TO_REPACK} REPACKING
    echo "Launching a repack request on VID ${VID_TO_REPACK}"
    kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/repack_systemtest.sh -v ${VID_TO_REPACK} -b ${REPACK_BUFFER_URL} -m -r ${BASE_REPORT_DIRECTORY}/Step$1-RepackCancellation -n repack_ctasystest & 2>/dev/null
  else
    echo "No vid found to repack"
    exit 1
  fi

  echo "Waiting for the launch of the repack request..."
  returnCode=1
  while [[ $returnCode != 0 ]]
  do
    kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json re ls --vid $VID_TO_REPACK 1> /dev/null 2> /dev/null
    returnCode=$?
  done
  echo "Repack request launched"
  echo
  echo "Waiting for the expansion of the repack request..."

  lastFSeqTapeToRepack=`kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json tape ls --vid ${VID_TO_REPACK} | jq -r ".[0] | .lastFseq"`
  lastExpandedFSeq=0
  while [[ $lastExpandedFSeq != $lastFSeqTapeToRepack ]]
  do
    lastExpandedFSeq=`kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json repack ls --vid ${VID_TO_REPACK} | jq -r ".[0] | .lastExpandedFseq" || 0`
  done
  nbFilesOnQueue=`kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json showqueues | jq -r ". [] | select(.vid == \"${VID_TO_REPACK}\") | .queuedFiles"`
  echo "Expansion finished with the following number of files in the retrieve queue: ${nbFilesOnQueue}, for schedulerBackendName = ${schedulerBackendName}"
  schedulerBackendName=`kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json version | jq -r '.[] | .schedulerBackendName'`
  if [[ "$schedulerBackendName" == "postgres" ]]; then
    if [[ -z $nbFilesOnQueue || $nbFilesOnQueue -eq  0 ]]
      then
        echo "Nb files queued is zero, test Failed"
        exit 1
    fi
  fi
  echo "Expansion finished, deleting the Repack Request"
  kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash -c 'kill $(pgrep -f /root/repack_systemtest.sh)'
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin repack rm --vid ${VID_TO_REPACK} || echo "Error while removing the Repack Request. Test FAILED"

  echo
  echo "Checking if the Retrieve queue of the VID ${VID_TO_REPACK} contains the Retrieve Requests created from the Repack Request expansion"
  nbFilesOnTapeToRepack=`kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json tapefile ls --vid ${VID_TO_REPACK} | jq "length"`
  echo "Nb files on tape = $nbFilesOnTapeToRepack"

  nbFilesOnQueue=`kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json showqueues | jq -r ". [] | select(.vid == \"${VID_TO_REPACK}\") | .queuedFiles"`
  echo "Nb files on the queue ${VID_TO_REPACK} = $nbFilesOnQueue"
  if [[ "$schedulerBackendName" == "postgres" ]]; then
    echo "Scheduler backend is postgres. Expecting no queued files for retrieval after repack request has been cancelled."
    if [[ -n $nbFilesOnQueue && $nbFilesOnQueue -ne 0 ]]; then
      echo "Nb files queued is not zero, test Failed"
      exit 1
    fi
  else
    echo "Scheduler backend is NOT postgres. It is: ${schedulerBackendName}. Expecting the retrieve queue to have jobs, checking."
    if [[ $nbFilesOnTapeToRepack != $nbFilesOnQueue ]]
    then
      echo "Nb files on tape != nb files queued, test Failed"
      exit 1
    fi
  fi

  echo "Putting all drives up"
  echo 'kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin drive up .*'
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin drive up '.*'

  WAIT_FOR_EMPTY_QUEUE_TIMEOUT=100

  SECONDS_PASSED=0
  while test 0 != `kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json showqueues | jq -r ". [] | select(.vid == \"${VID_TO_REPACK}\")" | wc -l`; do
    echo "Waiting for the Retrieve queue ${VID_TO_REPACK} to be empty: Seconds passed = $SECONDS_PASSED"
    sleep 1
    let SECONDS_PASSED=SECONDS_PASSED+1
    if test ${SECONDS_PASSED} == ${WAIT_FOR_EMPTY_QUEUE_TIMEOUT}; then
      echo "Timed out after ${WAIT_FOR_EMPTY_QUEUE_TIMEOUT} seconds waiting for retrieve queue ${VID_TO_REPACK} to be emptied. Test failed."
      exit 1
    fi
  done

  removeRepackRequest ${VID_TO_REPACK}
  echo "Setting the tape ${VID_TO_REPACK} back to ACTIVE"
  modifyTapeState ${VID_TO_REPACK} ACTIVE

  echo "Retrieve queue of VID ${VID_TO_REPACK} is empty, test OK"

  echo "*******************************************"
  echo "STEP $1. Testing Repack cancellation TEST OK"
  echo "*******************************************"
}

repackMoveAndAddCopies() {
  echo
  echo "*******************************************************"
  echo "STEP $1. Testing Repack \"Move and Add copies\" workflow"
  echo "*******************************************************"

  defaultTapepool="ctasystest"
  tapepoolDestination1_default="systest2_default"
  tapepoolDestination2_default="systest3_default"
  tapepoolDestination2_repack="systest3_repack"

  echo "Creating 2 destination tapepools : $tapepoolDestination1_default and $tapepoolDestination2_default"
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tapepool add --name $tapepoolDestination1_default --vo vo --partialtapesnumber 2 --comment "$tapepoolDestination1_default tapepool"
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tapepool add --name $tapepoolDestination2_default --vo vo --partialtapesnumber 2 --comment "$tapepoolDestination2_default tapepool"
  echo "OK"

  echo "Creating 1 destination tapepool for repack : $tapepoolDestination2_repack (will override $tapepoolDestination2_default)"
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tapepool add --name $tapepoolDestination2_repack --vo vo --partialtapesnumber 2 --comment "$tapepoolDestination2_repack tapepool"
  echo "OK"

  echo "Creating archive routes for adding two copies of the file"
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin archiveroute add --storageclass ctaStorageClass --copynb 2 --tapepool $tapepoolDestination1_default --comment "ArchiveRoute2_default"
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin archiveroute add --storageclass ctaStorageClass --copynb 3 --archiveroutetype DEFAULT --tapepool $tapepoolDestination2_default --comment "ArchiveRoute3_default"
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin archiveroute add --storageclass ctaStorageClass --copynb 3 --archiveroutetype REPACK --tapepool $tapepoolDestination2_repack --comment "ArchiveRoute3_repack"
  echo "OK"

  echo "Will change the tapepool of the tapes"

  allVID=`kubectl -n ${NAMESPACE}  exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json tape ls --all | jq -r ". [] | .vid"`
  allVIDTable=($allVID)

  nbVid=${#allVIDTable[@]}

  allTapepool=`kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json tapepool ls | jq -r ". [] .name"`
  allTapepoolTable=($allTapepool)

  nbTapepool=${#allTapepoolTable[@]}
  nbTapePerTapepool=$(($nbVid / $nbTapepool))

  countChanging=0
  tapepoolIndice=1 #We only change the vid of the remaining other tapes

  for ((i=$(($nbTapePerTapepool+$(($nbVid%$nbTapepool)))); i<$nbVid; i++));
  do
    echo "kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tape ch --vid ${allVIDTable[$i]} --tapepool ${allTapepoolTable[$tapepoolIndice]}"
    kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tape ch --vid ${allVIDTable[$i]} --tapepool ${allTapepoolTable[$tapepoolIndice]}
    countChanging=$((countChanging + 1))
    if [ $countChanging != 0 ] && [ $((countChanging % nbTapePerTapepool)) == 0 ]
    then
      tapepoolIndice=$((tapepoolIndice + 1))
    fi
  done

  echo "OK"

  storageClassName=`kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json storageclass ls | jq -r ". [0] | .name"`

  echo "Changing the storage class $storageClassName nb copies"
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin storageclass ch --name $storageClassName --numberofcopies 3
  echo "OK"

  echo "Putting all drives up"
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin dr up '.*'
  echo "OK"

  VID_TO_REPACK=$(getFirstVidContainingFiles)
  kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos ls -la /eos/ctaeos/repack/${VID_TO_REPACK}

  echo "Marking the tape ${VID_TO_REPACK} as REPACKING"
  modifyTapeStateAndWait ${VID_TO_REPACK} REPACKING
  echo "Launching the repack \"Move and add copies\" test on VID ${VID_TO_REPACK}"
  kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/repack_systemtest.sh -v ${VID_TO_REPACK} -b ${REPACK_BUFFER_URL} -t 300 -r ${BASE_REPORT_DIRECTORY}/Step$1-MoveAndAddCopies -n repack_ctasystest  || exit 1

  repackLsResult=`kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json repack ls --vid ${VID_TO_REPACK} | jq ". [0]"`
  totalFilesToRetrieve=`echo $repackLsResult | jq -r ".totalFilesToRetrieve"`
  totalFilesToArchive=`echo $repackLsResult | jq -r ".totalFilesToArchive"`
  retrievedFiles=`echo $repackLsResult | jq -r ".retrievedFiles"`
  archivedFiles=`echo $repackLsResult | jq -r ".archivedFiles"`

  if [[ $retrievedFiles != $totalFilesToRetrieve ]]
  then
    echo "RetrievedFiles ($retrievedFiles) != totalFilesToRetrieve ($totalFilesToRetrieve), test FAILED"
    exit 1
  else
    echo "RetrievedFiles ($retrievedFiles) = totalFilesToRetrieve ($totalFilesToRetrieve), OK"
  fi

  if [[ $archivedFiles != $totalFilesToArchive ]]
  then
    echo "ArchivedFiles ($archivedFiles) != totalFilesToArchive ($totalFilesToArchive), test FAILED"
    exit 1
  else
     echo "ArchivedFiles ($archivedFiles) == totalFilesToArchive ($totalFilesToArchive), OK"
  fi

  # Check that 2 copies were written to default tapepool (archive route 1 and 2) and 1 copy to repack tapepool (archive route 3)
  TAPEPOOL_LIST=$(kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -- cta-admin --json repack ls --vid ${VID_TO_REPACK} | jq ".[] | .destinationInfos[] | .vid" | xargs -I{} kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json tape ls --vid {} | jq -r '.[] .tapepool')

  if [[ $TAPEPOOL_LIST != *"$defaultTapepool"* ]]; then
    echo "Did not find $defaultTapepool in repack archive destination pools. Archive route failed."
    exit 1
  else
    echo "Found $defaultTapepool in repack archive destination pools."
  fi
  if [[ $TAPEPOOL_LIST != *"$tapepoolDestination1_default"* ]]; then
    echo "Did not find $tapepoolDestination1_default in repack archive destination pools. Archive route failed."
    exit 1
  else
    echo "Found $tapepoolDestination1_default in repack archive destination pools."
  fi
  if [[ $TAPEPOOL_LIST != *"$tapepoolDestination2_repack"* ]]; then
    echo "Did not find $tapepoolDestination2_repack in repack archive destination pools. Archive route failed."
    exit 1
  else
    echo "Found $tapepoolDestination2_repack in repack archive destination pools."
  fi

  kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos ls -la /eos/ctaeos/repack/${VID_TO_REPACK}
  echo "----"
  removeRepackRequest ${VID_TO_REPACK}
  echo "Setting the tape ${VID_TO_REPACK} back to ACTIVE"
  modifyTapeState ${VID_TO_REPACK} ACTIVE
  echo "Reclaimimg tape ${VID_TO_REPACK}"
  kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tape reclaim --vid ${VID_TO_REPACK}
  kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos ls -la /eos/ctaeos/repack/${VID_TO_REPACK}

  echo
  echo "***************************************************************"
  echo "STEP $1. Testing Repack \"Move and Add copies\" workflow TEST OK"
  echo "***************************************************************"
}

repackTapeRepair() {
  echo
  echo "*******************************************************"
  echo "STEP $1. Testing Repack \"Tape Repair\" workflow"
  echo "*******************************************************"

  VID_TO_REPACK=$(getFirstVidContainingFiles)
  if [ "$VID_TO_REPACK" == "null" ]
  then
    echo "No vid found to repack"
    exit 1
  fi

  echo "Getting files to inject into the repack buffer directory"

  tfls=`kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json tapefile ls --vid ${VID_TO_REPACK}`
  nbFileToInject=10

  if [[ $nbFileToInject != 0 ]]
  then
    echo "Will inject $nbFileToInject files into the repack buffer directory"
    bufferDirectory=${REPACK_BUFFER_URL}/TapeRepair-${VID_TO_REPACK}
    echo "Creating buffer directory in \"$bufferDirectory\""
    kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos mkdir $bufferDirectory
    kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos chmod 1777 $bufferDirectory

    echo "Retrieving files from the tape"
    allPid=()
    pathOfFilesToInject=()
    diskIds=()

    for i in $(seq 0 $(( nbFileToInject - 1 )) )
    do
      diskId=`echo $tfls | jq -r ". [$i] | .df.diskId"` || break
      diskIds[$i]=$diskId
      pathFileToInject=`kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos fileinfo fid:$diskId --path | cut -d":" -f2 | tr -d " "`
      pathOfFilesToInject[$i]=$pathFileToInject
    done

    kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/client_prepare_file.sh `for file in ${pathOfFilesToInject[@]}; do echo -n "-f $file "; done`

    echo "Copying the retrieved files into the repack buffer $bufferDirectory"

    for i in $(seq 0 $(( nbFileToInject - 1)) )
    do
      fseqFile=`echo $tfls | jq -r ". [] | select(.df.diskId == \"${diskIds[$i]}\") | .tf.fSeq"` || break
      kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos cp ${pathOfFilesToInject[$i]} $bufferDirectory/`printf "%9d\n" $fseqFile | tr ' ' 0`
    done

    echo "Marking the tape ${VID_TO_REPACK} as REPACKING"
    modifyTapeStateAndWait ${VID_TO_REPACK} REPACKING
    echo "Launching a repack request on the vid ${VID_TO_REPACK}"
    kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/repack_systemtest.sh -v ${VID_TO_REPACK} -b ${REPACK_BUFFER_URL} -m -r ${BASE_REPORT_DIRECTORY}/Step$1-RepackTapeRepair -n repack_ctasystest  ||      exit 1

    repackLsResult=`kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json repack ls --vid ${VID_TO_REPACK} | jq -r ". [0]"`
    userProvidedFiles=`echo $repackLsResult | jq -r ".userProvidedFiles"`
    archivedFiles=`echo $repackLsResult | jq -r ".archivedFiles"`
    retrievedFiles=`echo $repackLsResult | jq -r ".retrievedFiles"`
    totalFilesToRetrieve=`echo $repackLsResult | jq -r ".totalFilesToRetrieve"`
    totalFilesToArchive=`echo $repackLsResult | jq -r ".totalFilesToArchive"`

    if [[ $totalFilesToRetrieve != $(( $totalFilesToArchive - $userProvidedFiles )) ]]
    then
      echo "totalFilesToRetrieve ($totalFilesToRetrieve) != totalFilesToArchive ($totalFilesToArchive) - userProvidedFiles ($userProvidedFiles), test FAILED"
      exit 1
    else
      echo "totalFilesToRetrieve ($totalFilesToRetrieve) == totalFilesToArchive ($totalFilesToArchive) - userProvidedFiles ($userProvidedFiles), OK"
    fi

    if [[ $retrievedFiles != $totalFilesToRetrieve ]]
    then
      echo "retrievedFiles ($retrievedFiles) != totalFilesToRetrieve ($totalFilesToRetrieve) test FAILED"
      exit 1
    else
      echo "retrievedFiles ($retrievedFiles) == totalFilesToRetrieve ($totalFilesToRetrieve), OK"
    fi

    if [[ $archivedFiles != $totalFilesToArchive ]]
    then
      echo "archivedFiles ($archivedFiles) != totalFilesToArchive ($totalFilesToArchive), test FAILED"
      exit 1
    else
       echo "archivedFiles ($archivedFiles) == totalFilesToArchive ($totalFilesToArchive), OK"
    fi

    # For now manually remove the files in EOS; they were copied there by root so it seems that the tape daemon is not allowed to remove them even though the permissions are set 1777
    kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos rm -rF $bufferDirectory
    removeRepackRequest ${VID_TO_REPACK}
    echo "Setting the tape ${VID_TO_REPACK} back to ACTIVE"
    modifyTapeState ${VID_TO_REPACK} ACTIVE
    echo "Reclaiming tape ${VID_TO_REPACK}"
    kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin tape reclaim --vid ${VID_TO_REPACK}

  else
    echo "No file to inject, test not OK"
    exit 1
  fi

  echo
  echo "*******************************************************"
  echo "STEP $1. Testing Repack \"Tape Repair\" workflow TEST OK"
  echo "*******************************************************"
}

repackTapeRepairNoRecall() {
  echo
  echo "*******************************************************"
  echo "STEP $1. Testing Repack \"Tape Repair\" NO RECALL workflow"
  echo "*******************************************************"

  VID_TO_REPACK=$(getFirstVidContainingFiles)
  if [ "$VID_TO_REPACK" == "null" ]
  then
    echo "No vid found to repack"
    exit 1
  fi

  echo "Getting files to inject into the repack buffer directory"

  tfls=`kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json tapefile ls --vid ${VID_TO_REPACK}`
  nbFilesOnTape=`echo $tfls | jq length`
  nbFileToInject=10

  if [[ $nbFileToInject != 0 ]]
  then
    echo "Will inject $nbFileToInject files into the repack buffer directory"
    bufferDirectory=${REPACK_BUFFER_URL}/TapeRepairNoRecall-${VID_TO_REPACK}
    echo "Creating buffer directory in \"$bufferDirectory\""
    kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos mkdir $bufferDirectory
    kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos chmod 1777 $bufferDirectory

    echo "Retrieving files from the tape"
    allPid=()
    pathOfFilesToInject=()
    diskIds=()
    filesIndices=()
    # Prepare array of indices to pick from the tfls output
    for i in $(seq 0 $(( nbFileToInject - 1 )) )
    do
      filesIndices[$i]=$(( i + 1 ))
    done

    for i in $(seq 0 $(( nbFileToInject - 1 )) )
    do
      diskId=`echo $tfls | jq -r ". [${filesIndices[$i]}] | .df.diskId"` || break
      diskIds[$i]=$diskId
      pathFileToInject=`kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos fileinfo fid:$diskId --path | cut -d":" -f2 | tr -d " "`
      pathOfFilesToInject[$i]=$pathFileToInject
    done

    kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/client_prepare_file.sh `for file in ${pathOfFilesToInject[@]}; do echo -n "-f $file "; done`

    echo "Copying the retrieved files into the repack buffer $bufferDirectory"

    for i in $(seq 0 $(( nbFileToInject - 1)) )
    do
      fseqFile=`echo $tfls | jq -r ". [] | select(.df.diskId == \"${diskIds[$i]}\") | .tf.fSeq"` || break
      kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos cp ${pathOfFilesToInject[$i]} $bufferDirectory/`printf "%9d\n" $fseqFile | tr ' ' 0`
    done

    echo "Marking the tape ${VID_TO_REPACK} as REPACKING"
    modifyTapeStateAndWait ${VID_TO_REPACK} REPACKING

    echo "Launching a repack request on the vid ${VID_TO_REPACK}"
    kubectl -n ${NAMESPACE} exec ${CLIENT_POD} -c client -- bash /root/repack_systemtest.sh -v ${VID_TO_REPACK} -b ${REPACK_BUFFER_URL} -m -r ${BASE_REPORT_DIRECTORY}/Step$1-RepackTapeRepairNoRecall -n repack_ctasystest -u ||      exit 1

    repackLsResult=`kubectl -n ${NAMESPACE} exec ${CTA_CLI_POD} -c cta-cli -- cta-admin --json repack ls --vid ${VID_TO_REPACK} | jq -r ". [0]"`
    userProvidedFiles=`echo $repackLsResult | jq -r ".userProvidedFiles"`
    archivedFiles=`echo $repackLsResult | jq -r ".archivedFiles"`
    retrievedFiles=`echo $repackLsResult | jq -r ".retrievedFiles"`
    totalFilesToRetrieve=`echo $repackLsResult | jq -r ".totalFilesToRetrieve"`
    totalFilesToArchive=`echo $repackLsResult | jq -r ".totalFilesToArchive"`

    if [[ $totalFilesToRetrieve != $(( $totalFilesToArchive - $userProvidedFiles )) ]]
    then
      echo "totalFilesToRetrieve ($totalFilesToRetrieve) != totalFilesToArchive ($totalFilesToArchive) - userProvidedFiles ($userProvidedFiles), test FAILED"
      exit 1
    else
      echo "totalFilesToRetrieve ($totalFilesToRetrieve) == totalFilesToArchive ($totalFilesToArchive) - userProvidedFiles ($userProvidedFiles), OK"
    fi

    if [[ $retrievedFiles != $totalFilesToRetrieve ]]
    then
      echo "retrievedFiles ($retrievedFiles) != totalFilesToRetrieve ($totalFilesToRetrieve) test FAILED"
      exit 1
    else
      echo "retrievedFiles ($retrievedFiles) == totalFilesToRetrieve ($totalFilesToRetrieve), OK"
    fi

    if [[ $archivedFiles != $totalFilesToArchive || $userProvidedFiles != $archivedFiles ]]
    then
      echo "archivedFiles ($archivedFiles) != totalFilesToArchive ($totalFilesToArchive), test FAILED"
      exit 1
    else
       echo "archivedFiles ($archivedFiles) == totalFilesToArchive ($totalFilesToArchive), OK"
    fi

    # For now manually remove the files in EOS; they were copied there by root so it seems that the tape daemon is not allowed to remove them
    kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos rm -rF $bufferDirectory

    removeRepackRequest ${VID_TO_REPACK}
    echo "Setting the tape ${VID_TO_REPACK} back to ACTIVE"
    modifyTapeState ${VID_TO_REPACK} ACTIVE
    kubectl -n ${NAMESPACE} exec ${EOS_MGM_POD} -c eos-mgm -- eos ls -l /eos/ctaeos/repack/${VID_TO_REPACK}

  else
    echo "No file to inject, test not OK"
    exit 1
  fi

  echo
  echo "*******************************************************"
  echo "STEP $1. Testing Repack \"Tape Repair\" workflow NO RECALL TEST OK"
  echo "*******************************************************"
}

test -z ${COMMENT} || annotate "test ${TESTID} STARTED" "comment: ${COMMENT}<br/>files: $((${NB_DIRS}*${NB_FILES}))<br/>filesize: ${FILE_KB_SIZE}kB" 'test,start'

#Execution of each tests
archiveFiles ${NB_DIRS} ${NB_FILES} ${FILE_KB_SIZE} ${NB_PROCS}
roundTripRepack 1
repackJustMoveWithMaxFiles 2
repackJustMove 3
repackTapeRepair 4
repackJustAddCopies 5
repackCancellation 6
repackTapeRepairNoRecall 7
# Keep this test for last - it adds new tapepools and archive routes
repackMoveAndAddCopies 10

test -z ${COMMENT} || annotate "test ${TESTID} FINISHED" "Summary:</br>NB_FILES: $((${NB_FILES} * ${NB_DIRS}))</br>" 'test,end'