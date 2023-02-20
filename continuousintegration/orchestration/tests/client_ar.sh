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


# CALL ARCHIVE
. client_archive.sh

# CALL RETRIEVE
. client_retrieve.sh

# OTHER TESTS??
RESTAGEDFILES=0
. client_abortPrepare.sh

. client_deleteFiles.sh

echo "###"
echo "$(date +%s): Results:"
echo "REMOVED/EVICTED/RETRIEVED/ARCHIVED/RESTAGEDFILES/NB_FILES"
echo "${DELETED}/${EVICTED}/${RETRIEVED}/${ARCHIVED}/${RESTAGEDFILES}/$((${NB_FILES} * ${NB_DIRS}))"
echo "###"

test -z ${COMMENT} || annotate "test ${TESTID} FINISHED" "Summary:</br>NB_FILES: $((${NB_FILES} * ${NB_DIRS}))</br>ARCHIVED: ${ARCHIVED}<br/>RETRIEVED: ${RETRIEVED}<br/>STAGERRMED: ${STAGERRMED}</br>DELETED: ${DELETED}" 'test,end'


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
