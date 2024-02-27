#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2024 CERN
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

set -a

touch /tmp/RC
EOSINSTANCE=ctaeos
EOS_BASEDIR=/eos/ctaeos/cta
TEST_FILE_NAME_BASE=test
DATA_SOURCE=/dev/urandom
ARCHIVEONLY=0 # Only archive files or do the full test?
DONOTARCHIVE=0 # files were already archived in a previous run NEED TARGETDIR
TARGETDIR=''
LOGDIR='/var/log'
CLI_TARGET="xrd"

COMMENT=''
# id of the test so that we can track it
TESTID="$(date +%y%m%d%H%M)"

NB_PROCS=1
NB_FILES=1
NB_DIRS=1
FILE_KB_SIZE=1
VERBOSE=0
REMOVE=0
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
  -v    Verbose mode: displays live logs of rmcd to see tapes being mounted/dismounted in real time
  -r    Remove files at the end: launches the delete workflow on the files that were deleted. WARNING: THIS CAN BE FATAL TO THE NAMESPACE IF THERE ARE TOO MANY FILES AND XROOTD STARTS TO TIMEOUT.
  -a    Archiveonly mode: exits after file archival
  -g    Tape aware GC?
  -S    Track progress in SQLite?
  -c    CLI tool to execute
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

while getopts "Z:d:e:n:N:s:p:vS:rAPGt:m:c:" o; do
    case "${o}" in
        c)
            CLI_TARGET=${OPTARG}
            ;;
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
        Z)
            GFAL2_PROTOCOL=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))


if [[ -n ${GFAL2_PROTOCOL} ]]; then
    # Test gfal protocol is supported.
    if [[ ! "${GFAL2_PROTOCOL}" =~ ^(https|root)$ ]]; then
      echo "Invalid gfal2 protocol: ${GFAL2_PROTOCOL}"
      echo "Current supported protocols: https, root"
      exit 1
    fi
fi

if [ ! -z "${error}" ]; then
    echo -e "ERROR:\n${error}"
    exit 1
fi

case "${CLI_TARGET}" in
  xrd)
    . /root/cli_calls.sh
    ;;
  gfal2)
    CLI_TARGET="gfal2-${GFAL2_PROTOCOL}"
    . /root/cli_calls.sh
    ;;
  *)
    echo "ERROR: CLI target ${CLI_TARGET} not supported. Valid options: xrd, gfal2"
    exit 1
esac

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

echo "$(date +%s): TRACKERDB_FILE=${DB_NAME}"
echo "$(date +%s): TRACKERDB_TABLE=${DB_TABLE}"
ERROR_FILE=$(mktemp)
echo "$(date +%s): ERROR_FILE=${ERROR_FILE}"
EOS_BATCHFILE=$(mktemp --suffix=.eosh)
echo "$(date +%s): EOS_BATCHFILE=${EOS_BATCHFILE}"

# Create directory for xrootd error reports
ERROR_DIR="/dev/shm/$(basename ${EOS_DIR})"
mkdir ${ERROR_DIR}
echo "$(date +%s): ERROR_DIR=${ERROR_DIR}"

# get some common useful helpers for krb5
. /root/client_helper.sh

# Get kerberos credentials for user1
user_kinit
klist -s || die "Cannot get kerberos credentials for user ${USER}"

# Get kerberos credentials for poweruser1
eospower_kdestroy
eospower_kinit

NOW=$(date +%s)
LATER=$(echo "${NOW}+${TOKEN_TIMEOUT}"  | bc)
TOKEN=$(eos root://ctaeos token --tree --path '/eos/ctaeos' --expires "${LATER}" --owner user1 --group eosusers --permission rwxd)

TOKEN_EOSPOWER=$(eospower_eos root://"${EOSINSTANCE}" token --tree --path '/eos/ctaeos' --expires "${LATER}")

echo "Starting test ${TESTID}: ${COMMENT}"

#echo "$(date +%s): Dumping objectstore list"
#ssh root@ctappsfrontend cta-objectstore-list

test -z ${COMMENT} || annotate "test ${TESTID} STARTED" "comment: ${COMMENT}<br/>files: $((${NB_DIRS}*${NB_FILES}))<br/>filesize: ${FILE_KB_SIZE}kB" 'test,start'


set +a

# Store the setup environment into a file and
# source it every time we spawn a shell in the
# pod.
export -p > /root/client_env
export -f -p >> /root/client_env
