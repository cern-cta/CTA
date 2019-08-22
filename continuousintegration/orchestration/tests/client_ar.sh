#!/bin/bash

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
LOGDIR="${LOGDIR}/$(basebane ${EOS_DIR})"
mkdir -p ${LOGDIR} || die "Cannot create directory LOGDIR: ${LOGDIR}"
mkdir -p ${LOGDIR}/xrd_errors || die "Cannot create directory LOGDIR/xrd_errors: ${LOGDIR}/xrd_errors"

STATUS_FILE=$(mktemp)
ERROR_FILE=$(mktemp)
EOS_BATCHFILE=$(mktemp --suffix=.eosh)

dd if=/dev/urandom of=/tmp/testfile bs=1k count=${FILE_KB_SIZE} || exit 1

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

# Create directory for xrootd error reports
ERROR_DIR="/dev/shm/$(basename ${EOS_DIR})"
mkdir ${ERROR_DIR}
# not more than 100k files per directory so that we can rm and find as a standard user
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  eos root://${EOSINSTANCE} mkdir -p ${EOS_DIR}/${subdir} || die "Cannot create directory ${EOS_DIR}/{subdir} in eos instance ${EOSINSTANCE}."
  echo -n "Copying files to ${EOS_DIR}/${subdir} using ${NB_PROCS} processes..."
  for ((i=0;i<${NB_FILES};i++)); do
    echo ${TEST_FILE_NAME_BASE}$(printf %.2d ${subdir})$(printf %.6d $i)
done | xargs --max-procs=${NB_PROCS} -iTEST_FILE_NAME bash -c "XRD_LOGLEVEL=Dump xrdcp /tmp/testfile root://${EOSINSTANCE}/${EOS_DIR}/${subdir}/TEST_FILE_NAME 2>${ERROR_DIR}/TEST_FILE_NAME && rm ${ERROR_DIR}/TEST_FILE_NAME || echo ERROR with xrootd transfer for file TEST_FILE_NAME, full logs in ${ERROR_DIR}/TEST_FILE_NAME"
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
  echo -n "Recalling files to ${EOS_DIR}/${subdir} using ${NB_PROCS} processes..."
  cat ${STATUS_FILE} | grep ^${subdir}/ | cut -d/ -f2 | xargs --max-procs=${NB_PROCS} -iTEST_FILE_NAME bash -c "XRD_LOGLEVEL=Dump KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs ${EOSINSTANCE} prepare -s ${EOS_DIR}/${subdir}/TEST_FILE_NAME?activity=T0Reprocess 2>${ERROR_DIR}/RETRIEVE_TEST_FILE_NAME && rm ${ERROR_DIR}/RETRIEVE_TEST_FILE_NAME || echo ERROR with xrootd transfer for file TEST_FILE_NAME, full logs in ${ERROR_DIR}/RETRIEVE_TEST_FILE_NAME" | tee ${LOGDIR}/prepare_${subdir}.log | grep ^ERROR
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

TO_STAGERRM=$(cat ${STATUS_FILE} | wc -l)

echo "$(date +%s): $TO_STAGERRM files to be stagerrm'ed from EOS using 'xrdfs prepare -e'"
# We need the -s as we are staging the files from tape (see xrootd prepare definition)
cat ${STATUS_FILE} | sed -e "s%^%${EOS_DIR}/%" | XrdSecPROTOCOL=krb5 KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 xargs --max-procs=10 -n 40 xrdfs ${EOSINSTANCE} prepare -e > /dev/null


LEFTOVER=0
for ((subdir=0; subdir < ${NB_DIRS}; subdir++)); do
  LEFTOVER=$(( ${LEFTOVER} + $(eos root://${EOSINSTANCE} ls -y ${EOS_DIR}/${subdir} | egrep '^d[1-9][0-9]*::t1' | wc -l) ))
done

STAGERRMED=$((${TO_STAGERRM}-${LEFTOVER}))
echo "$(date +%s): $STAGERRMED files stagerrmed from EOS 'xrdfs prepare -e'"

LASTCOUNT=${STAGERRMED}

#echo "$(date +%s): Dumping objectstore list"
#ssh root@ctappsfrontend cta-objectstore-list


# Updating all files statuses
# Please note that s/d[^0]::t[^0] now maps to 'retrieved' and not 'archived' as
# in previous status mappings
eos root://${EOSINSTANCE} ls -y ${EOS_DIR} | sed -e 's/^\(d.::t.\).*\(test[0-9]\+\)$/\2 \1/;s/d[^0]::t[^0]/retrieved/;s/d[^0]::t0/copied/;s/d0::t0/error/;s/d0::t[^0]/tapeonly/' > ${STATUS_FILE}

# The format of the STATUS_FILE is two space separated columns per line.  The
# first column is the name of the file and the second is the status of the file.
# For example:
#
# test0000 retrieved
# test0001 retrieved
# test0002 retrieved
# test0003 retrieved

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
  INITIALFILESONTAPE=$(admin_cta archivefile ls  --all | grep ${EOS_DIR} | wc -l)
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
    FILESONTAPE=$(admin_cta archivefile ls --all > >(grep ${EOS_DIR} | wc -l) 2> >(cat > /tmp/ctaerr))
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
    admin_cta archivefile ls --all | grep ${EOS_DIR}
  else
    echo "All files have been deleted"
    LASTCOUNT=${RETRIEVED}
  fi
fi


echo "###"
echo "$(date +%s): Results:"
echo "REMOVED/STAGERRMED/RETRIEVED/ARCHIVED/NB_FILES"
echo "${DELETED}/${STAGERRMED}/${RETRIEVED}/${ARCHIVED}/$((${NB_FILES} * ${NB_DIRS}))"
echo "###"

test -z ${COMMENT} || annotate "test ${TESTID} FINISHED" "Summary:</br>NB_FILES: $((${NB_FILES} * ${NB_DIRS}))</br>ARCHIVED: ${ARCHIVED}<br/>RETRIEVED: ${RETRIEVED}<br/>STAGERRMED: ${STAGERRMED}</br>DELETED: ${DELETED}" 'test,end'


#echo "$(date +%s): Dumping objectstore list"
#ssh root@ctappsfrontend cta-objectstore-list


# stop tail
test -z $TAILPID || kill ${TAILPID} &> /dev/null

test ${LASTCOUNT} -eq $((${NB_FILES} * ${NB_DIRS})) && exit 0

echo "ERROR there were some lost files during the archive/retrieve test with ${NB_FILES} files (first 10):"
grep -v retrieved ${STATUS_FILE} | sed -e "s;^;${EOS_DIR}/;" | head -10

exit 1
