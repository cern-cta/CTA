#!/bin/bash

EOSINSTANCE=ctaeos
EOS_BASEDIR=/eos/ctaeos/cta
TEST_FILE_NAME_BASE=test
DATA_SOURCE=/dev/urandom

NB_PROCS=1
NB_FILES=1
FILE_KB_SIZE=1
VERBOSE=0
REMOVE=0
TAILPID=''


die() {
  echo "$@" 1>&2
  test -z $TAILPID || kill ${TAILPID} &> /dev/null
  exit 1
}


usage() { cat <<EOF 1>&2
Usage: $0 [-n <nb_files>] [-s <file_kB_size>] [-p <# parallel procs>] [-v] [-d <eos_dest_dir>] [-e <eos_instance>] [-S <data_source_file>] [-r]
  -v		Verbose mode: displays live logs of rmcd to see tapes being mounted/dismounted in real time
  -r		Remove files at the end: launches the delete workflow on the files that were deleted. WARNING: THIS CAN BE FATAL TO THE NAMESPACE IF THERE ARE TOO MANY FILES AND XROOTD STARTS TO TIMEOUT.
EOF
exit 1
}

while getopts "d:e:n:s:p:vS:r" o; do
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

STATUS_FILE=$(mktemp)
EOS_BATCHFILE=$(mktemp --suffix=.eosh)

dd if=/dev/urandom of=/tmp/testfile bs=1k count=${FILE_KB_SIZE} || exit 1

if [[ $VERBOSE == 1 ]]; then
  tail -v -f /mnt/logs/tpsrv0*/rmcd/cta/cta-rmcd.log &
  TAILPID=$!
fi

# get some common useful helpers for krb5
. /root/client_helper.sh

# Get kerberos credentials for poweruser1
eospower_kdestroy
eospower_kinit


EOS_DIR="${EOS_BASEDIR}/$(uuidgen)"
echo "Creating test dir in eos: ${EOS_DIR}"
# uuid should be unique no need to remove dir before...
# XrdSecPROTOCOL=sss eos -r 0 0 root://${EOSINSTANCE} rm -Fr ${EOS_DIR}
eos root://${EOSINSTANCE} mkdir -p ${EOS_DIR} || die "Cannot create directory ${EOS_DIR} in eos instance ${EOSINSTANCE}." 

echo -n "Copying files to ${EOS_DIR} using ${NB_PROCS} processes..."
for ((i=0;i<${NB_FILES};i++)); do
  echo ${TEST_FILE_NAME_BASE}$(printf %.4d $i)
done | xargs --max-procs=${NB_PROCS} -iTEST_FILE_NAME xrdcp --silent /tmp/testfile root://${EOSINSTANCE}/${EOS_DIR}/TEST_FILE_NAME
echo Done.


eos root://${EOSINSTANCE} ls ${EOS_DIR} | egrep "${TEST_FILE_NAME_BASE}[0-9]+" | sed -e 's/$/ copied/' > ${STATUS_FILE}

echo "Waiting for files to be on tape:"
SECONDS_PASSED=0
WAIT_FOR_ARCHIVED_FILE_TIMEOUT=$((40+${NB_FILES}/5))
while test 0 != $(grep -c copied$ ${STATUS_FILE}); do
  echo "Waiting for files to be archived to tape: Seconds passed = ${SECONDS_PASSED}"
  sleep 1
  let SECONDS_PASSED=SECONDS_PASSED+1

  if test ${SECONDS_PASSED} == ${WAIT_FOR_ARCHIVED_FILE_TIMEOUT}; then
    echo "Timed out after ${WAIT_FOR_ARCHIVED_FILE_TIMEOUT} seconds waiting for file to be archived to tape"
    break
  fi

  echo "$(egrep -c 'archived$|tapeonly' ${STATUS_FILE})/${NB_FILES} archived"

  # generating EOS batch script
  grep copied$ ${STATUS_FILE} | sed -e 's/ .*$//' | sed -e "s;^;file info ${EOS_DIR}/;" > ${EOS_BATCHFILE}

  # Updating all files statuses
  eos root://${EOSINSTANCE} ls -y ${EOS_DIR} | sed -e 's/^\(d.::t.\).*\(test[0-9]\+\)$/\2 \1/;s/d[^0]::t[^0]/archived/;s/d[^0]::t0/copied/;s/d0::t0/error/;s/d0::t[^0]/tapeonly/' > ${STATUS_FILE}

done

ARCHIVED=$(egrep -c 'archived$|tapeonly' ${STATUS_FILE})


echo "###"
echo "${ARCHIVED}/${NB_FILES} archived"
echo "###"

grep -q 'archived$' ${STATUS_FILE} && echo "Removing disk replica of $(grep -c 'archived$' ${STATUS_FILE}) archived files" || echo "$(grep -c 'tapeonly$' ${STATUS_FILE}) files are on tape with no disk replica"
for TEST_FILE_NAME in $(grep archived$ ${STATUS_FILE} | sed -e 's/ .*$//'); do
    XrdSecPROTOCOL=sss eos -r 0 0 root://${EOSINSTANCE} file drop ${EOS_DIR}/${TEST_FILE_NAME} 1 &> /dev/null || echo "Could not remove disk replica for ${EOS_DIR}/${TEST_FILE_NAME}"
done
# Updating all files statuses
eos root://${EOSINSTANCE} ls -y ${EOS_DIR} | sed -e 's/^\(d.::t.\).*\(test[0-9]\+\)$/\2 \1/;s/d[^0]::t[^0]/archived/;s/d[^0]::t0/copied/;s/d0::t0/error/;s/d0::t[^0]/tapeonly/' > ${STATUS_FILE}


TAPEONLY=$(grep -c tapeonly$ ${STATUS_FILE})

echo "###"
echo "${TAPEONLY}/${ARCHIVED} on tape only"
echo "###"
echo "Sleeping 400 seconds to allow MGM-FST communication to settle after disk copy deletion."
sleep 400
echo "###"

echo "Trigerring EOS retrieve workflow as poweruser1:powerusers (12001:1200)"
#for TEST_FILE_NAME in $(grep tapeonly$ ${STATUS_FILE} | sed -e 's/ .*$//'); do
#  XrdSecPROTOCOL=sss xrdfs ${EOSINSTANCE} prepare -s "${EOS_DIR}/${TEST_FILE_NAME}?eos.ruid=12001&eos.rgid=1200" || echo "Could not trigger retrieve for ${EOS_DIR}/${TEST_FILE_NAME}"
#done

# We need the -s as we are staging the files from tape (see xrootd prepare definition)
grep tapeonly$ ${STATUS_FILE} | sed -e 's/ .*$//' | KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xargs --max-procs=${NB_PROCS} -iTEST_FILE_NAME xrdfs ${EOSINSTANCE} prepare -s ${EOS_DIR}/TEST_FILE_NAME

TO_BE_RETRIEVED=${ARCHIVED}
RETRIEVING=${TO_BE_RETRIEVED}
RETRIEVED=0
# Wait for the copy to appear on disk
echo "$(date +%s): Waiting for files to be back on disk:"
SECONDS_PASSED=0
WAIT_FOR_RETRIEVED_FILE_TIMEOUT=$((40+${NB_FILES}/5))
while test 0 != ${RETRIEVING}; do
  echo "$(date +%s): Waiting for files to be retrieved from tape: Seconds passed = ${SECONDS_PASSED}"
  sleep 1
  let SECONDS_PASSED=SECONDS_PASSED+1

  if test ${SECONDS_PASSED} == ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT}; then
    echo "$(date +%s): Timed out after ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT} seconds waiting for file to be retrieved tape"
    break
  fi

  RETRIEVED=0
  RETRIEVED=$(( ${RETRIEVED} + $(eos root://${EOSINSTANCE} ls -y ${EOS_DIR} | egrep '^d[1-9][0-9]*::t1' | wc -l) ))  

  RETRIEVING=$((${TO_BE_RETRIEVED} - ${RETRIEVED}))

  echo "${RETRIEVED}/${TO_BE_RETRIEVED} retrieved"
done

echo "###"
echo "${RETRIEVED}/${TAPEONLY} retrieved files"
echo "###"

LASTCOUNT=${RETRIEVED}

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

echo "CHECKING sys.retrieves HAS BEEN RESET FOR EACH RETRIEVED FILE"
for RETRIEVED_FILE in `cat ${STATUS_FILE} | grep retrieved | awk '{print $1;}'`; do
  RETRIEVED_FILE_FULL_PATH=${EOS_DIR}/${RETRIEVED_FILE}

  SYS_RETRIEVES_ATTR_GET=`eos root://${EOSINSTANCE} attr get sys.retrieves ${RETRIEVED_FILE_FULL_PATH}`
  if ! echo ${SYS_RETRIEVES_ATTR_GET} | egrep -q '^sys.retrieves='; then
    die "Failed to get value of sys.retrieves for retrieved file ${RETRIEVED_FILE_FULL_PATH}"
  fi

  SYS_RETRIEVES_VALUE=`echo ${SYS_RETRIEVES_ATTR_GET} | tr '"' ' ' | awk '{print $NF;}'`
  if test "0" != "${SYS_RETRIEVES_VALUE}"; then
    die "ERROR sys.retrieves for ${RETRIEVED_FILE_FULL_PATH} is ${SYS_RETRIEVES_VALUE} when it should have been reset to 0"
  fi
done
echo "sys.retrieves HAS BEEN SUCCESSFULLY RESET TO 0 FOR ALL RETRIEVED FILES"

DELETED=0
if [[ $REMOVE == 1 ]]; then
  echo "Waiting for files to be removed from EOS and tapes"
  . /root/client_helper.sh 
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
  # XrdSecPROTOCOL=sss eos -r 0 0 root://${EOSINSTANCE} rm -Fr ${EOS_DIR} &
  KRB5CCNAME=/tmp/${EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos root://${EOSINSTANCE} rm -Fr ${EOS_DIR} &
  EOSRMPID=$!
  # wait a bit in case eos prematurely fails...
  sleep 1
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
echo Results:
echo "REMOVED/RETRIEVED/TAPEONLY/ARCHIVED/NB_FILES"
echo "${DELETED}/${RETRIEVED}/${TAPEONLY}/${ARCHIVED}/${NB_FILES}"
echo "###"

# stop tail
test -z $TAILPID || kill ${TAILPID} &> /dev/null

test ${LASTCOUNT} -eq ${NB_FILES} && exit 0

echo "ERROR there were some lost files during the archive/retrieve test with ${NB_FILES} files (first 10):"
grep -v retrieved ${STATUS_FILE} | sed -e "s;^;${EOS_DIR}/;" | head -10

exit 1
