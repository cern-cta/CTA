#!/bin/bash

EOSINSTANCE=ctaeos
EOS_BASEDIR=/eos/ctaeos/cta
TEST_FILE_NAME_BASE=test
DATA_SOURCE=/dev/urandom

NB_PROCS=1
NB_FILES=1
FILE_KB_SIZE=1
VERBOSE=0
TAILPID=''


die() {
  echo "$@" 1>&2
  exit 1
}


usage() { cat <<EOF 1>&2
Usage: $0 [-n <nb_files>] [-s <file_kB_size>] [-p <# parallel procs>] [-v] [-d <eos_dest_dir>] [-e <eos_instance>] [-S <data_source_file>]
EOF
exit 1
}

while getopts "d:e:n:s:p:vS:" o; do
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
  tail -v -f /mnt/logs/tpsrv0*/rmcd/castor/rmcd_legacy.log &
  TAILPID=$!
fi

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

  echo "$(grep -c archived$ ${STATUS_FILE})/${NB_FILES} archived"

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


echo "Trigerring EOS retrieve workflow as poweruser1:powerusers (12001:1200)"
#for TEST_FILE_NAME in $(grep tapeonly$ ${STATUS_FILE} | sed -e 's/ .*$//'); do
#  XrdSecPROTOCOL=sss xrdfs ${EOSINSTANCE} prepare -s "${EOS_DIR}/${TEST_FILE_NAME}?eos.ruid=12001&eos.rgid=1200" || echo "Could not trigger retrieve for ${EOS_DIR}/${TEST_FILE_NAME}"
#done

grep tapeonly$ ${STATUS_FILE} | sed -e 's/ .*$//' | XrdSecPROTOCOL=sss xargs --max-procs=${NB_PROCS} -iTEST_FILE_NAME xrdfs ${EOSINSTANCE} prepare -s "${EOS_DIR}/TEST_FILE_NAME?eos.ruid=12001&eos.rgid=1200"


# Wait for the copy to appear on disk
SECONDS_PASSED=0
WAIT_FOR_RETRIEVED_FILE_TIMEOUT=$((40+${NB_FILES}/5))
while test 0 != $(grep -c tapeonly$ ${STATUS_FILE}); do
  echo "Waiting for files to be retrieved from tape: Seconds passed = ${SECONDS_PASSED}"
  sleep 1
  let SECONDS_PASSED=SECONDS_PASSED+1

  if test ${SECONDS_PASSED} == ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT}; then
    echo "Timed out after ${WAIT_FOR_ARCHIVED_FILE_TIMEOUT} seconds waiting for file to be retrieved tape"
    break
  fi

  echo "$(grep -c retrieved$ ${STATUS_FILE})/${TAPEONLY} retrieved"

  # generating EOS batch script
  grep tapeonly$ ${STATUS_FILE} | sed -e 's/ .*$//' | sed -e "s;^;file info ${EOS_DIR}/;" > ${EOS_BATCHFILE}

  # Updating all files statuses
  eos --batch root://${EOSINSTANCE} ${EOS_BATCHFILE} | egrep '^ *File|nodrain.*online' | while read line; do
    if echo $line | grep -q File; then
      filename=$(basename $(echo $line | awk '{print $2}' | sed -e "s/'//g"))
    elif echo $line | grep -q nodrain; then
      # file is back on disk
      sed -i ${STATUS_FILE} -e "s/${filename} tapeonly/${filename} retrieved/"
    fi
  done

done

RETRIEVED=$(grep -c retrieved$ ${STATUS_FILE})

echo "###"
echo Results:
echo "RETRIEVED/TAPEONLY/ARCHIVED/NB_FILES"
echo "${RETRIEVED}/${TAPEONLY}/${ARCHIVED}/${NB_FILES}"
echo "###"

# stop tail
test -z $TAILPID || kill ${TAILPID}

test ${RETRIEVED} = ${NB_FILES} && exit 0

echo "ERROR there were some lost files during the archive/retrieve test with ${NB_FILES} files (first 10):"
grep -v retrieved ${STATUS_FILE} | sed -e "s;^;${EOS_DIR}/;" | head -10

exit 1
