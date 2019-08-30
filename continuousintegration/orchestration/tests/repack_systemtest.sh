#!/bin/bash

#default CI EOS instance
EOSINSTANCE=ctaeos
#default Repack timeout
WAIT_FOR_REPACK_TIMEOUT=300
# default generate report = true
GENERATE_REPORT=1

die() {
  echo "$@" 1>&2
  test -z $TAILPID || kill ${TAILPID} &> /dev/null
  exit 1
}

usage() { cat <<EOF 1>&2
Usage: $0 -v <vid> -b <bufferURL> [-e <eosinstance>] [-t <timeout>] [-a] [m]
(bufferURL example : /eos/ctaeos/repack)
eosinstance : the name of the ctaeos instance to be used (default ctaeos)
timeout : the timeout in seconds to wait for the repack to be done
-a : Launch a repack just add copies workflow
-m : Launch a repack just move workflow
-g : Avoid generating repack report at the end of repack
EOF
exit 1
}

testRepackBufferURL(){
  echo "Testing the repack buffer URL at root://${EOSINSTANCE}/${REPACK_BUFFER_BASEDIR}"
  eos root://${EOSINSTANCE} ls -d ${REPACK_BUFFER_BASEDIR} 1> /dev/null || die "Repack bufferURL directory does not exist"
  echo "Testing the insertion of a test file in the buffer URL"
  tempFilePath=$(mktemp /tmp/testFile.XXXX)
  tempFileName=${tempFilePath##*/}
  xrdcp ${tempFilePath} ${FULL_REPACK_BUFFER_URL}/${tempFileName} || die "Unable to write a file into the repack buffer directory"
  echo "File ${tempFilePath} written in ${FULL_REPACK_BUFFER_URL}/${tempFileName}"
  echo "Deleting test file from the test directory"
  eos root://${EOSINSTANCE} rm ${REPACK_BUFFER_BASEDIR}/${tempFileName} || die "Unable to delete the testing file"
  echo "Test repack buffer URL OK"
}

if [ $# -lt 2 ]
then
  usage
fi;

while getopts "v:e:b:t:amg" o; do
  case "${o}" in
    v)
      VID_TO_REPACK=${OPTARG}
      ;;
    e)
      EOSINSTANCE=${OPTARG}
      ;;
    b)
      REPACK_BUFFER_BASEDIR=${OPTARG}
      ;;
    t)
      WAIT_FOR_REPACK_TIMEOUT=${OPTARG}
      ;;
    a)
      ADD_COPIES_ONLY="-a"
      ;;
    m)
      MOVE_ONLY="-m"
      ;;
    g)
      GENERATE_REPORT=0
      ;;
    *)
      usage
      ;;
  esac
done
shift $((OPTIND -1))

if [ "x${REPACK_BUFFER_BASEDIR}" = "x" ]; then
  usage
  die "No repack buffer URL provided."
fi

if [ "x${VID_TO_REPACK}" = "x" ]; then
  usage
  die "No vid to repack provided."
fi

REPACK_OPTION=""

if [ "x${ADD_COPIES_ONLY}" != "x" ] && [ "x${MOVE_ONLY}" != "x" ]; then
  die "-a and -m options are mutually exclusive"
fi

[[ "x${ADD_COPIES_ONLY}" == "x" ]] && REPACK_OPTION=${MOVE_ONLY} || REPACK_OPTION=${ADD_COPIES_ONLY}

# get some common useful helpers for krb5
. /root/client_helper.sh

# Get kerberos credentials for user1
admin_kinit
admin_klist > /dev/null 2>&1 || die "Cannot get kerberos credentials for user ${USER}"

FULL_REPACK_BUFFER_URL=root://${EOSINSTANCE}/${REPACK_BUFFER_BASEDIR}
testRepackBufferURL

echo "Deleting existing repack request for VID ${VID_TO_REPACK}"
admin_cta repack rm --vid ${VID_TO_REPACK}

echo "Marking the tape ${VID_TO_REPACK} as full before Repacking it"
admin_cta tape ch --vid ${VID_TO_REPACK} --full true

echo "Launching repack request for VID ${VID_TO_REPACK}, bufferURL = ${FULL_REPACK_BUFFER_URL}"
admin_cta re add --vid ${VID_TO_REPACK} ${REPACK_OPTION} --bufferurl ${FULL_REPACK_BUFFER_URL}

SECONDS_PASSED=0
while test 0 = `admin_cta --json repack ls --vid ${VID_TO_REPACK} | jq -r '.[0] | select(.status == "Complete" or .status == "Failed")' | wc -l`; do
  echo "Waiting for repack request on tape ${VID_TO_REPACK} to be complete: Seconds passed = $SECONDS_PASSED"
  sleep 1
  let SECONDS_PASSED=SECONDS_PASSED+1

  if test ${SECONDS_PASSED} == ${WAIT_FOR_REPACK_TIMEOUT}; then
    echo "Timed out after ${WAIT_FOR_REPACK_TIMEOUT} seconds waiting for tape ${VID_TO_REPACK} to be repacked"
    exit 1
  fi
done
if test 1 = `admin_cta --json repack ls --vid ${VID_TO_REPACK} | jq -r '.[0] | select(.status == "Failed")' | wc -l`; then
    echo "Repack failed for tape ${VID_TO_REPACK}."
    exit 1
fi

echo "Repack request on VID ${VID_TO_REPACK} succeeded."

[[ $GENERATE_REPORT == 1 ]] && exec /root/repack_generate_report.sh -v ${VID_TO_REPACK} || exit 0