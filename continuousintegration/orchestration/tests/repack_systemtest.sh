#!/bin/bash

#default CI EOS instance
EOSINSTANCE=ctaeos
#default Repack timeout
WAIT_FOR_REPACK_TIMEOUT=300

die() {
  echo "$@" 1>&2
  test -z $TAILPID || kill ${TAILPID} &> /dev/null
  exit 1
}

usage() { cat <<EOF 1>&2
Usage: $0 -v <vid> -b <bufferURL> [-e <eosinstance>] [-t <timeout>]
(bufferURL example : /eos/ctaeos/repack)
eosinstance : the name of the ctaeos instance to be used (default ctaeos)
timeout : the timeout in seconds to wait for the repack to be done
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

while getopts "v:e:b:t:" o; do
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

echo "Backpressure test: setting too high free space requirements"
# This should be idempotent as we will be called several times
if [[ $( admin_cta --json ds ls | jq '.[] | select(.name=="repackBuffer") | .name') != '"repackBuffer"' ]]; then
  admin_cta ds add -n repackBuffer -r "root://${EOSINSTANCE}/${REPACK_BUFFER_BASEDIR}" -u "eos://${EOSINSTANCE}" -i 60 -f 111222333444555 -s 60 -m toto
else
  echo "Disk system repackBuffer alread defined. Ensuring too high free space requirements."
  admin_cta ds ch -n repackBuffer -f 111222333444555
fi
admin_cta ds ls

echo "Launching repack request for VID ${VID_TO_REPACK}, bufferURL = ${FULL_REPACK_BUFFER_URL}"
admin_cta re add --vid ${VID_TO_REPACK} --justmove --bufferurl ${FULL_REPACK_BUFFER_URL}

echo "Backpressure test: waiting to see a report of sleeping retrieve queue."
SECONDS_PASSED=0
while test 0 = `admin_cta --json sq | jq -r ".[] | select(.vid == \"${VID_TO_REPACK}\" and .sleepingForSpace == true) | .vid" | wc -l`; do
  echo "Waiting for retrieve queue for tape ${VID_TO_REPACK} to be sleepting: Seconds passed = $SECONDS_PASSED"
  sleep 1
  let SECONDS_PASSED=SECONDS_PASSED+1

  if test ${SECONDS_PASSED} == ${WAIT_FOR_REPACK_TIMEOUT}; then
    echo "Timed out after ${WAIT_FOR_REPACK_TIMEOUT} seconds waiting for tape ${VID_TO_REPACK} to be repacked"
    exit 1
  fi
done

echo "Turning free space requirement to one byte (zero is not allowed)."
admin_cta ds ch -n repackBuffer -f 1
admin_cta ds ls

echo "Now waiting for repack to proceed."
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

exec /root/repack_generate_report.sh -v ${VID_TO_REPACK}