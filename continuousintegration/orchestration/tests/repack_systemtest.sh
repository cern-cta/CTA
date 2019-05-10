#!/bin/bash

EOSINSTANCE=ctaeos

die() {
  echo "$@" 1>&2
  test -z $TAILPID || kill ${TAILPID} &> /dev/null
  exit 1
}

usage() { cat <<EOF 1>&2
Usage: $0 -v <vid> -b <bufferURL>
(bufferURL example : /eos/ctaeos/repack)
EOF
exit 1
}

testRepackBufferURL(){
  echo "Creating the repack buffer URL at root://${EOSINSTANCE}/${REPACK_BUFFER_BASEDIR}"
  eos root://${EOSINSTANCE} ls -d ${REPACK_BUFFER_BASEDIR} || die "Repack bufferURL directory does not exist"
  echo "Testing the insertion of a test file in the buffer URL"
  for ((i=0; i<300; i++)); do
    xrdcp /etc/group ${FULL_REPACK_BUFFER_URL}/testFile && break
    echo -n "."
    sleep 1
  done
  echo OK
  failed_xrdcp_test=$i
  if test $failed_xrdcp_test -eq 0; then
    echo "[SUCCESS]: Repack buffer URL OK" | tee -a /var/log/CI_tests
    echo "Removing the test file"
    eos root://${EOSINSTANCE} rm ${REPACK_BUFFER_BASEDIR}/testFile
  else
    echo "[ERROR]: Unable to write a file into the provided repack buffer URL." | tee -a /var/log/CI_tests
  fi
  echo "OK"
}

if [ $# -lt 2 ]
then
  usage
fi;

while getopts "v:e:b:" o; do
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

# get some common useful helpers for krb5
. /root/client_helper.sh

# Get kerberos credentials for user1
admin_kinit
klist -s || die "Cannot get kerberos credentials for user ${USER}"

# Get kerberos credentials for poweruser1
eospower_kdestroy
eospower_kinit

WAIT_FOR_REPACK_TIMEOUT=300

echo "Testing the repackBufferURL provided"
FULL_REPACK_BUFFER_URL=root://${EOSINSTANCE}/${REPACK_BUFFER_BASEDIR}
testRepackBufferURL

echo "Deleting existing repack request for VID ${VID_TO_REPACK}"
admin_cta re rm --vid ${VID_TO_REPACK}

echo "State of the tape VID ${VID_TO_REPACK} BEFORE repack"
admin_cta --json ta ls --vid ${VID_TO_REPACK}

echo "Launching repack request for VID ${VID_TO_REPACK}, bufferURL = ${FULL_REPACK_BUFFER_URL}"
admin_cta re add --vid ${VID_TO_REPACK} --justmove --bufferurl ${FULL_REPACK_BUFFER_URL}

SECONDS_PASSED=0
while test 0 = `admin_cta re ls --vid ${VID_TO_REPACK} | grep -E "Complete|Failed" | wc -l`; do
  echo "Waiting for repack request on tape ${VID_TO_REPACK} to be complete: Seconds passed = $SECONDS_PASSED"
  sleep 1
  let SECONDS_PASSED=SECONDS_PASSED+1

  if test ${SECONDS_PASSED} == ${WAIT_FOR_REPACK_TIMEOUT}; then
    echo "Timed out after ${WAIT_FOR_REPACK_TIMEOUT} seconds waiting for tape ${VID_TO_REPACK} to be repacked"
    exit 1
  fi
done
if test 1 = `admin_cta re ls -v ${VID_TO_REPACK} | grep -E "Failed" | wc -l`; then
    echo "Repack failed for tape ${VID_TO_REPACK}."
    exit 1
fi

echo "State of the tape VID ${VID_TO_REPACK} AFTER repack"
admin_cta --json ta ls --vid ${VID_TO_REPACK}