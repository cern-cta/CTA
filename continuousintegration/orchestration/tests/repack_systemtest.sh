#!/bin/bash

# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2021 CERN
# @license        This program is free software: you can redistribute it and/or modify
#                 it under the terms of the GNU General Public License as published by
#                 the Free Software Foundation, either version 3 of the License, or
#                 (at your option) any later version.
#
#                 This program is distributed in the hope that it will be useful,
#                 but WITHOUT ANY WARRANTY; without even the implied warranty of
#                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#                 GNU General Public License for more details.
#
#                 You should have received a copy of the GNU General Public License
#                 along with this program.  If not, see <http://www.gnu.org/licenses/>.

#default CI EOS instance
EOSINSTANCE=ctaeos
#default Repack timeout
WAIT_FOR_REPACK_TIMEOUT=300

REPORT_DIRECTORY=/var/log

die() {
  echo "$@" 1>&2
  test -z $TAILPID || kill ${TAILPID} &> /dev/null
  exit 1
}

usage() { cat <<EOF 1>&2
Usage: $0 -v <vid> -b <bufferURL> -n <mountPolicyName> [-e <eosinstance>] [-t <timeout>] [-r <reportDirectory>] [-a] [-m] [-d]
(bufferURL example : /eos/ctaeos/repack)
mountPolicyName: the name of the mountPolicy to be applied to the repack request (example: ctasystest)
eosinstance : the name of the ctaeos instance to be used (default : $EOSINSTANCE)
timeout : the timeout in seconds to wait for the repack to be done
reportDirectory : the directory to generate the report of the repack test (default : $REPORT_DIRECTORY)
-a : Launch a repack just add copies workflow
-m : Launch a repack just move workflow
-d : Force a repack on a disabled tape (adds --disabled to the repack add command)
-p : enable backpressure test
-u : recall only option flag
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

if [ $# -lt 3 ]
then
  usage
fi;

DISABLED_TAPE_FLAG=""
while getopts "v:e:b:t:r:n:amdpu" o; do
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
    r)
      REPORT_DIRECTORY=${OPTARG}
      ;;
    d)
      DISABLED_TAPE_FLAG="--disabledtape"
      ;;
    p)
      BACKPRESSURE_TEST=1
      ;;
    n)
      MOUNT_POLICY_NAME=${OPTARG}
      ;;
    u)
      NO_RECALL=1
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

if [ "x${MOUNT_POLICY_NAME}" = "x" ]; then
  usage
  die "No mount policy name provided."
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

if [ ! -z $BACKPRESSURE_TEST ]; then
  echo "Backpressure test: setting too high free space requirements"
  # This should be idempotent as we will be called several times
  if [[ $( admin_cta --json ds ls | jq '.[] | select(.name=="repackBuffer") | .name') != '"repackBuffer"' ]]; then
    admin_cta di add -n repackDiskInstance -m toto
    admin_cta dis add -n repackDiskInstanceSpace --di repackDiskInstance -u "eos:${EOSINSTANCE}:default" -i 5 -m toto
    admin_cta ds add -n repackBuffer --di repackDiskInstance --dis repackDiskInstanceSpace -r "root://${EOSINSTANCE}/${REPACK_BUFFER_BASEDIR}" -f 111222333444555 -s 20 -m toto
  else
    echo "Disk system repackBuffer alread defined. Ensuring too high free space requirements."
    admin_cta ds ch -n repackBuffer -f 111222333444555
  fi
  admin_cta ds ls
fi

echo "Launching repack request for VID ${VID_TO_REPACK}, bufferURL = ${FULL_REPACK_BUFFER_URL}"

NO_RECALL_FLAG=""
if [ ! -z $NO_RECALL ]; then
  NO_RECALL_FLAG="--nr"
fi

admin_cta repack add --mountpolicy ${MOUNT_POLICY_NAME} --vid ${VID_TO_REPACK} ${REPACK_OPTION} --bufferurl ${FULL_REPACK_BUFFER_URL} ${DISABLED_TAPE_FLAG} ${NO_RECALL_FLAG} || exit 1

if [ ! -z $BACKPRESSURE_TEST ]; then
  echo "Backpressure test: waiting to see a report of sleeping retrieve queue."
  SECONDS_PASSED=0
  while test 0 = `admin_cta --json sq | jq -r ".[] | select(.vid == \"${VID_TO_REPACK}\" and .sleepingForSpace == true) | .vid" | wc -l`; do
    echo "Waiting for retrieve queue for tape ${VID_TO_REPACK} to be sleeping: Seconds passed = $SECONDS_PASSED"
    sleep 1
    let SECONDS_PASSED=SECONDS_PASSED+1

    if test ${SECONDS_PASSED} == ${WAIT_FOR_REPACK_TIMEOUT}; then
      echo "Timed out after ${WAIT_FOR_REPACK_TIMEOUT} seconds waiting retrieve queue for tape ${VID_TO_REPACK} to be sleeping."
      exit 1
    fi
  done

  echo "Turning free space requirement to one byte (zero is not allowed)."
  admin_cta ds ch -n repackBuffer -f 1
  admin_cta ds ls
fi

echo "Now waiting for repack to proceed."
SECONDS_PASSED=0
while test 0 = `admin_cta --json repack ls --vid ${VID_TO_REPACK} | jq -r '.[0] | select(.status == "Complete" or .status == "Failed")' | wc -l`; do
  echo "Waiting for repack request on tape ${VID_TO_REPACK} to be complete: Seconds passed = $SECONDS_PASSED"
  sleep 1
  let SECONDS_PASSED=SECONDS_PASSED+1

  if test ${SECONDS_PASSED} == ${WAIT_FOR_REPACK_TIMEOUT}; then
    echo "Timed out after ${WAIT_FOR_REPACK_TIMEOUT} seconds waiting for tape ${VID_TO_REPACK} to be repacked"
    echo "Result of Repack ls"
    admin_cta repack ls --vid ${VID_TO_REPACK}
    destinationInfos=`admin_cta --json repack ls --vid ${VID_TO_REPACK} | jq -r ". [0] | .destinationInfos"`
    if [[ `echo $destinationInfos | jq length` != 0 ]]
    then
      header="DestinationVID\tNbFiles\ttotalSize\n"
      { echo -e $header; echo $destinationInfos | jq -r ".[] | [(.vid),(.files),(.bytes)] | @tsv"; } | column -t
    fi
    exit 1
  fi
done
if test 1 = `admin_cta --json repack ls --vid ${VID_TO_REPACK} | jq -r '[.[0] | select (.status == "Failed")] | length'`; then
    echo "Repack failed for tape ${VID_TO_REPACK}."
    admin_cta repack ls --vid ${VID_TO_REPACK}
    destinationInfos=`admin_cta --json repack ls --vid ${VID_TO_REPACK} | jq -r ". [0] | .destinationInfos"`
    if [[ `echo $destinationInfos | jq length` != 0 ]]
    then
      header="DestinationVID\tNbFiles\ttotalSize\n"
      { echo -e $header; echo $destinationInfos | jq -r ".[] | [(.vid),(.files),(.bytes)] | @tsv"; } | column -t
    fi
    exit 1
fi

echo
destinationInfos=`admin_cta --json repack ls --vid ${VID_TO_REPACK} | jq -r ". [0] | .destinationInfos"`
if [[ `echo $destinationInfos | jq length` != 0 ]]
then
  header="DestinationVID\tNbFiles\ttotalSize\n"
  { echo -e $header; echo $destinationInfos | jq -r ".[] | [(.vid),(.files),(.bytes)] | @tsv"; } | column -t
fi

amountArchivedFiles=`admin_cta --json repack ls --vid ${VID_TO_REPACK} | jq -r ". [0] | .archivedFiles"`
amountRecyleTapeFiles=`admin_cta --json recycletf ls --vid ${VID_TO_REPACK} | jq "length"`

echo "Amount of archived files = $amountArchivedFiles"
echo "Amount of recycled tape files = $amountRecyleTapeFiles"
if [[ $amountArchivedFiles -eq $amountRecyleTapeFiles ]]
then
  echo "The amount of archived files is equal to the amount of recycled tape files. Test OK"
else
  echo "The amount of archived files is not equal to the amount of recycled tape files. Test FAILED"
  exit 1
fi

echo
echo "Repack request on VID ${VID_TO_REPACK} succeeded."
echo

exit 0
