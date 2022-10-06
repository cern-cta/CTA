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

usage() { cat <<EOF 1>&2
Usage: $0 -i <eosinstance> -f <filename>
EOF
exit 1
}

while getopts "i:f:" o; do
  case "${o}" in
    i)
      EOSINSTANCE=${OPTARG}
      ;;
    f)
      TEST_FILE_NAME=${OPTARG}
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

echo "bash eos root://${EOSINSTANCE} rm /eos/ctaeos/cta/${TEST_FILE_NAME}"
eos root://${EOSINSTANCE} rm /eos/ctaeos/cta/${TEST_FILE_NAME}

SECONDS_PASSED=0
WAIT_FOR_RETRIEVED_FILE_TIMEOUT=10
while test true = `xrdfs root://${EOSINSTANCE} query prepare 0 /eos/ctaeos/${TEST_FILE_NAME} | jq . | jq '.responses[0] | .path_exists'`; do
  echo "Waiting for file to be deleted from tape: Seconds passed = ${SECONDS_PASSED}"
  sleep 1
  let SECONDS_PASSED=SECONDS_PASSED+1

  if test ${SECONDS_PASSED} == ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT}; then
    echo "Timed out after ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT} seconds waiting for file to be deleted"
    exit 1
  fi
done
