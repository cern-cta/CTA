#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

usage() { cat <<EOF 1>&2
Usage: $0 -i <eosMgmHost> -f <filename>
EOF
exit 1
}

while getopts "i:f:" o; do
  case "${o}" in
    i)
      EOS_MGM_HOST=${OPTARG}
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

if [[ ! -z "${error}" ]]; then
  echo -e "ERROR:\n${error}"
  exit 1
fi

echo "bash eos root://${EOS_MGM_HOST} rm /eos/ctaeos/cta/${TEST_FILE_NAME}"
eos root://${EOS_MGM_HOST} rm /eos/ctaeos/cta/${TEST_FILE_NAME}

SECONDS_PASSED=0
WAIT_FOR_RETRIEVED_FILE_TIMEOUT=10
while test true = $(xrdfs root://${EOS_MGM_HOST} query prepare 0 /eos/ctaeos/${TEST_FILE_NAME} | jq . | jq '.responses[0] | .path_exists'); do
  echo "Waiting for file to be deleted from tape: Seconds passed = ${SECONDS_PASSED}"
  sleep 1
  let SECONDS_PASSED=SECONDS_PASSED+1

  if test ${SECONDS_PASSED} == ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT}; then
    echo "Timed out after ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT} seconds waiting for file to be deleted"
    exit 1
  fi
done
