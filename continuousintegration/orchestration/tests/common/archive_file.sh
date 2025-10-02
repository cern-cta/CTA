#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
#
# SPDX-License-Identifier: GPL-3.0-or-later


EOS_MGM_HOST="ctaeos"

usage() { cat <<EOF 1>&2
Usage: $0 -f <filename>
EOF
exit 1
}

while getopts "f:" o; do
  case "${o}" in
    f)
      TEST_FILE_NAME=${OPTARG}
      ;;
    *)
      usage
      ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${TEST_FILE_NAME}" ]; then
  usage
fi

if [ ! -z "${error}" ]; then
  echo -e "ERROR:\n${error}"
  exit 1
fi

# get some common useful helpers for krb5
. /root/client_helper.sh

eospower_kdestroy
eospower_kinit

echo "xrdcp /etc/group root://${EOS_MGM_HOST}//eos/ctaeos/cta/${TEST_FILE_NAME}"
xrdcp /etc/group root://${EOS_MGM_HOST}//eos/ctaeos/cta/${TEST_FILE_NAME}

SECONDS_PASSED=0
WAIT_FOR_ARCHIVED_FILE_TIMEOUT=90
while test 0 == $(eos root://${EOS_MGM_HOST} info /eos/ctaeos/cta/${TEST_FILE_NAME} | awk '{print $4;}' | grep tape | wc -l); do
  echo "Waiting for file to be archived to tape: Seconds passed = ${SECONDS_PASSED}"
  sleep 1
  let SECONDS_PASSED=SECONDS_PASSED+1

  if test ${SECONDS_PASSED} == ${WAIT_FOR_ARCHIVED_FILE_TIMEOUT}; then
    echo "Timed out after ${WAIT_FOR_ARCHIVED_FILE_TIMEOUT} seconds waiting for file to be archived to tape"
    exit 1
  fi
done

echo
echo "FILE ARCHIVED TO TAPE"
echo
eos root://${EOS_MGM_HOST} info /eos/ctaeos/cta/${TEST_FILE_NAME}
echo
echo "Information about the testing file:"
echo "********"
eos root://${EOS_MGM_HOST} attr ls /eos/ctaeos/cta/${TEST_FILE_NAME}
eos root://${EOS_MGM_HOST} ls -l /eos/ctaeos/cta/${TEST_FILE_NAME}
eos root://${EOS_MGM_HOST} info /eos/ctaeos/cta/${TEST_FILE_NAME}
echo
echo "Removing disk replica as poweruser1:powerusers (12001:1200)"

KRB5CCNAME=/tmp/${EOSADMIN_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 eos -r 0 0 root://${EOS_MGM_HOST} file drop /eos/ctaeos/cta/${TEST_FILE_NAME} 1
echo
echo "Information about the testing file without disk replica"
eos root://${EOS_MGM_HOST} ls -l /eos/ctaeos/cta/${TEST_FILE_NAME}
eos root://${EOS_MGM_HOST} info /eos/ctaeos/cta/${TEST_FILE_NAME}
echo
