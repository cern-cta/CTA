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

EOSINSTANCE=ctaeos

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

echo "xrdcp /etc/group root://${EOSINSTANCE}//eos/ctaeos/cta/${TEST_FILE_NAME}"
xrdcp /etc/group root://${EOSINSTANCE}//eos/ctaeos/cta/${TEST_FILE_NAME}

SECONDS_PASSED=0
WAIT_FOR_ARCHIVED_FILE_TIMEOUT=90
while test 0 == $(eos root://${EOSINSTANCE} info /eos/ctaeos/cta/${TEST_FILE_NAME} | awk '{print $4;}' | grep tape | wc -l); do
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
eos root://${EOSINSTANCE} info /eos/ctaeos/cta/${TEST_FILE_NAME}
echo
echo "Information about the testing file:"
echo "********"
  eos root://${EOSINSTANCE} attr ls /eos/ctaeos/cta/${TEST_FILE_NAME}
  eos root://${EOSINSTANCE} ls -l /eos/ctaeos/cta/${TEST_FILE_NAME}
  eos root://${EOSINSTANCE} info /eos/ctaeos/cta/${TEST_FILE_NAME}
echo
echo "Removing disk replica as poweruser1:powerusers (12001:1200)"

XrdSecPROTOCOL=sss eos -r 0 0 root://${EOSINSTANCE} file drop /eos/ctaeos/cta/${TEST_FILE_NAME} 1
echo
echo "Information about the testing file without disk replica"
  eos root://${EOSINSTANCE} ls -l /eos/ctaeos/cta/${TEST_FILE_NAME}
  eos root://${EOSINSTANCE} info /eos/ctaeos/cta/${TEST_FILE_NAME}
echo
