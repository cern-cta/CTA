#!/bin/bash


EOSINSTANCE=ctaeos


TEST_FILE_NAME=`uuidgen`
echo "xrdcp /etc/group root://localhost//eos/ctaeos/cta/${TEST_FILE_NAME}"
xrdcp /etc/group root://${EOSINSTANCE}//eos/ctaeos/cta/${TEST_FILE_NAME}

SECONDS_PASSED=0
WAIT_FOR_ARCHIVED_FILE_TIMEOUT=90
while test 0 = `eos root://${EOSINSTANCE} info /eos/ctaeos/cta/${TEST_FILE_NAME} | awk '{print $4;}' | grep tape | wc -l`; do
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
  eos root://${EOSINSTANCE} info /eos/ctaeos/cta/${TEST_FILE_NAME}
echo
echo "Removing disk replica"
 XrdSecPROTOCOL=sss eos root://${EOSINSTANCE} file tag /eos/ctaeos/cta/${TEST_FILE_NAME} -1
echo
echo "Information about the testing file without disk replica"
  eos root://${EOSINSTANCE} info /eos/ctaeos/cta/${TEST_FILE_NAME}
echo
echo "trigger EOS retrive workflow"
echo "xrdfs localhost prepare -s /eos/ctaeos/cta/${TEST_FILE_NAME}"
#  XrdSecPROTOCOL=sss xrdfs ${EOSINSTANCE} prepare -s /eos/ctaeos/cta/${TEST_FILE_NAME}
  XrdSecPROTOCOL=sss xrdfs ctaeos prepare -s "/eos/ctaeos/cta/${TEST_FILE_NAME}?eos.ruid=0&eos.rgid=0"
