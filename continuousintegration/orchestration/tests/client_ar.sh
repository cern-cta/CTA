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
  eos root://${EOSINSTANCE} ls -l /eos/ctaeos/cta/${TEST_FILE_NAME}
  eos root://${EOSINSTANCE} info /eos/ctaeos/cta/${TEST_FILE_NAME}
echo
echo "Removing disk replica"
 XrdSecPROTOCOL=sss eos root://${EOSINSTANCE} file tag /eos/ctaeos/cta/${TEST_FILE_NAME} -1
echo
echo "Information about the testing file without disk replica"
  eos root://${EOSINSTANCE} ls -l /eos/ctaeos/cta/${TEST_FILE_NAME}
  eos root://${EOSINSTANCE} info /eos/ctaeos/cta/${TEST_FILE_NAME}
echo
echo "Trigerring EOS retrieve workflow as poweruser1:powerusers (12001:1200)"
echo "XrdSecPROTOCOL=sss xrdfs ${EOSINSTANCE} prepare -s \"/eos/ctaeos/cta/${TEST_FILE_NAME}?eos.ruid=12001&eos.rgid=1200\""
  XrdSecPROTOCOL=sss xrdfs ${EOSINSTANCE} prepare -s "/eos/ctaeos/cta/${TEST_FILE_NAME}?eos.ruid=12001&eos.rgid=1200"


# Wait for the copy to appear on disk
SECONDS_PASSED=0
WAIT_FOR_RETRIEVED_FILE_TIMEOUT=90
while test 0 = `eos root://${EOSINSTANCE} info /eos/ctaeos/cta/${TEST_FILE_NAME} | awk '{print $4;}' | grep -F "default.0" | wc -l`; do
  echo "Waiting for file to be retrieved from tape: Seconds passed = ${SECONDS_PASSED}"
  sleep 1
  let SECONDS_PASSED=SECONDS_PASSED+1

  if test ${SECONDS_PASSED} == ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT}; then
    echo "Timed out after ${WAIT_FOR_RETRIEVED_FILE_TIMEOUT} seconds waiting for file to be retrieved from tape"
    exit 1
  fi
done
echo
echo "FILE RETRIEVED FROM DISK"
echo
echo "Information about the testing file:"
echo "********"
  eos root://${EOSINSTANCE} attr ls /eos/ctaeos/cta/${TEST_FILE_NAME}
  eos root://${EOSINSTANCE} ls -l /eos/ctaeos/cta/${TEST_FILE_NAME}
  eos root://${EOSINSTANCE} info /eos/ctaeos/cta/${TEST_FILE_NAME}


msgNum=$(grep "\"File suc" /mnt/logs/tpsrv*/taped/cta/cta-taped.log | grep ${TEST_FILE_NAME} | tail -n 4 | wc -l)
if [ $msgNum == "4" ]; then
  echo "OK: all tests passed"
    rc=0
else
  echo "FAIL: tests failed"
    rc=1
fi

