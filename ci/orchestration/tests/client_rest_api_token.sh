#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Test CTA Tape Rest API compliance.

. /root/client_helper.sh

#admin_kdestroy &>/dev/null
#admin_kinit &>/dev/null

#eosadmin_kdestroy &>/dev/null
#eosadmin_kinit &>/dev/null

#NOTE: In this context it should be eos service names.
EOS_MGM_HOST="ctaeos"
EOS_MGM_PORT="8443"
NOW=$(date +%s)
LATER=$(echo "${NOW}+86400" | bc)

# Generate Tokens
#TOKEN_EOSUSER1=$(eosadmin_eos root://"${EOS_MGM_HOST}" token --tree --path '/eos/ctaeos/://:/api/' --expires "${LATER}" --owner user1 --group eosusers --permission rwx)
#TOKEN_EOSPOWER1=$(eosadmin_eos root://"${EOS_MGM_HOST}" token --tree --path '/eos/ctaeos/://:/api/' --expires "${LATER}" --owner poweruser1 --group powerusers --permission prwx)

# Get SCI token
SCI_TOKEN=$(cat /token_file)
SCI_TOKEN_EXP=$(echo $SCI_TOKEN | cut -d. -f2 | base64 --decode | jq '.exp')
if [ -z "$SCI_TOKEN_EXP" ] || [ "$SCI_TOKEN_EXP" -lt "$(date +%s)" ]; then
  echo "$(date +%s): SCI_TOKEN expired on $SCI_TOKEN_EXP."
  exit 1
fi

# By default check https connections certificates
# disable for now on Alma9 so that this is not on the critical path
CHECK_CERTIFICATES=1
grep -qi almalinux /etc/redhat-release 2>/dev/null && CHECK_CERTIFICATES=0 # Disable for now in Alma9
test ${CHECK_CERTIFICATES} -eq 0 && echo -e "\n\nWARNING: Certificate checks are disabled in this test.\n\n"

CURL_OPTS=""
test 0 -eq ${CHECK_CERTIFICATES} && CURL_OPTS+="--insecure"

#echo "Printing eosuser token dump"
#eos root://"${EOS_MGM_HOST}" token --token "${TOKEN_EOSUSER1}" | jq .
#echo
#echo "Printing poweruser token dump"
#eos root://"${EOS_MGM_HOST}" token --token "${TOKEN_EOSPOWER1}" | jq .
#echo

# Discover endpoint

REST_API_URI=$(curl ${CURL_OPTS} "https://${EOS_MGM_HOST}:${EOS_MGM_PORT}/.well-known/wlcg-tape-rest-api" | jq -r '.endpoints[] | select(.version == "v1") | .uri')

########################################################################################################################
# Archive files with the sci_token
########################################################################################################################

FILE1="/eos/ctaeos/preprod/test1/file"
FILE2="/eos/ctaeos/preprod/test2/file"
FILE3="/eos/ctaeos/preprod/test3/file"
FILE4="/eos/ctaeos/preprod/test4/file"

# Start by cleaning up any leftovers from previous tests

curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" -X DELETE "https://${EOS_MGM_HOST}:${EOS_MGM_PORT}/${FILE1}"
curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" -X DELETE "https://${EOS_MGM_HOST}:${EOS_MGM_PORT}/${FILE2}"
curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" -X DELETE "https://${EOS_MGM_HOST}:${EOS_MGM_PORT}/${FILE3}"
curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" -X DELETE "https://${EOS_MGM_HOST}:${EOS_MGM_PORT}/${FILE4}"

curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" -X DELETE "https://${EOS_MGM_HOST}:${EOS_MGM_PORT}/$(dirname "${FILE1}")"
curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" -X DELETE "https://${EOS_MGM_HOST}:${EOS_MGM_PORT}/$(dirname "${FILE2}")"
curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" -X DELETE "https://${EOS_MGM_HOST}:${EOS_MGM_PORT}/$(dirname "${FILE3}")"
curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" -X DELETE "https://${EOS_MGM_HOST}:${EOS_MGM_PORT}/$(dirname "${FILE4}")"

# Now add the new files

curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" -X MKCOL "https://${EOS_MGM_HOST}:${EOS_MGM_PORT}/$(dirname "${FILE1}")"
curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" -X MKCOL "https://${EOS_MGM_HOST}:${EOS_MGM_PORT}/$(dirname "${FILE2}")"
curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" -X MKCOL "https://${EOS_MGM_HOST}:${EOS_MGM_PORT}/$(dirname "${FILE3}")"
curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" -X MKCOL "https://${EOS_MGM_HOST}:${EOS_MGM_PORT}/$(dirname "${FILE4}")"

tmp_file=$(mktemp)
echo "Dummy" > "${tmp_file}"

curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" "https://${EOS_MGM_HOST}:${EOS_MGM_PORT}/${FILE1}" --upload-file "${tmp_file}"
curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" "https://${EOS_MGM_HOST}:${EOS_MGM_PORT}/${FILE2}" --upload-file "${tmp_file}"
curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" "https://${EOS_MGM_HOST}:${EOS_MGM_PORT}/${FILE3}" --upload-file "${tmp_file}"
curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" "https://${EOS_MGM_HOST}:${EOS_MGM_PORT}/${FILE4}" --upload-file "${tmp_file}"

rm -f "${tmp_file}"

########################################################################################################################
# Check that files have been archived with the sci_token and Tape REST API (ARCHIVEINFO)
########################################################################################################################

ARCHIVEINFO_REQ_BODY="{\"paths\":[\"${FILE1}\", \"${FILE2}\", \"${FILE3}\", \"${FILE4}\"]}"

echo "Archiving files..."

FINAL_COUNT=0
TIMEOUT=90
SECONDS_PASSED=0

# Wait for files to be archived

while test "${FINAL_COUNT}" -ne 4; do

  echo "$(date +%s): Waiting for files to be archived."
  if test "${SECONDS_PASSED}" -eq "${TIMEOUT}"; then
    echo "$(date +%s): Timed out waiting for files to be archived."
    exit 1
  fi

  FINAL_COUNT=$(curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" "${REST_API_URI}/archiveinfo/" -d "${ARCHIVEINFO_REQ_BODY}" | jq '.[] | select(.locality == "TAPE" ) | .locality' | wc -l)
  let SECONDS_PASSED=SECONDS_PASSED+1
  if [ "${FINAL_COUNT}" -ne 4 ]; then
    sleep 1
  fi
done

echo "All files archived."
curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" "${REST_API_URI}/archiveinfo/" -d "${ARCHIVEINFO_REQ_BODY}" | jq

########################################################################################################################
# Request files to be staged with the sci_token and Tape REST API (STAGE)
########################################################################################################################

echo "Staging files..."

FINAL_COUNT=0
TIMEOUT=90
SECONDS_PASSED=0

STAGE_REQ_BODY="{\"files\":[{\"path\":\"${FILE1}\"}, {\"path\":\"${FILE2}\"}, {\"path\":\"${FILE3}\"}, {\"path\":\"${FILE4}\"}]}"
REQ_ID=$(curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" "${REST_API_URI}/stage/" -d "$STAGE_REQ_BODY" | jq -r .requestId)

# Wait for files to be staged

while test "${FINAL_COUNT}" -ne 4; do

  echo "$(date +%s): Waiting for files to be staged."
  if test "${SECONDS_PASSED}" -eq "${TIMEOUT}"; then
    echo "$(date +%s): Timed out waiting for files to be staged."
    exit 1
  fi

  FINAL_COUNT=$(curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" "${REST_API_URI}/archiveinfo/" -d "${ARCHIVEINFO_REQ_BODY}" | jq '.[] | select(.locality == "DISK_AND_TAPE" ) | .locality' | wc -l)
  let SECONDS_PASSED=SECONDS_PASSED+1
  if [ "${FINAL_COUNT}" -ne 4 ]; then
    sleep 1
  fi
done

echo "Checking stage request"

STAGE_REQUEST_QUERY=$(curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" "${REST_API_URI}/stage/${REQ_ID}")
STAGE_REQUEST_COUNT=$(echo ${STAGE_REQUEST_QUERY} | jq '.files[] | select(.onDisk == true) | .path' | wc -l)
echo ${STAGE_REQUEST_COUNT}

if test "${STAGE_REQUEST_COUNT}" -ne 4; then
  echo "Stage request query not showing all files on disk"
  exit 1
fi

echo "All files staged."
curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" "${REST_API_URI}/archiveinfo/" -d "${ARCHIVEINFO_REQ_BODY}" | jq

########################################################################################################################
# Request files to be released with the sci_token and Tape REST API (RELEASE)
########################################################################################################################

echo "Releasing files..."

FINAL_COUNT=0
TIMEOUT=90
SECONDS_PASSED=0
RELEASE_REQ_BODY="{\"paths\":[\"${FILE1}\", \"${FILE2}\", \"${FILE3}\", \"${FILE4}\"]}"
curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" "${REST_API_URI}/release/${REQ_ID}" -d "${RELEASE_REQ_BODY}"

while test "${FINAL_COUNT}" -ne 4; do

  echo "$(date +%s): Waiting for files to be released."
  if test "${SECONDS_PASSED}" -eq "${TIMEOUT}"; then
    echo "$(date +%s): Timed out waiting for files to be released."
    exit 1
  fi

  FINAL_COUNT=$(curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" "${REST_API_URI}/archiveinfo/" -d "${ARCHIVEINFO_REQ_BODY}" | jq '.[] | select(.locality == "TAPE" ) | .locality' | wc -l)
  let SECONDS_PASSED=SECONDS_PASSED+1
  if [ "${FINAL_COUNT}" -ne 4 ]; then
    sleep 1
  fi
done

echo "All files released."
curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" "${REST_API_URI}/archiveinfo/" -d "${ARCHIVEINFO_REQ_BODY}" | jq

########################################################################################################################
# Request files to be cancelled with the sci_token and Tape REST API (CANCEL)
########################################################################################################################

echo "Cancelling files..."

FINAL_COUNT=0
TIMEOUT=90
SECONDS_PASSED=0
CANCEL_REQ_BODY="{\"paths\":[\"${FILE1}\", \"${FILE2}\", \"${FILE3}\", \"${FILE4}\"]}"
curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" "${REST_API_URI}/stage/${REQ_ID}/cancel" -d "${CANCEL_REQ_BODY}"

while test "${FINAL_COUNT}" -ne 4; do

  echo "$(date +%s): Waiting for files to be cancelled."
  if test "${SECONDS_PASSED}" -eq "${TIMEOUT}"; then
    echo "$(date +%s): Timed out waiting for files to be cancelled."
    exit 1
  fi

  FINAL_COUNT=$(curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" "${REST_API_URI}/archiveinfo/" -d "${ARCHIVEINFO_REQ_BODY}" | jq '.[] | select(.locality == "TAPE" ) | .locality' | wc -l)
  let SECONDS_PASSED=SECONDS_PASSED+1
  if [ "${FINAL_COUNT}" -ne 4 ]; then
    sleep 1
  fi
done

echo "All files cancelled."
curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" "${REST_API_URI}/archiveinfo/" -d "${ARCHIVEINFO_REQ_BODY}" | jq

########################################################################################################################
# Request request to be deleted (DELETE)
########################################################################################################################

echo "Deleting request..."
curl ${CURL_OPTS} -L -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" "${REST_API_URI}/stage/${REQ_ID}" -X DELETE

# Check that request no longer exists

STAGE_REQUEST_QUERY=$(curl ${CURL_OPTS} -L --fail -H "Accept: application/json" -H "Authorization: Bearer ${SCI_TOKEN}" "${REST_API_URI}/stage/${REQ_ID}")

if [ "$?" -eq 0 ]; then
  echo "Stage request still exists"
  echo "${STAGE_REQUEST_QUERY}"
  exit 1
else
  echo "Stage request deleted"
fi

########################################################################################################################
# Test completed. Clean resources.
########################################################################################################################

echo "$(date +%s): Rest API test completed successfully. OK."

# Remove the file manually.
eos root://"${EOS_MGM_HOST}" rm /eos/ctaeos/preprod/test_http-rest-api
IFS=$OLDIFS
echo "$(date +%s): Rest API test completed successfully. OK."
