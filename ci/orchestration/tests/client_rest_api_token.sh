#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Test CTA Tape Rest API compliance using WLCG tokens

EOS_MGM_HOST="ctaeos"
EOS_MGM_PORT="8443"

# Get WLCG tokens

WLCG_TOKEN_OTHER=$(cat /etc/cta/wlcg-token-other.jwt)
WLCG_TOKEN_STAGE_ALL=$(cat /etc/cta/wlcg-token-stage-all.jwt)
WLCG_TOKEN_POLL_ALL=$(cat /etc/cta/wlcg-token-poll-all.jwt)
WLCG_TOKEN_STAGE_TEST1=$(cat /etc/cta/wlcg-token-stage-test1.jwt)

WLCG_TOKEN_OTHER_TEXT="no 'storage.stage/poll'"
WLCG_TOKEN_STAGE_ALL_TEXT="'storage.stage:/'"
WLCG_TOKEN_POLL_ALL_TEXT="'storage.poll:/'"
WLCG_TOKEN_STAGE_TEST1_TEXT="'storage.stage:/eos/ctaeos/preprod/test1/'"

# By default check https connections certificates

CHECK_CERTIFICATES=1
grep -qi almalinux /etc/redhat-release 2>/dev/null && CHECK_CERTIFICATES=0 # Disable for now in Alma9
test ${CHECK_CERTIFICATES} -eq 0 && echo -e "\n\nWARNING: Certificate checks are disabled in this test.\n\n"

CURL_OPTS=""
test 0 -eq ${CHECK_CERTIFICATES} && CURL_OPTS+="--insecure"

# Discover Tape REST API endpoint on EOS

REST_API_URI=$(curl ${CURL_OPTS} "https://${EOS_MGM_HOST}:${EOS_MGM_PORT}/.well-known/wlcg-tape-rest-api" | jq -r '.endpoints[] | select(.version == "v1") | .uri')

########################################################################################################################
# Archive files with the WLCG token
########################################################################################################################

FILE1="/eos/ctaeos/preprod/test1/file"
FILE2="/eos/ctaeos/preprod/test2/file"

# Start by cleaning up any leftovers from previous tests

curl ${CURL_OPTS} -L -s -H "Accept: application/json" -H "Authorization: Bearer ${WLCG_TOKEN_OTHER}" -X DELETE "https://${EOS_MGM_HOST}:${EOS_MGM_PORT}/${FILE1}"
curl ${CURL_OPTS} -L -s -H "Accept: application/json" -H "Authorization: Bearer ${WLCG_TOKEN_OTHER}" -X DELETE "https://${EOS_MGM_HOST}:${EOS_MGM_PORT}/${FILE2}"

curl ${CURL_OPTS} -L -s -H "Accept: application/json" -H "Authorization: Bearer ${WLCG_TOKEN_OTHER}" -X DELETE "https://${EOS_MGM_HOST}:${EOS_MGM_PORT}/$(dirname "${FILE1}")"
curl ${CURL_OPTS} -L -s -H "Accept: application/json" -H "Authorization: Bearer ${WLCG_TOKEN_OTHER}" -X DELETE "https://${EOS_MGM_HOST}:${EOS_MGM_PORT}/$(dirname "${FILE2}")"

# Now add the new files

tmp_file=$(mktemp)
echo "Dummy" > "${tmp_file}"

curl ${CURL_OPTS} -L -s -H "Accept: application/json" -H "Authorization: Bearer ${WLCG_TOKEN_OTHER}" -X MKCOL "https://${EOS_MGM_HOST}:${EOS_MGM_PORT}/$(dirname "${FILE1}")"
curl ${CURL_OPTS} -L -s -H "Accept: application/json" -H "Authorization: Bearer ${WLCG_TOKEN_OTHER}" -X MKCOL "https://${EOS_MGM_HOST}:${EOS_MGM_PORT}/$(dirname "${FILE2}")"

curl ${CURL_OPTS} -L -s -H "Accept: application/json" -H "Authorization: Bearer ${WLCG_TOKEN_OTHER}" "https://${EOS_MGM_HOST}:${EOS_MGM_PORT}/${FILE1}" --upload-file "${tmp_file}"
curl ${CURL_OPTS} -L -s -H "Accept: application/json" -H "Authorization: Bearer ${WLCG_TOKEN_OTHER}" "https://${EOS_MGM_HOST}:${EOS_MGM_PORT}/${FILE2}" --upload-file "${tmp_file}"

rm -f "${tmp_file}"

########################################################################################################################
# Check that files have been archived with the sci_token and Tape REST API (ARCHIVEINFO)
########################################################################################################################

ARCHIVEINFO_REQ_BODY="{\"paths\":[\"${FILE1}\", \"${FILE2}\"]}"

echo "Archiving files..."

FINAL_COUNT=0
TIMEOUT=90
SECONDS_PASSED=0

# Wait for files to be archived

while test "${FINAL_COUNT}" -ne 2; do

  echo "$(date +%s): Waiting for files to be archived."
  if test "${SECONDS_PASSED}" -eq "${TIMEOUT}"; then
    echo "$(date +%s): Timed out waiting for files to be archived."
    exit 1
  fi

  FINAL_COUNT=$(curl ${CURL_OPTS} -L -s -H "Accept: application/json" -H "Authorization: Bearer ${WLCG_TOKEN_STAGE_ALL}" "${REST_API_URI}/archiveinfo/" -d "${ARCHIVEINFO_REQ_BODY}" | jq '.[] | select(.locality == "TAPE" ) | .locality' | wc -l)
  let SECONDS_PASSED=SECONDS_PASSED+1
  if [ "${FINAL_COUNT}" -ne 2 ]; then
    sleep 1
  fi
done

echo "All files archived."

echo "Checking archive info (GET ARCHIVEINFO) with different tokens..."

ARCHIVEINFO_OTHER_RESP=$(curl ${CURL_OPTS} -L -s -H "Accept: application/json" -H "Authorization: Bearer ${WLCG_TOKEN_OTHER}" "${REST_API_URI}/archiveinfo/" -d "${ARCHIVEINFO_REQ_BODY}")
ARCHIVEINFO_STAGE_ALL_RESP=$(curl ${CURL_OPTS} -L -s -H "Accept: application/json" -H "Authorization: Bearer ${WLCG_TOKEN_STAGE_ALL}" "${REST_API_URI}/archiveinfo/" -d "${ARCHIVEINFO_REQ_BODY}")
ARCHIVEINFO_POLL_ALL_RESP=$(curl ${CURL_OPTS} -L -s -H "Accept: application/json" -H "Authorization: Bearer ${WLCG_TOKEN_POLL_ALL}" "${REST_API_URI}/archiveinfo/" -d "${ARCHIVEINFO_REQ_BODY}")
ARCHIVEINFO_STAGE_TEST1_RESP=$(curl ${CURL_OPTS} -L -s -H "Accept: application/json" -H "Authorization: Bearer ${WLCG_TOKEN_STAGE_TEST1}" "${REST_API_URI}/archiveinfo/" -d "${ARCHIVEINFO_REQ_BODY}")

SUCCESS=true

echo "With ${WLCG_TOKEN_OTHER_TEXT} token: "
if [[ "$(echo ${ARCHIVEINFO_OTHER_RESP} | jq -r '.[] | select(has("error")) | .error' | wc -l)" -eq 2 ]] &&
   [[ "$(echo ${ARCHIVEINFO_OTHER_RESP} | jq -r '.[] | select(has("locality")) | .locality' | wc -l)" -eq 0 ]]; then
  echo "OK"
else
  echo "ERROR: Unexpected result"
  echo ${ARCHIVEINFO_OTHER_RESP}
  SUCCESS=false
fi

echo "With ${WLCG_TOKEN_STAGE_ALL_TEXT} token: "
if [[ "$(echo ${ARCHIVEINFO_STAGE_ALL_RESP} | jq -r '.[] | select(has("error")) | .error' | wc -l)" -eq 0 ]] &&
   [[ "$(echo ${ARCHIVEINFO_STAGE_ALL_RESP} | jq -r '.[] | select(has("locality")) | .locality' | wc -l)" -eq 2 ]]; then
  echo "OK"
else
  echo "ERROR: Unexpected result"
  echo ${ARCHIVEINFO_STAGE_ALL_RESP}
  SUCCESS=false
fi

echo "With ${WLCG_TOKEN_POLL_ALL_TEXT} token: "
if [[ "$(echo ${ARCHIVEINFO_POLL_ALL_RESP} | jq -r '.[] | select(has("error")) | .error' | wc -l)" -eq 0 ]] &&
   [[ "$(echo ${ARCHIVEINFO_POLL_ALL_RESP} | jq -r '.[] | select(has("locality")) | .locality' | wc -l)" -eq 2 ]]; then
  echo "OK"
else
  echo "ERROR: Unexpected result"
  echo ${ARCHIVEINFO_POLL_ALL_RESP}
  SUCCESS=false
fi

echo "With ${WLCG_TOKEN_STAGE_TEST1_TEXT} token: "
if [[ "$(echo ${ARCHIVEINFO_STAGE_TEST1_RESP} | jq -r '.[] | select(has("error")) | .error' | wc -l)" -eq 1 ]] &&
   [[ "$(echo ${ARCHIVEINFO_STAGE_TEST1_RESP} | jq -r '.[] | select(has("locality")) | .locality')" == "TAPE" ]]; then
  echo "OK"
else
  echo "ERROR: Unexpected result"
  echo ${ARCHIVEINFO_STAGE_TEST1_RESP}
  SUCCESS=false
fi

if ! "${SUCCESS}"; then
  exit 1
fi

########################################################################################################################
# Request files to be staged with the WLCG_TOKEN_STAGE_ALL and Tape REST API (STAGE)
########################################################################################################################

echo "New staging request submission (POST STAGE) with ${WLCG_TOKEN_STAGE_ALL_TEXT} token..."

FINAL_COUNT=0
TIMEOUT=90
SECONDS_PASSED=0

STAGE_REQ_BODY="{\"files\":[{\"path\":\"${FILE1}\"}, {\"path\":\"${FILE2}\"}]}"
REQ_ID=$(curl ${CURL_OPTS} -L -s -H "Accept: application/json" -H "Authorization: Bearer ${WLCG_TOKEN_STAGE_ALL}" "${REST_API_URI}/stage/" -d "$STAGE_REQ_BODY" | jq -r .requestId)

# Wait for files to be staged

while test "${FINAL_COUNT}" -ne 2; do

  echo "$(date +%s): Waiting for files to be staged."
  if test "${SECONDS_PASSED}" -eq "${TIMEOUT}"; then
    echo "$(date +%s): Timed out waiting for files to be staged."
    exit 1
  fi

  FINAL_COUNT=$(curl ${CURL_OPTS} -L -s -H "Accept: application/json" -H "Authorization: Bearer ${WLCG_TOKEN_STAGE_ALL}" "${REST_API_URI}/archiveinfo/" -d "${ARCHIVEINFO_REQ_BODY}" | jq '.[] | select(.locality == "DISK_AND_TAPE" ) | .locality' | wc -l)
  let SECONDS_PASSED=SECONDS_PASSED+1
  if [ "${FINAL_COUNT}" -ne 2 ]; then
    sleep 1
  fi
done

echo "All files staged."

echo "Checking stage request (GET STAGE) with different tokens..."

GET_STAGE_OTHER_RESP=$(curl ${CURL_OPTS} -L -s -H "Accept: application/json" -H "Authorization: Bearer ${WLCG_TOKEN_OTHER}" "${REST_API_URI}/stage/${REQ_ID}")
GET_STAGE_STAGE_ALL_RESP=$(curl ${CURL_OPTS} -L -s -H "Accept: application/json" -H "Authorization: Bearer ${WLCG_TOKEN_STAGE_ALL}" "${REST_API_URI}/stage/${REQ_ID}")
GET_STAGE_OTHER_RESP=$(curl ${CURL_OPTS} -L -s -H "Accept: application/json" -H "Authorization: Bearer ${WLCG_TOKEN_POLL_ALL}" "${REST_API_URI}/stage/${REQ_ID}")
GET_STAGE_POLL_ALL_RESP=$(curl ${CURL_OPTS} -L -s -H "Accept: application/json" -H "Authorization: Bearer ${WLCG_TOKEN_STAGE_TEST1}" "${REST_API_URI}/stage/${REQ_ID}")

SUCCESS=true

echo "With ${WLCG_TOKEN_OTHER_TEXT} token: "
if [[ "$(echo ${GET_STAGE_OTHER_RESP} | jq -r '.files[] | select(has("error")) | .error' | wc -l)" -eq 2 ]] &&
   [[ "$(echo ${GET_STAGE_OTHER_RESP} | jq -r '.files[] | select(has("onDisk")) | .onDisk' | wc -l)" -eq 0 ]]; then
  echo "OK"
else
  echo "ERROR: Unexpected result"
  echo ${GET_STAGE_OTHER_RESP}
  SUCCESS=false
fi

echo "With ${WLCG_TOKEN_STAGE_ALL_TEXT} token: "
if [[ "$(echo ${GET_STAGE_STAGE_ALL_RESP} | jq -r '.files[] | select(has("error")) | .error' | wc -l)" -eq 0 ]] &&
   [[ "$(echo ${GET_STAGE_STAGE_ALL_RESP} | jq -r '.files[] | select(has("onDisk")) | .onDisk' | wc -l)" -eq 2 ]]; then
  echo "OK"
else
  echo "ERROR: Unexpected result"
  echo ${GET_STAGE_STAGE_ALL_RESP}
  SUCCESS=false
fi

echo "With ${WLCG_TOKEN_POLL_ALL_TEXT} token: "
if [[ "$(echo ${GET_STAGE_OTHER_RESP} | jq -r '.files[] | select(has("error")) | .error' | wc -l)" -eq 0 ]] &&
   [[ "$(echo ${GET_STAGE_OTHER_RESP} | jq -r '.files[] | select(has("onDisk")) | .onDisk' | wc -l)" -eq 2 ]]; then
  echo "OK"
else
  echo "ERROR: Unexpected result"
  echo ${GET_STAGE_OTHER_RESP}
  SUCCESS=false
fi

echo "With ${WLCG_TOKEN_STAGE_TEST1_TEXT} token: "
if [[ "$(echo ${GET_STAGE_POLL_ALL_RESP} | jq -r '.files[] | select(has("error")) | .error' | wc -l)" -eq 1 ]] &&
   [[ "$(echo ${GET_STAGE_POLL_ALL_RESP} | jq -r '.files[] | select(has("onDisk")) | .onDisk')" == "true" ]]; then
  echo "OK"
else
  echo "ERROR: Unexpected result"
  echo ${GET_STAGE_POLL_ALL_RESP}
  SUCCESS=false
fi

if ! "${SUCCESS}"; then
  exit 1
fi

########################################################################################################################
# Request files to be released with the WLCG_TOKEN_STAGE_ALL and Tape REST API (RELEASE)
########################################################################################################################

echo "Releasing files..."

FINAL_COUNT=0
TIMEOUT=90
SECONDS_PASSED=0
RELEASE_REQ_BODY="{\"paths\":[\"${FILE1}\", \"${FILE2}\"]}"
curl ${CURL_OPTS} -L -s -H "Accept: application/json" -H "Authorization: Bearer ${WLCG_TOKEN_STAGE_ALL}" "${REST_API_URI}/release/${REQ_ID}" -d "${RELEASE_REQ_BODY}"

while test "${FINAL_COUNT}" -ne 2; do

  echo "$(date +%s): Waiting for files to be released."
  if test "${SECONDS_PASSED}" -eq "${TIMEOUT}"; then
    echo "$(date +%s): Timed out waiting for files to be released."
    exit 1
  fi

  FINAL_COUNT=$(curl ${CURL_OPTS} -L -s -H "Accept: application/json" -H "Authorization: Bearer ${WLCG_TOKEN_STAGE_ALL}" "${REST_API_URI}/archiveinfo/" -d "${ARCHIVEINFO_REQ_BODY}" | jq '.[] | select(.locality == "TAPE" ) | .locality' | wc -l)
  let SECONDS_PASSED=SECONDS_PASSED+1
  if [ "${FINAL_COUNT}" -ne 2 ]; then
    sleep 1
  fi
done

echo "All files released."
curl ${CURL_OPTS} -L -s -H "Accept: application/json" -H "Authorization: Bearer ${WLCG_TOKEN_STAGE_ALL}" "${REST_API_URI}/archiveinfo/" -d "${ARCHIVEINFO_REQ_BODY}" | jq

########################################################################################################################
# Request files to be cancelled with the WLCG_TOKEN_STAGE_ALL and Tape REST API (CANCEL)
########################################################################################################################

echo "Cancelling files..."

FINAL_COUNT=0
TIMEOUT=90
SECONDS_PASSED=0
CANCEL_REQ_BODY="{\"paths\":[\"${FILE1}\", \"${FILE2}\"]}"
curl ${CURL_OPTS} -L -s -H "Accept: application/json" -H "Authorization: Bearer ${WLCG_TOKEN_STAGE_ALL}" "${REST_API_URI}/stage/${REQ_ID}/cancel" -d "${CANCEL_REQ_BODY}"

while test "${FINAL_COUNT}" -ne 2; do

  echo "$(date +%s): Waiting for files to be cancelled."
  if test "${SECONDS_PASSED}" -eq "${TIMEOUT}"; then
    echo "$(date +%s): Timed out waiting for files to be cancelled."
    exit 1
  fi

  FINAL_COUNT=$(curl ${CURL_OPTS} -L -s -H "Accept: application/json" -H "Authorization: Bearer ${WLCG_TOKEN_STAGE_ALL}" "${REST_API_URI}/archiveinfo/" -d "${ARCHIVEINFO_REQ_BODY}" | jq '.[] | select(.locality == "TAPE" ) | .locality' | wc -l)
  let SECONDS_PASSED=SECONDS_PASSED+1
  if [ "${FINAL_COUNT}" -ne 2 ]; then
    sleep 1
  fi
done

echo "All files cancelled."
curl ${CURL_OPTS} -L -s -H "Accept: application/json" -H "Authorization: Bearer ${WLCG_TOKEN_STAGE_ALL}" "${REST_API_URI}/archiveinfo/" -d "${ARCHIVEINFO_REQ_BODY}" | jq

########################################################################################################################
# Request request to be deleted (DELETE)
########################################################################################################################

echo "Deleting request..."
curl ${CURL_OPTS} -L -s -H "Accept: application/json" -H "Authorization: Bearer ${WLCG_TOKEN_STAGE_ALL}" "${REST_API_URI}/stage/${REQ_ID}" -X DELETE

# Check that request no longer exists

HTTP_CODE=$(curl ${CURL_OPTS} -L --fail -s -H "Accept: application/json" -H "Authorization: Bearer ${WLCG_TOKEN_STAGE_ALL}" "${REST_API_URI}/stage/${REQ_ID}" -w "%{http_code}")

if [[ "$?" -eq 0 ]]; then
  echo "Stage request still exists"
  exit 1
elif [[ "${HTTP_CODE}" -ne 404 ]]; then
  echo "Wrong HTTP code returned: ${HTTP_CODE}"
  exit 1
else
  echo "Stage request no longer exists"
fi

########################################################################################################################
# Test completed. Clean resources.
########################################################################################################################

echo "Rest API test completed successfully. OK."
