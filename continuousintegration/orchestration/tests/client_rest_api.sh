#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
#
# SPDX-License-Identifier: GPL-3.0-or-later


# Test CTA Tape Rest API compliance.

. /root/client_helper.sh

admin_kdestroy &>/dev/null
admin_kinit &>/dev/null

eosadmin_kdestroy &>/dev/null
eosadmin_kinit &>/dev/null

#NOTE: In this context it should be eos service names.
EOS_MGM_HOST="ctaeos"
NOW=$(date +%s)
LATER=$(echo "${NOW}+86400" | bc)

# Generate Tokens
TOKEN_EOSUSER1=$(eosadmin_eos root://"${EOS_MGM_HOST}" token --tree --path '/eos/ctaeos/://:/api/' --expires "${LATER}" --owner user1 --group eosusers --permission rwx)

TOKEN_EOSPOWER1=$(eosadmin_eos root://"${EOS_MGM_HOST}" token --tree --path '/eos/ctaeos/://:/api/' --expires "${LATER}" --owner poweruser1 --group powerusers --permission prwx)

# By default check https connections certificates
# disable for now on Alma9 so that this is not on the critical path
CHECK_CERTIFICATES=1
grep -qi almalinux /etc/redhat-release 2>/dev/null && CHECK_CERTIFICATES=0 # Disable for now in Alma9
test ${CHECK_CERTIFICATES} -eq 0 && echo -e "\n\nWARNING: Certificate checks are disabled in this test.\n\n"

CURL_OPTS=""
test 0 -eq ${CHECK_CERTIFICATES} && CURL_OPTS+="--insecure"

echo "Printing eosuser token dump"
eos root://"${EOS_MGM_HOST}" token --token "${TOKEN_EOSUSER1}" | jq .
echo
echo "Printing poweruser token dump"
eos root://"${EOS_MGM_HOST}" token --token "${TOKEN_EOSPOWER1}" | jq .
echo

# Discover endpoint of v1, /.well-known/wlcg-tape-rest-api in instance http(s)://ctaeos:8443
HTTPS_URI=$(curl --insecure "https://${EOS_MGM_HOST}:8443/.well-known/wlcg-tape-rest-api" | jq -r '.endpoints[] | select(.version == "v1") | .uri')

echo "$(date +%s): Testing compliance of .well-known/wlcg-tape-rest-api endpoint"
WELL_KNOWN=$(curl --insecure "https://${EOS_MGM_HOST}:8443/.well-known/wlcg-tape-rest-api")
echo "Full well known: ${WELL_KNOWN}"

test $(echo ${WELL_KNOWN} | jq -r .sitename | wc -l) -eq 1 && echo "Site name:  $(echo ${WELL_KNOWN} | jq -r .sitename)"  || { echo "ERROR: Sitename not in the response from .well-known"; exit 1; }
echo "Description: $(echo ${WELL_KNOWN} | jq -r .description)"
test   $(echo ${WELL_KNOWN} | jq -r .endpoints[0].uri | wc -l) -eq 1 && echo "URI:       $(echo ${WELL_KNOWN} | jq -r .endpoints[0].uri)" || { echo "ERROR: URI not found in the response from .well-known."; exit 1; }
test $(echo ${WELL_KNOWN} | jq -r .endpoints[0].version | wc -l) -eq 1 && echo "Version:   $(echo ${WELL_KNOWN} | jq -r .endpoints[0].version)" || { echo "ERROR: Metadata not found in the response from .well-known"; exit 1;}
echo "Metadata: $(echo ${WELL_KNOWN} | jq -r .endpoints[0].metadata)"

# Archive the file.

eos root://"${EOS_MGM_HOST}" rm /eos/ctaeos/preprod/test_http-rest-api 2>/dev/null # Delete the file if present so that we can run the test multiple times
INIT_COUNT=$(eos root://"${EOS_MGM_HOST}" ls -y /eos/ctaeos/preprod/ | grep test_http-rest-api | wc -l)
test "${INIT_COUNT}" -eq 0 || { echo "Test file test_http-rest-api already in EOS before archiving."; exit 1; }
tmp_file=$(mktemp)
echo "Dummy" > "${tmp_file}"

curl -L --insecure -H "Accept: application/json" -H "Authorization: Bearer ${TOKEN_EOSUSER1}" "https://${EOS_MGM_HOST}:8443/eos/ctaeos/preprod/test_http-rest-api" --upload-file "${tmp_file}"

FINAL_COUNT=0
TIMEOUT=90
SECONDS_PASSED=0
while test "${FINAL_COUNT}" -eq 0; do

  echo "$(date +%s): Waiting for file to be archived."

  if test "${SECONDS_PASSED}" -eq "${TIMEOUT}"; then
    echo "$(date +%s): Timed out waiting for file to be archived."
    exit 1
  fi

  FINAL_COUNT=$(eos root://"${EOS_MGM_HOST}" ls -y /eos/ctaeos/preprod | grep "test_http-rest-api" | grep 'd0::t1' | wc -l)

  let SECONDS_PASSED=SECONDS_PASSED+1
  sleep 1
done
rm -f "${tmp_file}"
echo "$(date +%s): File succesfully archived."
eos root://"${EOS_MGM_HOST}" ls -y /eos/ctaeos/preprod

# Archive Info Request information about the progression of writing files
# to tape.
echo "$(date +%s): Showing archiveinfo..."
curl ${CURL_OPTS} -L --capath /etc/grid-security/certificates -H "Accept: application/json" -H "Authorization: Bearer ${TOKEN_EOSPOWER1}" ${HTTPS_URI}/archiveinfo/ -d '{"paths":["/eos/ctaeos/preprod/test_http-rest-api"]}' | jq .

# Stage
# Request that tape-stored files are made available with disk latency.

REQ_ID=$(curl ${CURL_OPTS} -L --capath /etc/grid-security/certificates -H "Accept: application/json" -H "Authorization: Bearer ${TOKEN_EOSPOWER1}" ${HTTPS_URI}/stage/ -d '{"files":[{"path":"/eos/ctaeos/preprod/test_http-rest-api"}]}' | jq -r .requestId)

SECONDS_PASSED=0
FINAL_COUNT=0
while test "${FINAL_COUNT}" -eq 0; do
  echo "$(date +%s): Waiting for file to be staged."

  if test "${SECONDS_PASSED}" -eq "${TIMEOUT}"; then
    echo "$(date +%s): Timed out waiting for file to be staged."
    exit 1
  fi

  FINAL_COUNT=$(eos root://"${EOS_MGM_HOST}" ls -y /eos/ctaeos/preprod | grep 'test_http-rest-api' | grep 'd1::t1' | wc -l)

  let SECONDS_PASSED=SECONDS_PASSED+1
  sleep 1
done
echo "$(date +%s): File staged successfully."
eos root://"${EOS_MGM_HOST}" ls -y /eos/ctaeos/preprod

# Release
# Indicate that files previously staged through STAGE are no longer required to have disk like latency.
# For CTA the actual REQ_ID is not necessary, any character after 'release/'
# will make the request valid. But dCache and StoRM use them. More info:
#      https://gitlab.cern.ch/cta/CTA/-/issues/384
curl ${CURL_OPTS} -L --capath /etc/grid-security/certificates -H "Accept: application/json" -H "Authorization: Bearer ${TOKEN_EOSPOWER1}" "${HTTPS_URI}/release/${REQ_ID}" -d '{"paths":["/eos/ctaeos/preprod/test_http-rest-api"]}'

EVICT_COUNT=$(eos root://"${EOS_MGM_HOST}" ls -y /eos/ctaeos/preprod | grep 'test_http-rest-api' | grep 'd0::t1' | wc -l)
test "${EVICT_COUNT}" -eq 1 || { echo "$(date +%s): File did not get evicted.";  exit 1; }
echo "$(date +%s): File successfully evicted."
eos root://"${EOS_MGM_HOST}" ls -y /eos/ctaeos/preprod

# Cancellation and deletion.
# Steps:
# 1. Check file evict counter.
echo "$(date +%s): Checking evict counter before abort prepare."
OLDIFS=$IFS
IFS='::'
tmp=$(eos root://"${EOS_MGM_HOST}" ls -y /eos/ctaeos/preprod | grep test_http-rest-api | awk '{ print $1 }')
split1=( $tmp )
EVICT_INIT=${split1[0]}
echo "Evict start counter: ${EVICT_INIT}"


#    2. Put drive down
IFS=' ' read -r -a dr_names <<< $(admin_cta --json dr ls | jq -r '.[] | select(.driveStatus=="UP") | .driveName')
IFS=' ' read -r -a dr_names_down <<< $(admin_cta --json dr ls | jq -r '.[] | select(.driveStatus=="DOWN") | .driveName')
echo "$(date +%s): Putting drives down."

for drive in "${dr_names[@]}"; do
  echo "Putting drive $drive down."
  admin_cta dr down "$drive" -r "Tape Rest API abort prepare test"
  sleep 3
done


#    3. Do request.
IFS=$OLDIFS
REQ_ID=$(curl ${CURL_OPTS} -L --capath /etc/grid-security/certificates -H "Accept: application/json" -H "Authorization: Bearer ${TOKEN_EOSPOWER1}" ${HTTPS_URI}/stage/ -d '{"files":[{"path":"/eos/ctaeos/preprod/test_http-rest-api"}]}' | jq -r .requestId)
echo "$(date +%s): Request id 1 - ${REQ_ID}"

#    3.1 Check progress of request.
echo "$(date +%s): Checking progress of request"
curl ${CURL_OPTS} -L --capath /etc/grid-security/certificates -H "Accept: application/json" -H "Authorization: Bearer ${TOKEN_EOSPOWER1}" "${HTTPS_URI}/stage/${REQ_ID}"

#    4. Cancel request.
echo "$(date +%s): Cancelling stage request"
curl ${CURL_OPTS} -L --capath /etc/grid-security/certificates -H "Accept: application/json" -H "Authorization: Bearer ${TOKEN_EOSPOWER1}" "${HTTPS_URI}/stage/${REQ_ID}/cancel/" -d '{"paths":["/eos/ctaeos/preprod/test_http-rest-api"]}'

#    5. Do another request.
IFS=$OLDIFS
REQ_ID=$(curl ${CURL_OPTS} -L --capath /etc/grid-security/certificates -H "Accept: application/json" -H "Authorization: Bearer ${TOKEN_EOSPOWER1}" ${HTTPS_URI}/stage/ -d '{"files":[{"path":"/eos/ctaeos/preprod/test_http-rest-api"}]}' | jq -r .requestId)
echo "$(date +%s): Request id 2 - ${REQ_ID}"

# 6. Delete the request.
curl ${CURL_OPTS} -L -X DELETE --capath /etc/grid-security/certificates -H "Accept: application/json" -H "Authorization: Bearer ${TOKEN_EOSPOWER1}" "${HTTPS_URI}/stage/${REQ_ID}"

#    5. Put drive up.
for drive in "${dr_names[@]}"; do
  echo "Putting drive $drive up."
  admin_cta dr up "$drive"
  sleep 3
done

#    6. Check evict counter reamins the same.
tmp=$(eos root://"${EOS_MGM_HOST}" ls -y /eos/ctaeos/preprod | grep test_http-rest-api | awk '{ print $1 }')
IFS='::'
split1=( $tmp  )
EVICT_FINAL=${split1[0]}
echo "Evict final counter: ${EVICT_FINAL}"

test "${EVICT_INIT}" == "${EVICT_FINAL}" &&
echo "$(date +%s): Request aborted successfully." || { echo "$(date +%s): File got staged despite abortion call."; exit 1; }
eos root://"${EOS_MGM_HOST}" ls -y /eos/ctaeos/preprod

# Remove the file manually.
eos root://"${EOS_MGM_HOST}" rm /eos/ctaeos/preprod/test_http-rest-api
IFS=$OLDIFS
echo "$(date +%s): Rest API test completed successfully. OK."
