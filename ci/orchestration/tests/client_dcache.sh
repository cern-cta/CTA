#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

dnf install -y openssh-clients

admin_username="admin"
admin_pwd="dickerelch"
dcache_endpoint="store-door-svc"

test_file="/etc/group"
remote_path="data/cta${test_file}"
WAIT_FOR_ARCHIVE_TIMEOUT=20
SECONDS_PASSED=0

# Write a file to dCache
echo "Uploading file to dCache..."
curl -k -u "${admin_username}:${admin_pwd}" --upload-file "${test_file}" "https://${dcache_endpoint}:8083/${remote_path}"

# Wait for file to be archived to tape
echo "Checking if file is archived to tape..."
while true; do
  file_on_tape=$(curl -k -u "${admin_username}:${admin_pwd}" -s "http://${dcache_endpoint}:3880/api/v1/namespace/${remote_path}?locality=true" | jq -r .fileLocality)

  if [[ "$file_on_tape" == "ONLINE_AND_NEARLINE" ]]; then
    echo "File is now archived to tape"
    break
  fi

  echo "$(date +%s): Waiting for file to be archived to tape: Seconds passed = ${SECONDS_PASSED}"
  sleep 1
  ((SECONDS_PASSED++))

  if [[ $SECONDS_PASSED -ge $WAIT_FOR_ARCHIVE_TIMEOUT ]]; then
    echo "Timed out after ${WAIT_FOR_ARCHIVE_TIMEOUT} seconds waiting for file to be archived to tape"
    break
  fi
done

# Delete file from dCache
echo "Deleting file from dCache..."
curl -k -u "${admin_username}:${admin_pwd}" -X DELETE "https://${dcache_endpoint}:8083/${remote_path}"

# Confirm file deletion
echo "Confirming file deletion..."
while true; do
  resp=$(curl -k -u "${admin_username}:${admin_pwd}" -s "http://${dcache_endpoint}:3880/api/v1/namespace/${remote_path}?locality=true")
  status=$(echo "$resp" | jq -r '.status')
  title=$(echo "$resp" | jq -r '.title')

  if [[ "$title" == "Not Found" && "$status" == "404" ]]; then
    echo "File successfully deleted from tape"
    break
  else
    echo "File not deleted from tape yet"
    echo "$resp"
    sleep 1
  fi
done

echo "dCache regression tests completed"
