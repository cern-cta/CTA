#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


usage() {
  echo "Usage: $0 <metadata>"
  echo ""
  echo "Sends a curl request to EOS mgm with some archive metadata. "
  echo "Can be used in conjunction with grep_eosreport_for_archive_metadata on the EOS MGM instance to test whether the metadata is present in the eosreport logs."
  exit 1
}

if [ -z "$1" ]; then
  usage
fi

. /root/client_helper.sh

admin_kdestroy &>/dev/null
admin_kinit &>/dev/null

eospower_kdestroy &>/dev/null
eospower_kinit &>/dev/null

FILE_LOCATION=eos/ctaeos/preprod/test_archive-metadata
EOS_MGM_HOST="ctaeos"

METADATA="$1"

NOW=$(date +%s)
LATER=$(echo "${NOW}+86400" | bc)

# Generate Tokens
TOKEN_EOSUSER1=$(eosadmin_eos root://"${EOS_MGM_HOST}" token --tree --path '/eos/ctaeos/://:/api/' --expires "${LATER}" --owner user1 --group eosusers --permission rwx)

echo "Printing eosuser token dump"
eos root://"${EOS_MGM_HOST}" token --token "${TOKEN_EOSUSER1}" | jq .
echo

# Delete the file in case it exists
echo "Removing potentially existing file"
eos root://${EOS_MGM_HOST} rm ${FILE_LOCATION} 2>/dev/null

# Do a curl request
TMP_FILE=$(mktemp)
echo "Dummy" > "${TMP_FILE}"

echo "Making curl request"
curl -X PUT -L --insecure -H "Accept: application/json" -H "ArchiveMetadata: ${METADATA}" -H "Authorization: Bearer ${TOKEN_EOSUSER1}" "https://${EOS_MGM_HOST}:8443/${FILE_LOCATION}" --upload-file "${TMP_FILE}"
