#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2024 CERN
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

usage() {
  echo "Usage: $0 <metadata>"
  echo ""
  echo "Sends a curl request to ctaeos with some archive metadata. "
  echo "Can be used in conjunction with grep_eosreport_for_archive_metadata on the ctaeos instance to test whether the metadata is present in the eosreport logs."
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
EOSINSTANCE=root://ctaeos

ENDPOINT=https://ctaeos:8444/${FILE_LOCATION}
METADATA="$1"

NOW=$(date +%s)
LATER=$(echo "${NOW}+86400" | bc)

# Generate Tokens
TOKEN_EOSUSER=$(eos "${EOSINSTANCE}" token --tree --path '/eos/ctaeos' --expires "${LATER}" --owner user1 --group eosusers --permission rwx)

echo "Printing eosuser token dump"
eos "${EOSINSTANCE}" token --token "${TOKEN_EOSUSER}" | jq .
echo

# Delete the file in case it exists
echo "Removing potentially existing file"
eos ${EOSINSTANCE} rm ${FILE_LOCATION} 2>/dev/null

# Do a curl request
TMP_FILE=$(mktemp)

echo "Making curl request"
curl -L --insecure -H "Accept: application/json" -H "ArchiveMetadata: ${METADATA}" -H "Authorization: Bearer ${TOKEN_EOSUSER}" ${ENDPOINT} --upload-file "${TMP_FILE}"