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
  echo "Looks for specific archive metadata in the eosreport logs."
  exit 1
}

if [ -z "$1" ]; then
  usage
fi

echo "Looking for archive metadata activity..."

REPORT_FILE_NAME="/var/eos/report/$(date +%Y)/$(date +%m)/$(date +%Y)$(date +%m)$(date +%d).eosreport"
ARCHIVE_METADATA="$1"

# Check that archivemetadata is present in the eos report logs
ARCHIVE_METADATA_LOGLINE=$(grep "archivemetadata=" "${REPORT_FILE_NAME}")
echo "  Archive metadata eosreport log line:"
echo "  ${ARCHIVE_METADATA_LOGLINE}"

if grep -q "${ARCHIVE_METADATA}" <<<"${ARCHIVE_METADATA_LOGLINE}"; then
  echo "  Archive metadata successfully ended up in eos report logs."
else
  echo "  ERROR: Archive metadata log line not found."
  exit 1
fi
