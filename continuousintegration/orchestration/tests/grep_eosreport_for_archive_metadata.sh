#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
#
# SPDX-License-Identifier: GPL-3.0-or-later


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
ARCHIVE_METADATA_LOGLINES=$(grep "archivemetadata=" "${REPORT_FILE_NAME}")
echo "  Archive metadata eosreport log lines:"
echo "  ${ARCHIVE_METADATA_LOGLINES}"

if grep -q "${ARCHIVE_METADATA}" <<<"${ARCHIVE_METADATA_LOGLINES}"; then
  echo "  Archive metadata successfully ended up in eos report logs."
else
  echo "  ERROR: Archive metadata log line not found."
  exit 1
fi
