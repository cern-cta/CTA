#!/bin/sh

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2022 CERN
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

# Send CLOSEW events to the CTA Frontend to retry failed archive requests

CTA_SEND_EVENT=/usr/bin/cta-send-event
CTA_EVENT=CLOSEW

error()
{
  echo "$*" >&2
  exit 1
}

usage()
{
  echo "Usage: $(basename $0) /eos/path [/eos/path...]" >&2
  exit 2
}

[ -x ${CTA_SEND_EVENT} ] || error "Cannot execute ${CTA_SEND_EVENT}"
[ $# -gt 0 ] || usage

for FILENAME in $*
do
  FILEMD=$(eos --json fileinfo ${FILENAME})
  [ "$(echo "$FILEMD" | jq .retc)" == "null" ] || error "$(basename $0): Cannot open ${FILENAME} for reading"
  echo ${FILEMD} | jq '' | ${CTA_SEND_EVENT} ${CTA_EVENT}
done
