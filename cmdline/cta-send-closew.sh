#!/bin/sh
#
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
