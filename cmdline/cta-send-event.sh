#!/bin/sh
#
# Send CLOSEW or PREPARE events to the CTA Frontend to retry failed archive or retrieval requests

CTA_SEND_EVENT=/usr/bin/cta-send-event
EOS_HOST=localhost:1094

error()
{
  echo "$*" >&2
  exit 1
}

usage()
{
  echo "Usage: $(basename $0) CLOSEW|PREPARE <eos_instance> /eos/path [/eos/path...]" >&2
  exit 2
}

[ $# -gt 2 ] || usage
EVENT=$1
shift
[ "${EVENT}" == "CLOSEW" -o "${EVENT}" == "PREPARE" ] || usage
INSTANCE=$1
shift
[ -x ${CTA_SEND_EVENT} ] || error "Cannot execute ${CTA_SEND_EVENT}"

for FILENAME in $*
do
  FILEMD=$(eos --json fileinfo $1)
  [ "$(echo "$FILEMD" | jq .retc)" == "null" ] || error "$(basename $0): Cannot open $1 for reading"
  echo ${FILEMD} | jq ".instance = \"${INSTANCE}\"" | ${CTA_SEND_EVENT} ${EOS_HOST} ${EVENT}
done
