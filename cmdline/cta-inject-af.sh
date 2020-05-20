#!/bin/sh
#
# Send a CLOSEW event to the CTA Frontend in order to retry a failed archive request

CTA_SEND_EVENT=/usr/bin/cta-send-event
EOS_HOST=localhost:1094

error()
{
  echo "$*" >&2
  exit 1
}

get_eos_md()
{
  eos --json fileinfo $1
  [ $? -eq 0 ] || error "$(basename $0): Cannot open $1 for reading"
}

[ $# -gt 0 ] || error "Usage: $(basename $0) /eos/path [/eos/path...]"
[ -x ${CTA_SEND_EVENT} ] || error "Cannot execute ${CTA_SEND_EVENT}"

for FILENAME in $*
do
  get_eos_md $FILENAME | ${CTA_SEND_EVENT} ${EOS_HOST} CLOSEW
done
