#!/bin/sh

EOS_MGM_LOG=/var/log/eos/mgm/xrdlog.mgm

if test -f ${EOS_MGM_LOG}; then
  echo "${EOS_MGM_LOG} exists"
else
  echo "${EOS_MGM_LOG} does not exist or is not a regular file"
  exit 1
fi

echo "Grepping ${EOS_MGM_LOG} for ERROR messages"
if grep -q ERROR ${EOS_MGM_LOG}; then
  echo "Found ERROR messages in ${EOS_MGM_LOG}"
  exit 1
else
  echo "No ERROR messages found in ${EOS_MGM_LOG}"
  exit 0
fi
