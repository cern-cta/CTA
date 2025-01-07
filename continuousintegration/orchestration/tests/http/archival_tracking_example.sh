#!/bin/bash

EOS_MGM_HOST="ctaeos"
PORT=9000

json='
{
  "paths":[
    "/eos/ctaeos/tape/test.txt"
  ]
}
'

echo $json | curl -L -v --capath /etc/grid-security/certificates --cert ~/.globus/usercert.pem --cacert ~/.globus/usercert.pem --key ~/.globus/userkey.pem  https://$EOS_MGM_HOST:$PORT/api/v0/archiveinfo -X POST -H "Content-Type: application/json" -d @- 2> /dev/null | jq
