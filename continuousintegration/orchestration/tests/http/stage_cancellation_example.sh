#!/bin/bash

# Usage: ./stage_cancellation.sh stage_request_id

EOS_MGM_HOST="eos-mgm"
PORT=9000

json='
{
  "paths":[
    "/eos/ctaeos/tape/test.txt"
  ]
}'

echo $json | sudo curl -L --capath /etc/grid-security/certificates --cert ~/.globus/usercert.pem --cacert ~/.globus/usercert.pem --key ~/.globus/userkey.pem -H 'Content-Type: application/json' -X POST https://$EOS_MGM_HOST:$PORT/api/v0/stage/$1/cancel --data @-
