#!/bin/bash

# This should be the service name; not the pod name
EOSINSTANCE=eos-mgm
PORT=9000

json='
{
  "paths":[
    "/eos/ctaeos/tape/test.txt"
  ]
}
'

echo $json | sudo curl -L -v --capath /etc/grid-security/certificates --cert ~/.globus/usercert.pem --cacert ~/.globus/usercert.pem --key ~/.globus/userkey.pem -X POST  https://$EOSINSTANCE:$PORT/api/v0/release/fake_id -d @- 2>/dev/null


