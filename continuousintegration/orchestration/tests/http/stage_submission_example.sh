#!/bin/bash

EOSINSTANCE=ctaeos
PORT=9000

json='
{
  "files":[
    {
      "path":"/eos/ctaeos/tape/test.txt"
    }
  ] 
}
'

echo $json | sudo curl -L -v --capath /etc/grid-security/certificates --cert ~/.globus/usercert.pem --cacert ~/.globus/usercert.pem --key ~/.globus/userkey.pem -X POST https://$INSTANCE:$PORT/api/v0/stage -d @- 2> /dev/null | jq
