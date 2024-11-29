#!/bin/bash

# Usage: ./stage_deletion.sh stage_request_id
EOS_MGM_HOST="eos-mgm"
PORT=9000

sudo curl -L -v --capath /etc/grid-security/certificates --cert ~/.globus/usercert.pem --cacert ~/.globus/usercert.pem --key ~/.globus/userkey.pem -X DELETE https://$EOS_MGM_HOST:$PORT/api/v0/stage/$1 2> /dev/null
