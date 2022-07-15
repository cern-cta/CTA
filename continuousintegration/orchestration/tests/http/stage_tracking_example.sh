#!/bin/bash
# Usage: ./stage_tracking_example.sh stage_request_id

EOSINSTANCE=ctaeos
PORT=9000

sudo curl -L -v --capath /etc/grid-security/certificates --cert ~/.globus/usercert.pem --cacert ~/.globus/usercert.pem --key ~/.globus/userkey.pem -X GET https://$EOSINSTANCE:$PORT/api/v0/stage/$1 2> /dev/null | jq
