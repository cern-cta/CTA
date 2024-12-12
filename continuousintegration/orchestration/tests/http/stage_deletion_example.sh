#!/bin/bash

# Usage: ./stage_deletion.sh stage_request_id
# This should be the service name; not the pod name
EOSINSTANCE=eos-mgm
PORT=9000

sudo curl -L -v --capath /etc/grid-security/certificates --cert ~/.globus/usercert.pem --cacert ~/.globus/usercert.pem --key ~/.globus/userkey.pem -X DELETE https://$EOSINSTANCE:$PORT/api/v0/stage/$1 2> /dev/null
