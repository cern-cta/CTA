#!/bin/bash
# This should be the service name; not the pod name
EOSINSTANCE=eos-mgm
PORT=9000
DESTDIR=/eos/ctaeos/tape/
SOURCEFILE=file.txt
curl -k -L -v --capath /etc/grid-security/certificates --cert ~/.globus/usercert.pem --cacert ~/.globus/usercert.pem --key ~/.globus/userkey.pem  https://$EOSINSTANCE:$PORT$DESTDIR --upload-file $SOURCEFILE
