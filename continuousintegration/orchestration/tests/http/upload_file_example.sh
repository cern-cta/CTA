#!/bin/bash
EOS_MGM_HOST="ctaeos"
PORT=9000
DESTDIR=/eos/ctaeos/tape/
SOURCEFILE=file.txt
curl -k -L -v --capath /etc/grid-security/certificates --cert ~/.globus/usercert.pem --cacert ~/.globus/usercert.pem --key ~/.globus/userkey.pem  https://$EOS_MGM_HOST:$PORT$DESTDIR --upload-file $SOURCEFILE
