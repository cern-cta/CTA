#!/bin/bash
EOSINSTANCE=ctaeos
PORT=9000
EXPIRE=`date +%s`; let LATER=$EXPIRE+64000
token=`eos token --path /eos/ctaeos/tape/ --expires $LATER --tree --owner owner_username --group owner_group_name`
curl -k -L -v  https://$EOSINSTANCE:$PORT/api/v0/archiveinfo -X POST -H "Authorization: Bearer $token" -H "Content-Type: application/json" -d '{"paths":["/eos/ctaeos/tape/test.txt","/file/does/not/exist"]}' 2> /dev/null | jq
[
  {
    "locality": "TAPE",
    "path": "/eos/ctaeos/tape/test.txt"
  },
  {
    "error": "USER ERROR: file does not exist or is not accessible to you",
    "path": "/file/does/not/exist"
  }
]


token=`eos token --path /eos/ctaeos/ --expires $LATER`
curl -k -L -v  https://$EOSINSTANCE:$PORT/api/v0/archiveinfo -X POST -H "Authorization: Bearer $token" -H "Content-Type: application/json" -d '{"paths":["/eos/ctaeos/tape/test.txt","/file/does/not/exist"]}' 2> /dev/null | jq
[
  {
    "error": "USER ERROR: you don't have prepare permission",
    "locality": "TAPE",
    "path": "/eos/ctaeos/tape/test.txt"
  },
  {
    "error": "USER ERROR: file does not exist or is not accessible to you",
    "path": "/file/does/not/exist"
  }
]
