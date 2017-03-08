#!/bin/sh 

echo "Default run command: sleeping forever"
# sleep forever but exit immediately when pod is deleted
exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"
