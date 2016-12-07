#!/bin/sh 

/shared/bin/init_pod.sh

# source library configuration file
echo "Using this configuration for library:"
cat ${LIBRARY_CONFIG}

. ${LIBRARY_CONFIG}

# to get rmcd logs to stdout
mkfifo /var/log/castor/rmcd_legacy.log
for ((;;)); do cat </var/log/castor/rmcd_legacy.log; done &
disown

ln -s /dev/${LIBRARYDEVICE} /dev/smc
/usr/bin/rmcd -f /dev/smc
