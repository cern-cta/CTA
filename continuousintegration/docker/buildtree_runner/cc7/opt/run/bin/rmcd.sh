#!/bin/sh 

/opt/run/bin/init_pod.sh

# source library configuration file
echo "Using this configuration for library:"
/opt/run/bin/init_library.sh
cat /tmp/library-rc.sh
. /tmp/library-rc.sh

# to get rmcd logs to stdout
mkfifo /var/log/castor/rmcd_legacy.log
for ((;;)); do cat </var/log/castor/rmcd_legacy.log; done &
disown

ln -s /dev/${LIBRARYDEVICE} /dev/smc
/usr/bin/rmcd -f /dev/smc
