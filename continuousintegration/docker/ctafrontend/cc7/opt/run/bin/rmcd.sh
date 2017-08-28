#!/bin/bash 

. /opt/run/bin/init_pod.sh

yum-config-manager --enable cta-artifacts
yum-config-manager --enable ceph
yum-config-manager --enable castor

# Install missing RPMs
yum -y install mt-st mtx lsscsi sg3_utils cta-taped cta-debuginfo castor-rmc-server

# source library configuration file
echo "Using this configuration for library:"
/opt/run/bin/init_library.sh
cat /tmp/library-rc.sh
. /tmp/library-rc.sh

# to get rmcd logs to stdout
tail -F /var/log/castor/rmcd_legacy.log &

ln -s /dev/${LIBRARYDEVICE} /dev/smc
/usr/bin/rmcd -f /dev/smc
