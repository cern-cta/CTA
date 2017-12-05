#!/bin/bash 

. /opt/run/bin/init_pod.sh

if [ ! -e /etc/buildtreeRunner ]; then
  yum-config-manager --enable cta-artifacts
  yum-config-manager --enable ceph
  yum-config-manager --enable castor

  # Install missing RPMs
  yum -y install mt-st mtx lsscsi sg3_utils cta-rmcd
fi

# source library configuration file
echo "Using this configuration for library:"
/opt/run/bin/init_library.sh
cat /tmp/library-rc.sh
. /tmp/library-rc.sh

# to get rmcd logs to stdout
tail -F /var/log/cta/cta-rmcd.log &

ln -s /dev/${LIBRARYDEVICE} /dev/smc
/usr/bin/cta-rmcd -f /dev/smc
