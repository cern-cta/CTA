#!/bin/bash 

set -e

. /opt/run/bin/init_pod.sh

if [ ! -e /etc/buildtreeRunner ]; then
  yum-config-manager --enable cta-artifacts
  yum-config-manager --enable ceph
  yum-config-manager --enable castor

  # Install missing RPMs
  yum -y install mt-st lsscsi sg3_utils cta-taped cta-tape-label cta-debuginfo ceph-common
fi

echo "Configuring database"
/opt/run/bin/init_database.sh
. /tmp/database-rc.sh

echo ${DATABASEURL} > /etc/cta/cta-catalogue.conf

yum -y install cta-catalogueutils cta-systemtests

/usr/bin/cta-rdbmsUnitTests /etc/cta/cta-catalogue.conf

echo 'yes' | /usr/bin/cta-catalogue-schema-drop /etc/cta/cta-catalogue.conf
/usr/bin/cta-catalogue-schema-create /etc/cta/cta-catalogue.conf
