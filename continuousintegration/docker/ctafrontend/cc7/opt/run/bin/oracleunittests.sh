#!/bin/bash 

# The CERN Tape Archive (CTA) project
# Copyright (C) 2015  CERN
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

set -e

. /opt/run/bin/init_pod.sh

if [ ! -e /etc/buildtreeRunner ]; then
  yum-config-manager --enable cta-artifacts
  yum-config-manager --enable ceph
  yum-config-manager --enable castor

  # Install missing RPMs
  yum -y install mt-st lsscsi sg3_utils cta-taped cta-tape-label cta-debuginfo ceph-common oracle-instantclient-tnsnames.ora
fi

echo "Configuring database"
/opt/run/bin/init_database.sh
. /tmp/database-rc.sh

echo ${DATABASEURL} > /etc/cta/cta-catalogue.conf

yum -y install cta-catalogueutils cta-systemtests

2>&1 /usr/bin/cta-rdbmsUnitTests /etc/cta/cta-catalogue.conf | awk '{print "oracle " $0;}'

# Disabled/commented out the valgrind tests of Oracle until they work better with CI
#2>&1 /usr/bin/cta-rdbmsUnitTests-oracle.sh /etc/cta/cta-catalogue.conf | awk '{print "valgrind oracle " $0;}'

echo 'yes' | /usr/bin/cta-catalogue-schema-drop /etc/cta/cta-catalogue.conf
/usr/bin/cta-catalogue-schema-create /etc/cta/cta-catalogue.conf
