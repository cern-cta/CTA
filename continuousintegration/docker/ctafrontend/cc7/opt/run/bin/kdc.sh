#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2022 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.

. /opt/run/bin/init_pod.sh

# Install missing RPMs (kdc)
if [ ! -e /etc/buildtreeRunner ]; then
  yum -y install heimdal-server heimdal-workstation
fi

# Init the kdc store
echo -n "Initing kdc... "
/usr/lib/heimdal/bin/kadmin -l -r TEST.CTA init --realm-max-ticket-life=unlimited --realm-max-renewable-life=unlimited TEST.CTA || (echo Failed. ; exit 1)
echo Done.

KEYTABS="user1 user2 poweruser1 poweruser2 ctaadmin1 ctaadmin2 eosadmin1 eosadmin2 cta/cta-frontend eos/eos-server"

# Start kdc
echo -n "Starting kdc... "
/usr/libexec/kdc &
echo Done.

echo -n "Generating krb5.conf... "
cat > /etc/krb5.conf << EOF_krb5
[libdefaults]
 default_realm = TEST.CTA

[realms]
  TEST.CTA = {
   kdc=kdc
  }
EOF_krb5
echo Done.

# Populate KDC and generate keytab files
echo "Populating kdc... "
/usr/lib/heimdal/bin/kadmin -l -r TEST.CTA add --random-password --use-defaults ${KEYTABS}

for NAME in ${KEYTABS}; do
   echo -n "  Generating /root/$(basename ${NAME}).keytab for ${NAME}"
  /usr/lib/heimdal/bin/kadmin -l -r TEST.CTA ext_keytab --keytab=/root/$(basename ${NAME}).keytab ${NAME} && echo OK || echo FAILED
done

echo Done.

echo "### KDC ready ###"
touch /root/kdcReady

# sleep forever but exit immediately when pod is deleted
exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"
