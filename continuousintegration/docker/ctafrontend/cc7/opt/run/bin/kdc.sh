#!/bin/bash

/opt/run/bin/init_pod.sh

# Install missing RPMs (kdc)
yum -y install heimdal-server heimdal-workstation

# Init the kdc store
echo -n "Initing kdc... "
/usr/lib/heimdal/bin/kadmin -l -r TEST.CTA init --realm-max-ticket-life=unlimited --realm-max-renewable-life=unlimited TEST.CTA || (echo Failed. ; exit 1)
echo Done.

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
echo -n "Populating kdc... "
/usr/lib/heimdal/bin/kadmin -l -r TEST.CTA add --random-password --use-defaults admin1 admin2 user1 user2 cta/cta-frontend eos/eos-server

/usr/lib/heimdal/bin/kadmin -l -r TEST.CTA ext_keytab --keytab=/root/admin1.keytab admin1
/usr/lib/heimdal/bin/kadmin -l -r TEST.CTA ext_keytab --keytab=/root/admin2.keytab admin2
/usr/lib/heimdal/bin/kadmin -l -r TEST.CTA ext_keytab --keytab=/root/user1.keytab user1
/usr/lib/heimdal/bin/kadmin -l -r TEST.CTA ext_keytab --keytab=/root/user2.keytab user2
/usr/lib/heimdal/bin/kadmin -l -r TEST.CTA ext_keytab --keytab=/root/cta-frontend.keytab cta/cta-frontend
/usr/lib/heimdal/bin/kadmin -l -r TEST.CTA ext_keytab --keytab=/root/eos.keytab eos/eos-server
echo Done.

echo "### KDC ready ###"
touch /root/kdcReady

# sleep forever but exit immediately when pod is deleted
exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"
