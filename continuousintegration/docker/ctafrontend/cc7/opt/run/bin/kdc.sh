#!/bin/bash

/opt/run/bin/init_pod.sh

# Install missing RPMs (kdc)
yum -y install heimdal-server heimdal-workstation

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
