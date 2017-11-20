#!/bin/bash 

. /opt/run/bin/init_pod.sh

if [ ! -e /etc/buildtreeRunner ]; then
yum-config-manager --enable cta-artifacts
yum-config-manager --enable ceph

# Install missing RPMs
# cta-catalogueutils is needed to delete the db at the end of instance
yum -y install cta-frontend cta-debuginfo cta-catalogueutils ceph-common
fi

# /etc/cta/cta-frontend-xrootd.conf is now provided by ctafrontend rpm and comes with CI-ready content

/opt/run/bin/init_objectstore.sh
. /tmp/objectstore-rc.sh

echo "ObjectStore BackendPath $OBJECTSTOREURL" > /etc/cta/cta-frontend.conf
echo "Catalogue NumberOfConnections 10" >>/etc/cta/cta-frontend.conf
echo "Log URL file:/var/log/cta/cta-frontend.log" >>/etc/cta/cta-frontend.conf

/opt/run/bin/init_database.sh
. /tmp/database-rc.sh

echo ${DATABASEURL} >/etc/cta/cta-catalogue.conf

# EOS INSTANCE NAME used as username for SSS key
EOSINSTANCE=ctaeos

# Wait for the keytab files to be pushed in by the creation script
echo -n "Waiting for /etc/cta/eos.sss.keytab."
for ((;;)); do test -e /etc/cta/eos.sss.keytab && break; sleep 1; echo -n .; done
echo OK
echo -n "Waiting for /etc/cta/cta-frontend.krb5.keytab."
for ((;;)); do test -e /etc/cta/cta-frontend.krb5.keytab && break; sleep 1; echo -n .; done
echo OK

echo "Generating core file in /var/log/cta directory so that those are available as artifacts"
echo '/var/log/cta/core_%e.%p' > /proc/sys/kernel/core_pattern

if [ "-${CI_CONTEXT}-" == '-nosystemd-' ]; then
  # systemd is not available
  runuser --shell='/bin/bash' --session-command='cd ~cta; xrootd -l /var/log/cta-frontend-xrootd.log -k fifo -n cta -c /etc/cta/cta-frontend-xrootd.conf -I v4' cta
  echo "ctafrontend died"
  echo "analysing core file if any"
  /opt/run/bin/ctafrontend_bt.sh
  sleep infinity
else
  # systemd is available
  echo "Launching frontend with systemd:"
  systemctl start cta-frontend

  echo "Status is now:"
  systemctl status cta-frontend
fi
