#!/bin/bash 

. /opt/run/bin/init_pod.sh

if [ ! -e /etc/buildtreeRunner ]; then
yum-config-manager --enable cta-artifacts
yum-config-manager --enable ceph

# Install missing RPMs
# cta-catalogueutils is needed to delete the db at the end of instance
yum -y install cta-frontend cta-debuginfo cta-catalogueutils ceph-common
fi

# just to get the ctafrontend xrootd configuration file in /etc/cta/cta-frontend-xrootd.conf
# can be removed when taking config file from rpm
yes | cp -r /opt/ci/ctafrontend/etc / 

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

# Create SSS key for ctafrontend, must be forwardable in kubernetes realm
echo y | xrdsssadmin -k ctafrontend+ -u ${EOSINSTANCE} -g cta add /etc/ctafrontend_SSS_s.keytab
# copy it in the client file that contains only one SSS
cp /etc/ctafrontend_SSS_s.keytab /etc/ctafrontend_SSS_c.keytab
chmod 600 /etc/ctafrontend_SSS_s.keytab /etc/ctafrontend_SSS_c.keytab
chown cta /etc/ctafrontend_SSS_s.keytab /etc/ctafrontend_SSS_c.keytab

# Wait for the keytab file to be pushed in by the creation script.
echo -n "Waiting for /etc/cta/cta-frontend.krb5.keytab"
for ((;;)); do test -e /etc/cta/cta-frontend.krb5.keytab && break; sleep 1; echo -n .; done
echo OK

echo "Generating core file in /var/log/cta directory so that those are available as artifacts"
echo '/var/log/cta/core_%e.%p' > /proc/sys/kernel/core_pattern

echo "Launching frontend"
runuser --shell='/bin/bash' --session-command='cd ~cta; xrootd -l /var/log/cta-frontend-xrootd.log -k fifo -n cta -c /etc/cta/cta-frontend-xrootd.conf -I v4' cta

echo "ctafrontend died"
echo "analysing core file if any"
/opt/run/bin/ctafrontend_bt.sh
sleep infinity
