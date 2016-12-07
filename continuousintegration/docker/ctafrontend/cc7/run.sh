#!/bin/sh 

echo "ObjectStore BackendPath $objectstore" > /etc/cta/cta-frontend.conf
echo "Catalogue NumberOfConnections 1" >>/etc/cta/cta-frontend.conf

echo ${catdb} >/etc/cta/cta_catalogue_db.conf

useradd cta
# disable kerberos5 check for the admin privileges
  sed -i -e "s/krb5/unix/" /usr/lib64/libXrdCtaOfs.so
runuser --shell='/bin/bash' --session-command='cd ~cta; xrootd -n cta -c /etc/xrootd/xrootd-cta.cfg -I v4' cta&
/bin/bash
