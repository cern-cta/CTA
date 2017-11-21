#!/bin/bash 

. /opt/run/bin/init_pod.sh

if [ ! -e /etc/buildtreeRunner ]; then
  yum-config-manager --enable cta-artifacts
  yum-config-manager --enable ceph
  yum-config-manager --enable castor

  # Install missing RPMs
  yum -y install mt-st lsscsi sg3_utils cta-taped cta-debuginfo ceph-common
fi

echo "Using this configuration for library:"
/opt/run/bin/init_library.sh
cat /tmp/library-rc.sh
. /tmp/library-rc.sh

ln -s /dev/${LIBRARYDEVICE} /dev/smc

tpconfig="${DRIVENAMES[${driveslot}]} ${LIBRARYNAME} /dev/${DRIVEDEVICES[${driveslot}]} smc${driveslot}"

/opt/run/bin/init_objectstore.sh
. /tmp/objectstore-rc.sh

echo "Configuring database"
/opt/run/bin/init_database.sh
. /tmp/database-rc.sh

echo ${DATABASEURL} > /etc/cta/cta-catalogue.conf

# cta-taped setup
  echo "taped BufferCount 10" > /etc/cta/cta-taped.conf
  echo "taped MountCriteria 2000000, 5" >> /etc/cta/cta-taped.conf 
  echo "ObjectStore BackendPath $OBJECTSTOREURL" >> /etc/cta/cta-taped.conf
  echo "${tpconfig}" > /etc/cta/TPCONFIG

####
# configuring taped using the official location for SSS: /etc/cta/cta-taped.sss.keytab
CTATAPEDSSS="cta-taped.sss.keytab"

# key generated with 'echo y | xrdsssadmin -k taped+ -u stage -g tape  add /tmp/taped.keytab'
#echo '0 u:stage g:tape n:taped+ N:6361736405290319874 c:1481207182 e:0 f:0 k:8e2335f24cf8c7d043b65b3b47758860cbad6691f5775ebd211b5807e1a6ec84' >> /etc/cta/${CTATAPEDSSS}
echo -n '0 u:daemon g:daemon n:ctaeos+ N:6361884315374059521 c:1481241620 e:0 f:0 k:1a08f769e9c8e0c4c5a7e673247c8561cd23a0e7d8eee75e4a543f2d2dd3fd22' > /etc/cta/${CTATAPEDSSS}
chmod 600 /etc/cta/${CTATAPEDSSS}
chown cta /etc/cta/${CTATAPEDSSS}

cat <<EOF > /etc/sysconfig/cta-taped
CTA_TAPED_OPTIONS="--log-to-file=/var/log/cta/cta-taped.log"
XrdSecPROTOCOL=sss
XrdSecSSSKT=/etc/cta/${CTATAPEDSSS}
EOF


if [ "-${CI_CONTEXT}-" == '-systemd-' ]; then
  # systemd is available
  echo "Launching rmcd with systemd:"
  systemctl start cta-taped

  echo "Status is now:"
  systemctl status cta-taped

else
  # systemd is not available

. /etc/sysconfig/cta-taped

tail -F /var/log/cta/cta-taped.log &

# cta-taped is ran with runuser to avoid a bug with Docker that prevents both
# the setresgid(-1, 1474, -1) and setresuid(-1, 14029, -1) system calls from
# working correctly
runuser -c "/usr/bin/cta-taped --foreground ${CTA_TAPED_OPTIONS}"

echo "taped died"

sleep infinity

fi
