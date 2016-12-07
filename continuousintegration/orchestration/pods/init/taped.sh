#!/bin/sh 

/shared/bin/init_pod.sh

# source library configuration file
echo "Using this configuration for library:"
cat ${LIBRARY_CONFIG}

. ${LIBRARY_CONFIG}


ln -s /dev/${LIBRARYDEVICE} /dev/smc
#/usr/bin/rmcd -f /dev/smc&

mkdir -p /etc/castor

tpconfig="${DRIVENAMES[${driveslot}]} ${LIBRARYNAME} /dev/${DRIVEDEVICES[${driveslot}]} smc0"

# cta-tapserverd setup 
# to be drop later
  echo "${tpconfig}" > /etc/castor/TPCONFIG
  echo "TapeServer ObjectStoreBackendPath $objectstore" >/etc/castor/castor.conf
  echo "TapeServer BufSize 5242880" >>/etc/castor/castor.conf
  echo "TapeServer NbBufs 10" >>/etc/castor/castor.conf
  echo "TapeServer EOSRemoteHostAndPort ${eoshost}" >>/etc/castor/castor.conf

# cta-taped setup
  echo "taped BufferCount 10" > /etc/cta/cta.conf
  echo "general ObjectStoreURL ${objectstore}" >> /etc/cta/cta.conf
  echo "${tpconfig}" > /etc/cta/TPCONFIG

echo ${catdb} >/etc/cta/cta_catalogue_db.conf

/bin/cta-taped --stdout --foreground
