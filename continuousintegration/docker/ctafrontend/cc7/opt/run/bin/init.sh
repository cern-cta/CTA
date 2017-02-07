#!/bin/sh 

# enable cta repository from previously built artifacts
yum-config-manager --enable cta-artifacts
yum-config-manager --enable ceph

# install needed packages
yum -y install cta-objectstore-tools cta-doc mt-st mtx lsscsi sg3_utils cta-catalogueutils ceph-common
yum clean packages

echo "Using this configuration for library:"
/opt/run/bin/init_library.sh
cat /tmp/library-rc.sh
. /tmp/library-rc.sh

echo "Creating objectstore"
/opt/run/bin/init_objectstore.sh
. /tmp/objectstore-rc.sh

if [ "$OBJECTSTORETYPE" == "file" ]; then
  rm -fr $OBJECTSTOREURL
  mkdir -p $OBJECTSTOREURL
  cta-objectstore-initialize $OBJECTSTOREURL
  chmod -R 777 $OBJECTSTOREURL
else
  if [[ $(rados -p $OBJECTSTOREPOOL --id $OBJECTSTOREID --namespace $OBJECTSTORENAMESPACE ls | wc -l) -gt 0 ]]; then
    echo "Rados objectstore ${OBJECTSTOREURL} is not empty: deleting content"
    rados -p $OBJECTSTOREPOOL --id $OBJECTSTOREID --namespace $OBJECTSTORENAMESPACE ls | xargs -itoto rados -p $OBJECTSTOREPOOL --id $OBJECTSTOREID --namespace $OBJECTSTORENAMESPACE rm toto
  fi
  cta-objectstore-initialize $OBJECTSTOREURL
  echo "Rados objectstore ${OBJECTSTOREURL} content:"
  rados -p $OBJECTSTOREPOOL --id $OBJECTSTOREID --namespace $OBJECTSTORENAMESPACE ls
fi

echo "Creating DB"
/opt/run/bin/init_database.sh
. /tmp/database-rc.sh

if [ "$DATABASETYPE" == "sqlite" ]; then
  mkdir -p $(dirname $(echo ${DATABASEURL} | cut -d: -f2))
  echo ${DATABASEURL} >/etc/cta/cta_catalogue_db.conf
  cta-catalogue-schema-create /etc/cta/cta_catalogue_db.conf
  chmod -R 777 $(dirname $(echo ${DATABASEURL} | cut -d: -f2)) # needed?
else
  echo ${DATABASEURL} >/etc/cta/cta_catalogue_db.conf
  cta-catalogue-schema-create /etc/cta/cta_catalogue_db.conf
fi

if [ ! $LIBRARYTYPE == "mhvtl" ]; then
  echo "Real tapes, not labelling";
else
  # library management
  # BEWARE STORAGE SLOTS START @1 and DRIVE SLOTS START @0!!
  echo "Labelling tapes using the first drive in ${LIBRARYNAME}: ${DRIVENAMES[${driveslot}]} on /dev/${DRIVEDEVICES[${driveslot}]}:"
  for ((i=0; i<${#TAPES[@]}; i++)); do
    vid=${TAPES[${i}]}
    tapeslot=$((${i}+1)) # tape slot is 1 for tape[0] and so on...

    echo -n "${vid} in slot ${tapeslot} "
    mtx -f /dev/${LIBRARYDEVICE} load ${tapeslot} ${driveslot}
    cd /tmp
      echo "VOL1${vid}                           CASTOR                                    3">label.file
      dd if=label.file of=/dev/${DRIVEDEVICES[${driveslot}]} bs=80 count=1
      mt -f /dev/${DRIVEDEVICES[${driveslot}]} rewind
    mtx -f /dev/${LIBRARYDEVICE} unload ${tapeslot} ${driveslot}
    echo "OK"
  done
fi

echo "### INIT COMPLETED ###"
