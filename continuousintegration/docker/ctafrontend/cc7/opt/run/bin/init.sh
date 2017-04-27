#!/bin/sh 

/opt/run/bin/init_pod.sh

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

echo "Configuring objectstore:"
/opt/run/bin/init_objectstore.sh
. /tmp/objectstore-rc.sh

if [ "$KEEP_OBJECTSTORE" == "0" ]; then
  echo "Wiping objectstore"
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
else
  echo "Reusing objectstore (no check)"
fi


echo "Configuring database:"
/opt/run/bin/init_database.sh
. /tmp/database-rc.sh
echo ${DATABASEURL} >/etc/cta/cta_catalogue_db.conf

if [ "$KEEP_DATABASE" == "0" ]; then
  echo "Wiping database"
  cta-catalogue-schema-unlock /etc/cta/cta_catalogue_db.conf
  cta-catalogue-schema-status /etc/cta/cta_catalogue_db.conf
  echo yes | cta-catalogue-schema-drop /etc/cta/cta_catalogue_db.conf

  if [ "$DATABASETYPE" == "sqlite" ]; then
    mkdir -p $(dirname $(echo ${DATABASEURL} | cut -d: -f2))
    cta-catalogue-schema-create /etc/cta/cta_catalogue_db.conf
    chmod -R 777 $(dirname $(echo ${DATABASEURL} | cut -d: -f2)) # needed?
  else
    cta-catalogue-schema-create /etc/cta/cta_catalogue_db.conf
  fi
else
  echo "Reusing database (no check)"
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
      mt -f /dev/${DRIVEDEVICES[${driveslot}]} rewind
      dd if=label.file of=/dev/${DRIVEDEVICES[${driveslot}]} bs=80 count=1
      mt -f /dev/${DRIVEDEVICES[${driveslot}]} rewind
    mtx -f /dev/${LIBRARYDEVICE} unload ${tapeslot} ${driveslot}
    echo "OK"
  done
fi

echo "### INIT COMPLETED ###"
