#!/bin/sh 

# copy library configuration from persistent volume to shared volume
mkdir -p /shared/${INSTANCE_NAME}
cp -f /library/config ${LIBRARY_CONFIG}

echo "Using this configuration for library:"
cat ${LIBRARY_CONFIG}
. ${LIBRARY_CONFIG}

rm -rf ${objectstore}
mkdir -p ${objectstore}
  cta-objectstore-initialize ${objectstore}
  chmod -R 777 ${objectstore}

rm -rf ${catdbdir}
mkdir -p ${catdbdir}
  sqlite3 ${catdbdir}/${catdbfile} <  `rpm -ql cta-doc|grep sqlite`
  chmod -R 777 ${catdbdir}


# library management
# BEWARE STORAGE SLOTS START @1 and DRIVE SLOTS START @0!!
echo "Labelling tapes using the first drive in ${LIBRARYNAME}: ${DRIVENAMES[0]} on /dev/${DRIVEDEVICES[0]}:"
for ((i=0; i<${#TAPES[@]}; i++)); do
  vid=${TAPES[${i}]}
  tapeslot=$((${i}+1)) # tape slot is 1 for tape[0] and so on...

  echo -n "${vid} in slot ${tapeslot} "
  mtx -f /dev/${LIBRARYDEVICE} load ${tapeslot} 0
  cd /tmp
    echo "VOL1${vid}                           CASTOR                                    3">label.file
    dd if=label.file of=/dev/${DRIVEDEVICES[0]} bs=80 count=1
    mt -f /dev/${DRIVEDEVICES[0]} rewind
  mtx -f /dev/${LIBRARYDEVICE} unload ${tapeslot} 0
  echo "OK"
done

exit 0
