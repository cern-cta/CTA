#!/bin/bash
echo "## Attempting to stop mhvtl"
service mhvtl stop

echo "## Remonving old mhvtl configuration"
rm -rf /etc/mhvtl
rm -rf /opt/mhvtl

echo "## Re-creting /opt/mhvtl"
mkdir /opt/mhvtl
chown vtl.vtl /opt/mhvtl

echo "## Populating mhvtl configuration"
mkdir /etc/mhvtl
cp /root/etc-mhvtl/{mhvtl.conf,library_contents.10,library_contents.30,device.conf} /etc/mhvtl

echo "## Starting mhvtl."
service mhvtl start

echo "## Waiting 5 seconds as the generic scsi devices do not appear instantly"
sleep 5

echo "## Mounting tapes"
# find the list of media changers. We expect 4 tape drives per library
echo -n "### Detecting media changers..."
SCSI_DEVS=`ls -d /sys/bus/scsi/devices/*`
# Media changer scsi type is "8"
MC_DEVS=`for i in ${SCSI_DEVS}; do if [ -e $i/type ] && grep -q 8 $i/type; then ls $i/scsi_generic; fi; done`
echo ${MC_DEVS}

for i in $MC_DEVS; do
  mtx -f /dev/$i load 10 0
  mtx -f /dev/$i load 1 1
  mtx -f /dev/$i load 2 2
  mtx -f /dev/$i load 3 3
  mtx -f /dev/$i status | grep Transfer
done

echo "## Starting system test program"
/usr/local/bin/TapeDriveInfo

