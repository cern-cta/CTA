#!/bin/bash

# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2003-2021 CERN
# @license        This program is free software: you can redistribute it and/or modify
#                 it under the terms of the GNU General Public License as published by
#                 the Free Software Foundation, either version 3 of the License, or
#                 (at your option) any later version.
#
#                 This program is distributed in the hope that it will be useful,
#                 but WITHOUT ANY WARRANTY; without even the implied warranty of
#                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#                 GNU General Public License for more details.
#
#                 You should have received a copy of the GNU General Public License
#                 along with this program.  If not, see <http://www.gnu.org/licenses/>.

echo "## Attempting to stop mhvtl"
service mhvtl stop

echo "###  Waiting 5 seconds to let the dust settle"
sleep 5

echo "### Stopping leftover processes"
killall vtltape
killall vtllibrary
rmmod mhvtl

echo "## Removing old mhvtl tapes"
rm -rf /opt/mhvtl

echo "## Re-creting /opt/mhvtl"
mkdir /opt/mhvtl
chown vtl.vtl /opt/mhvtl

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
  for t in `seq \`mtx -f /dev/$i status | grep Transfer | wc -l\` `; do
    mtx -f /dev/$i load $t $(($t - 1))
  done
  mtx -f /dev/$i status | grep Transfer
done

echo "## Starting system test program"
/usr/local/bin/TapeDriveReadWriteTest
ret=$?

echo "## Shutting down mhvtl"
service mhvtl stop
rmmod mhvtl

exit $ret
