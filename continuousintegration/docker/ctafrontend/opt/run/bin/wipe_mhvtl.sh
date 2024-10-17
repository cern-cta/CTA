#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2022 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.

. /opt/run/bin/init_pod.sh

die() {
  stdbuf -i 0 -o 0 -e 0 echo "$@"
  sleep 1
  exit 1
}

# install the needed packages
# the scheduler tools are installed once the scheduler type is known (see below)
yum -y install mt-st mtx lsscsi sg3_utils
yum clean packages

# library management
# BEWARE STORAGE SLOTS START @1 and DRIVE SLOTS START @0!!
# Emptying drives and move tapes to home slots
echo "Unloading tapes that could be remaining in the drives from previous runs"
mtx -f /dev/${LIBRARYDEVICE} status
for unload in $(mtx -f /dev/${LIBRARYDEVICE}  status | grep '^Data Transfer Element' | grep -vi ':empty' | sed -e 's/Data Transfer Element /drive/;s/:.*Storage Element /-slot/;s/ .*//'); do
  # normally, there is no need to rewind with virtual tapes...
  mtx -f /dev/${LIBRARYDEVICE} unload $(echo ${unload} | sed -e 's/^.*-slot//') $(echo ${unload} | sed -e 's/drive//;s/-.*//') || echo "COULD NOT UNLOAD TAPE"
done
#  echo "Labelling tapes using the first drive in ${LIBRARYNAME}: ${DRIVENAMES[${driveslot}]} on /dev/${DRIVEDEVICES[${driveslot}]}:"
#  for ((i=0; i<${#TAPES[@]}; i++)); do
#    vid=${TAPES[${i}]}
#    tapeslot=$((${i}+1)) # tape slot is 1 for tape[0] and so on...
#
#    echo -n "${vid} in slot ${tapeslot} "
#    mtx -f /dev/${LIBRARYDEVICE} load ${tapeslot} ${driveslot}
#    cd /tmp
#      echo "VOL1${vid}                           CASTOR                                    3">label.file
#      mt -f /dev/${DRIVEDEVICES[${driveslot}]} rewind
#      dd if=label.file of=/dev/${DRIVEDEVICES[${driveslot}]} bs=80 count=1
#      mt -f /dev/${DRIVEDEVICES[${driveslot}]} rewind
#    mtx -f /dev/${LIBRARYDEVICE} unload ${tapeslot} ${driveslot}
#    echo "OK"
#  done

echo "### WIPE COMPLETED ###"
