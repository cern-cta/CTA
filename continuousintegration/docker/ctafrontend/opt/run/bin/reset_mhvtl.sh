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

die() {
  stdbuf -i 0 -o 0 -e 0 echo "$@"
  sleep 1
  exit 1
}


. /opt/run/bin/init_pod.sh

echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "${BASH_SOURCE[0]}")] Started"
library_devices="$1"

if [[ -z "$library_devices" ]]; then
  die "No library devices to reset"
fi


# install the needed packages
# the scheduler tools are installed once the scheduler type is known (see below)
yum -y install mt-st mtx lsscsi sg3_utils
yum clean packages

# library management
# BEWARE STORAGE SLOTS START @1 and DRIVE SLOTS START @0!!
# Emptying drives and move tapes to home slots
echo "Unloading tapes that could be remaining in the drives from previous runs"
for library_device in ${library_devices}; do
  echo "Cleaning library device: /dev/${library_device}"
  mtx -f /dev/${library_device} status
  for unload in $(mtx -f /dev/${library_device}  status | grep '^Data Transfer Element' | grep -vi ':empty' | sed -e 's/Data Transfer Element /drive/;s/:.*Storage Element /-slot/;s/ .*//'); do
    # normally, there is no need to rewind with virtual tapes...
    mtx -f /dev/${library_device} unload $(echo ${unload} | sed -e 's/^.*-slot//') $(echo ${unload} | sed -e 's/drive//;s/-.*//') || echo "COULD NOT UNLOAD TAPE"
  done
done

echo "### WIPE COMPLETED ###"
echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "$0")] Done"
