#!/bin/bash -e

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

# This script recreates an environment similar to the one created by
# it-puppet-hostgroup-cta profiles.

# (Re-)create the mhvtl environment
echo Wiping mhvtl and kubernetes library info...
systemctl stop mhvtl &> /dev/null || systemctl stop mhvtl.target &> /dev/null
sleep 2
rm -rf /etc/mhvtl
umount /opt/mhvtl || true
rm -rf /opt/mhvtl
rm -rf /opt/kubernetes/CTA/library
mkdir -p /opt/mhvtl
mount -t tmpfs -o size=512m tmpfs /opt/mhvtl

################################################################################
### puppet:///modules/hg_cta/00-cta-tape.rules
echo Installing udev rules...
cp -v 00-cta-tape.rules /etc/udev/rules.d/00-cta-tape.rules
chmod 0644 /etc/udev/rules.d/00-cta-tape.rules

################################################################################
### mhvtl config directory
echo Creating mhvtl config directory...
mkdir -p /etc/mhvtl

################################################################################
### mhvtl conf: template('hg_cta/mhvtl/mhvtl.conf.erb')
echo Installing mhvtl.conf....
cp -v mhvtl.conf /etc/mhvtl/mhvtl.conf
chmod 0644 /etc/mhvtl/mhvtl.conf

################################################################################
### template('hg_cta/mhvtl/generate_mhvtl_config.sh.erb')
echo mhvtl config pre-run
ls -lR /opt/mhvtl

echo Running mhvtl config generator...
ndrives=3
if [ -n "$1" ]; then
  echo "Using $1 number of drives instead of default (3)."
  ndrives=$1
fi
./generate_mhvtl_config.sh "$ndrives"

echo mhvtl config post run
ls -lR /opt/mhvtl


################################################################################
### Create mhvtl directory
mkdir -p /opt/mhvtl
sudo groupadd vtl || true
sudo adduser vtl -g vtl || true
sudo make_vtl_media -C /etc/mhvtl
chown -R vtl.vtl /opt/mhvtl

################################################################################
#### Start mhvtl
echo Starting mhvtl...
systemctl start mhvtl &> /dev/null || systemctl start mhvtl.target &> /dev/null
sleep 2

################################################################################
### puppet:///modules/hg_cta/generate_librarydevice_PV.sh
echo Generating kubernetes persistent volumes
./generate_librarydevice_PV.sh

################################################################################
### puppet:///modules/hg_cta/generate_PV.sh
echo Generating the log persistent volume
mkdir -pv /shared/logs
sudo kubectl delete pv log || true
sudo kubectl create -f ./log_PV.yaml
sudo kubectl get persistentvolumes -l type=logs

echo Generating the stg persistent volume
rm -rf /shared/stg
mkdir -pv /shared/stg
sudo kubectl delete pv stg || true
sudo kubectl create -f ./stg_PV.yaml
sudo kubectl get persistentvolumes -l type=stg

echo Generating the share host path
rm -rf /shared/cta
mkdir -pv /shared/cta/catdb
touch /shared/cta/catdb/catdb
chmod 1777 /shared/cta
