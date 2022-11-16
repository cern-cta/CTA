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

LOGMOUNT=/mnt/logs

PV_PATH=""

PV_PATH="${LOGMOUNT}/${MY_NAME}"
mkdir -p ${PV_PATH}

echo "Copying initial /var/log content to ${PV_PATH}"
cd /var/log
tar -c . | tar -C ${PV_PATH} -xv

echo "Mounting logs volume ${PV_PATH} in /var/log"
mount --bind ${PV_PATH} /var/log

# all core dumps will go there as all the pods AND kubelet are sharing the same kernel.core_pattern
mkdir -p /var/log/tmp
chmod 1777 /var/log/tmp
echo '/var/log/tmp/%h-%t-%e-%p-%s.core' > /proc/sys/kernel/core_pattern

# This libraries are needed to install oracle-instant-client
yum install --assumeyes wget libaio;

# Creation of cta-catalogue.con
/shared/scripts/init_database.sh;
. /tmp/database-rc.sh;
mkdir -p /shared/etc_cta;
echo ${DATABASEURL} &> /shared/etc_cta/cta-catalogue.conf;

cd /root
if [[ $CTA_VERSION ]]
then
  echo "CTA_VERSION"
  /entrypoint.sh -d -v ${CTA_VERSION} -f ${CATALOGUE_SOURCE_VERSION} -t ${CATALOGUE_DESTINATION_VERSION} -c update;
else
  echo "COMMIT_ID"
  /entrypoint.sh -d -i ${COMMIT_ID} -f ${CATALOGUE_SOURCE_VERSION} -t ${CATALOGUE_DESTINATION_VERSION} -c update;
fi

