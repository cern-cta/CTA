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

# oracle sqlplus client binary path
ORACLE_SQLPLUS="/usr/bin/sqlplus64"

die() {
  stdbuf -i 0 -o 0 -e 0 echo "$@"
  sleep 1
  exit 1
}

# enable cta repository from previously built artifacts
yum-config-manager --enable cta-artifacts
yum-config-manager --enable ceph

# install the needed packages
# the scheduler tools are installed once the scheduler type is known (see below)
yum -y install mt-st mtx lsscsi sg3_utils cta-catalogueutils ceph-common
yum clean packages

echo "Using this configuration for library:"
/opt/run/bin/init_library.sh
cat /tmp/library-rc.sh
. /tmp/library-rc.sh

echo "Configuring Scheduler store:"
/opt/run/bin/init_objectstore.sh
. /tmp/objectstore-rc.sh
echo ${OBJECTSTOREURL} >/etc/cta/cta-scheduler.conf

if [ "$OBJECTSTORETYPE" == "postgres" ]; then
    echo "Installing the cta-scheduler-utils"
    yum -y install cta-scheduler-utils
else
    echo "Installing the cta-objectstore-tools"
    yum -y install cta-objectstore-tools
fi

if [ "$KEEP_OBJECTSTORE" == "0" ]; then
  if [ "$OBJECTSTORETYPE" == "file" ]; then
    echo "Wiping objectstore"
    rm -fr $OBJECTSTOREURL
    mkdir -p $OBJECTSTOREURL
    cta-objectstore-initialize $OBJECTSTOREURL || die "ERROR: Could not Wipe the objectstore. cta-objectstore-initialize $OBJECTSTOREURL FAILED"
    chmod -R 777 $OBJECTSTOREURL
  elif [ "$OBJECTSTORETYPE" == "postgres" ]; then
    echo "Postgres scheduler config file content: "
    cat /etc/cta/cta-scheduler.conf
    echo "Droping the scheduler DB schema"
    echo "yes" | cta-scheduler-schema-drop /etc/cta/cta-scheduler.conf || die "ERROR: Could not drop scheduler schema. cta-scheduler-schema-drop /etc/cta/cta-scheduler.conf FAILED"
    echo "Creating the scheduler DB schema"
    cta-scheduler-schema-create /etc/cta/cta-scheduler.conf || die "ERROR: Could not create scheduler schema. cta-scheduler-schema-create /etc/cta/cta-scheduler.conf FAILED"
  else
    echo "Wiping objectstore"
    if [[ $(rados -p $OBJECTSTOREPOOL --id $OBJECTSTOREID --namespace $OBJECTSTORENAMESPACE ls | wc -l) -gt 0 ]]; then
      echo "Rados objectstore ${OBJECTSTOREURL} is not empty: deleting content"
      rados -p $OBJECTSTOREPOOL --id $OBJECTSTOREID --namespace $OBJECTSTORENAMESPACE ls | xargs -L 100 -P 100 rados -p $OBJECTSTOREPOOL --id $OBJECTSTOREID --namespace $OBJECTSTORENAMESPACE rm
    fi
    cta-objectstore-initialize $OBJECTSTOREURL || die "ERROR: Could not Wipe the objectstore. cta-objectstore-initialize $OBJECTSTOREURL FAILED"
    echo "Rados objectstore ${OBJECTSTOREURL} content:"
    rados -p $OBJECTSTOREPOOL --id $OBJECTSTOREID --namespace $OBJECTSTORENAMESPACE ls
  fi
else
  echo "Reusing the existing scheduling backend (no check)"
fi

echo "Configuring database:"
/opt/run/bin/init_database.sh
. /tmp/database-rc.sh
echo ${DATABASEURL} >/etc/cta/cta-catalogue.conf

if [ "$KEEP_DATABASE" == "0" ]; then
  echo "Wiping database"
  if [ "$DATABASETYPE" != "sqlite" ]; then
    if ! (echo yes | cta-catalogue-schema-drop /etc/cta/cta-catalogue.conf); then
      # pause to let db come up
      echo "Database connection failed, pausing before a retry"
      sleep 5
      echo yes | cta-catalogue-schema-drop /etc/cta/cta-catalogue.conf || die "ERROR: Could not wipe database. cta-catalogue-schema-drop /etc/cta/cta-catalogue.conf FAILED"
      echo "Database wiped"
    fi
  else
    rm -fr $(dirname $(echo ${DATABASEURL} | cut -d: -f2))
  fi

  if [ "$DATABASETYPE" == "sqlite" ]; then
    mkdir -p $(dirname $(echo ${DATABASEURL} | cut -d: -f2))
    cta-catalogue-schema-create -v $SCHEMA_VERSION /etc/cta/cta-catalogue.conf || die "ERROR: Could not create database schema. cta-catalogue-schema-create /etc/cta/cta-catalogue.conf FAILED"
    chmod -R 777 $(dirname $(echo ${DATABASEURL} | cut -d: -f2)) # needed?
  elif [ "$DATABASETYPE" == "oracle" ]; then
    echo "Purging Oracle recycle bin"
    test -f ${ORACLE_SQLPLUS} || echo "ERROR: ORACLE SQLPLUS client is not present, cannot purge recycle bin: ${ORACLE_SQLPLUS}"
    LD_LIBRARY_PATH=$(readlink ${ORACLE_SQLPLUS} | sed -e 's;/bin/[^/]\+;/lib;') ${ORACLE_SQLPLUS} $(echo $DATABASEURL | sed -e 's/oracle://') @/opt/ci/init/purge_database.ext
    LD_LIBRARY_PATH=$(readlink ${ORACLE_SQLPLUS} | sed -e 's;/bin/[^/]\+;/lib;') ${ORACLE_SQLPLUS} $(echo $DATABASEURL | sed -e 's/oracle://') @/opt/ci/init/purge_recyclebin.ext
    cta-catalogue-schema-create -v $SCHEMA_VERSION /etc/cta/cta-catalogue.conf || die "ERROR: Could not create database schema. cta-catalogue-schema-create /etc/cta/cta-catalogue.conf FAILED"
  elif [ "$DATABASETYPE" == "postgres" ]; then
    echo "Creating Postgres Catalogue schema"
    cta-catalogue-schema-create -v $SCHEMA_VERSION /etc/cta/cta-catalogue.conf || die "ERROR: Could not create Postgres database schema. cta-catalogue-schema-create /etc/cta/cta-catalogue.conf FAILED"
  else
    die "ERROR: Unsupported database type: ${DATABASETYPE}"
  fi
else
  echo "Reusing database (no check)"
fi


if [ ! $LIBRARYTYPE == "mhvtl" ]; then
  echo "Real tapes - do nothing";
else
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
fi

echo "### INIT COMPLETED ###"