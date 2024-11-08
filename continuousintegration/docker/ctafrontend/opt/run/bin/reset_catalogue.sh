#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2024 CERN
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

echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "${BASH_SOURCE[0]}")] Started"

die() {
  stdbuf -i 0 -o 0 -e 0 echo "$@"
  sleep 1
  exit 1
}

# install the needed packages
# the scheduler tools are installed once the scheduler type is known (see below)
yum -y install cta-catalogueutils
yum clean packages

echo "Using catalogue backend: $CATALOGUE_BACKEND"

CATALOGUE_URL=$(cat /etc/cta/cta-catalogue.conf)

echo "Wiping catalogue"
if [ "$CATALOGUE_BACKEND" != "sqlite" ]; then
  if ! (echo yes | cta-catalogue-schema-drop /etc/cta/cta-catalogue.conf); then
    # pause to let db come up
    echo "Database connection failed, pausing before a retry"
    sleep 5
    echo yes | cta-catalogue-schema-drop /etc/cta/cta-catalogue.conf || die "ERROR: Could not wipe database. cta-catalogue-schema-drop /etc/cta/cta-catalogue.conf FAILED"
    echo "Database wiped"
  fi
else
  rm -fr $(dirname $(echo ${CATALOGUE_URL} | cut -d: -f2))
fi

if [ "$CATALOGUE_BACKEND" == "sqlite" ]; then
  mkdir -p $(dirname $(echo ${CATALOGUE_URL} | cut -d: -f2))
  cta-catalogue-schema-create -v $SCHEMA_VERSION /etc/cta/cta-catalogue.conf || die "ERROR: Could not create database schema. cta-catalogue-schema-create /etc/cta/cta-catalogue.conf FAILED"
  chmod -R 777 $(dirname $(echo ${CATALOGUE_URL} | cut -d: -f2)) # needed?
elif [ "$CATALOGUE_BACKEND" == "oracle" ]; then
  # yum install -y wget
  # wget https://download.oracle.com/otn_software/linux/instantclient/2112000/el9/oracle-instantclient-sqlplus-21.12.0.0.0-1.el9.x86_64.rpm
  # yum install -y oracle-instantclient-sqlplus-21.12.0.0.0-1.el9.x86_64.rpm
  # echo "Purging Oracle recycle bin"
  # ORACLE_SQLPLUS="/usr/bin/sqlplus64"
  # test -f ${ORACLE_SQLPLUS} || echo "ERROR: ORACLE SQLPLUS client is not present, cannot purge recycle bin: ${ORACLE_SQLPLUS}"
  # LD_LIBRARY_PATH=$(readlink ${ORACLE_SQLPLUS} | sed -e 's;/bin/[^/]\+;/lib;') ${ORACLE_SQLPLUS} $(echo $CATALOGUE_URL | sed -e 's/oracle://') @/opt/ci/purge_database.ext
  # LD_LIBRARY_PATH=$(readlink ${ORACLE_SQLPLUS} | sed -e 's;/bin/[^/]\+;/lib;') ${ORACLE_SQLPLUS} $(echo $CATALOGUE_URL | sed -e 's/oracle://') @/opt/ci/purge_recyclebin.ext
  cta-catalogue-schema-create -v $SCHEMA_VERSION /etc/cta/cta-catalogue.conf || die "ERROR: Could not create database schema. cta-catalogue-schema-create /etc/cta/cta-catalogue.conf FAILED"
elif [ "$CATALOGUE_BACKEND" == "postgres" ]; then
  echo "Creating Postgres Catalogue schema"
  cta-catalogue-schema-create -v $SCHEMA_VERSION /etc/cta/cta-catalogue.conf || die "ERROR: Could not create Postgres database schema. cta-catalogue-schema-create /etc/cta/cta-catalogue.conf FAILED"
else
  die "ERROR: Unsupported database type: ${CATALOGUE_BACKEND}"
fi

echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "$0")] Catalogue reset completed"
