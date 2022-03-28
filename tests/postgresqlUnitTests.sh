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

# Utility script to run postgresql tests like in CI.

if ! rpm -q --quiet rh-postgresql12-postgresql-server; then
  echo "Please install the package rh-postgresql12-postgresql-server to run this script."
  exit 1
fi

if [[ !  ( -x ./catalogue/cta-catalogue-schema-create && -x ./tests/cta-rdbmsUnitTests ) ]]; then
  echo "Please run this script from a build tree of CTA."
  exit 1
fi

if [[ $(id -u) != "0" ]]; then
  echo "Please run this script as root (sudo)."
  exit 1;
fi

POSTGRESQL_DATA_DIR=/usr/local/cta_test_pgsql_data
POSTGRESQL_LOG_DIR=/var/log/postgres
echo POSTGRESQL_DATA_DIR=${POSTGRESQL_DATA_DIR}
echo POSTGRESQL_LOG_DIR=${POSTGRESQL_LOG_DIR}
mkdir -p ${POSTGRESQL_DATA_DIR}
chown -R postgres:postgres ${POSTGRESQL_DATA_DIR}
mkdir -p ${POSTGRESQL_LOG_DIR}
chown -R postgres:postgres ${POSTGRESQL_LOG_DIR}
export LD_LIBRARY_PATH=/opt/rh/rh-postgresql12/root/usr/lib64
POSTGRES_BIN=/opt/rh/rh-postgresql12/root/usr/bin
echo POSTGRES_BIN=${POSTGRES_BIN}
(cd / ;
runuser -u postgres -- ${POSTGRES_BIN}/initdb -D ${POSTGRESQL_DATA_DIR}
runuser -u postgres -- ${POSTGRES_BIN}/pg_ctl start -w -t 10 -D ${POSTGRESQL_DATA_DIR} -l ${POSTGRESQL_LOG_DIR}/cta_test_postgres.log
runuser -u postgres -- ${POSTGRES_BIN}/createdb cta
runuser -u postgres -- ${POSTGRES_BIN}/createuser -E cta
)
CTA_CATALOGUE_CONF=`mktemp`
echo CTA_CATALOGUE_CONF=${CTA_CATALOGUE_CONF}
echo 'postgresql:postgresql://cta@localhost/cta' > ${CTA_CATALOGUE_CONF}
./catalogue/cta-catalogue-schema-create ${CTA_CATALOGUE_CONF}
./tests/cta-rdbmsUnitTests ${CTA_CATALOGUE_CONF}
(cd / ; runuser -u postgres -- ${POSTGRES_BIN}/pg_ctl stop -D ${POSTGRESQL_DATA_DIR})
rm -rf ${POSTGRESQL_DATA_DIR}
