#!/bin/bash

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "${BASH_SOURCE[0]}")] Started"

die() {
  echo "$@" 1>&2
  exit 1
}

# install the needed packages
dnf install -y cta-catalogueutils

echo "Wiping catalogue"
if ! (echo yes | cta-catalogue-schema-drop /etc/cta/cta-catalogue.conf); then
  # pause to let db come up
  echo "Database connection failed, pausing before a retry"
  sleep 5
  echo yes | cta-catalogue-schema-drop /etc/cta/cta-catalogue.conf || die "ERROR: Could not wipe database. cta-catalogue-schema-drop /etc/cta/cta-catalogue.conf FAILED"
fi

if [ "$CATALOGUE_BACKEND" == "oracle" ]; then
  # dnf install -y oracle-instantclient-sqlplus
  # echo "Purging Oracle recycle bin"
  # ORACLE_SQLPLUS="/usr/bin/sqlplus64"
  # test -f ${ORACLE_SQLPLUS} || echo "ERROR: ORACLE SQLPLUS client is not present, cannot purge recycle bin: ${ORACLE_SQLPLUS}"
  # LD_LIBRARY_PATH=$(readlink ${ORACLE_SQLPLUS} | sed -e 's;/bin/[^/]\+;/lib;') ${ORACLE_SQLPLUS} $(cat /etc/cta/cta-catalogue.conf | sed -e 's/oracle://') @/opt/ci/purge_database.ext
  # LD_LIBRARY_PATH=$(readlink ${ORACLE_SQLPLUS} | sed -e 's;/bin/[^/]\+;/lib;') ${ORACLE_SQLPLUS} $(cat /etc/cta/cta-catalogue.conf | sed -e 's/oracle://') @/opt/ci/purge_recyclebin.ext
  true
fi
echo "Catalogue wiped"

echo "Creating catalogue schema..."
cta-catalogue-schema-create -v $SCHEMA_VERSION /etc/cta/cta-catalogue.conf || die "ERROR: Could not create database schema. cta-catalogue-schema-create /etc/cta/cta-catalogue.conf FAILED"

echo "$(date '+%Y-%m-%d %H:%M:%S') [$(basename "$0")] Catalogue reset completed"
