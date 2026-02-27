#!/bin/bash

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -e

# install the needed packages
dnf install -y cta-catalogueutils

echo "Wiping catalogue"
echo yes | cta-catalogue-schema-drop /etc/cta/cta-catalogue.conf

if [[ "$CATALOGUE_BACKEND" == "oracle" ]]; then
  # dnf install -y oracle-instantclient-sqlplus
  # echo "Purging Oracle recycle bin"
  # ORACLE_SQLPLUS="/usr/bin/sqlplus64"
  # test -f ${ORACLE_SQLPLUS} || echo "ERROR: ORACLE SQLPLUS client is not present, cannot purge recycle bin: ${ORACLE_SQLPLUS}"
  # LD_LIBRARY_PATH=$(readlink ${ORACLE_SQLPLUS} | sed -e 's;/bin/[^/]\+;/lib;') ${ORACLE_SQLPLUS} $(cat /etc/cta/cta-catalogue.conf | sed -e 's/oracle://') @/opt/ci/purge_recyclebin.ext
  true
fi
echo "Catalogue wiped"

echo "Creating catalogue schema..."
cta-catalogue-schema-create -v $SCHEMA_VERSION /etc/cta/cta-catalogue.conf
