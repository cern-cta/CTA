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

DATABASE_CONFIG_DIR=/etc/config/database

function get_conf {
  test -r ${DATABASE_CONFIG_DIR}/$1 && cat ${DATABASE_CONFIG_DIR}/$1 || echo -n UNDEF
}

DATABASETYPE=UNDEF
DATABASEURL=UNDEF

rm -f /tmp/database-rc.sh

case "$(get_conf database.type)" in
  "UNDEF")
    echo "database configmap is not defined"
    ls ${DATABASE_CONFIG_DIR}
    exit 1
    ;;

  "sqlite")
    echo "Configuring sqlite database"
    DATABASETYPE=sqlite
    DATABASEURL=sqlite:$(echo $(get_conf database.file.path) | sed -e "s#%NAMESPACE#${MY_NAMESPACE}#")
  ;;
  "oracle")
    echo "Configuring oracle database"
    DATABASETYPE=oracle
    DATABASEURL=oracle:$(get_conf database.oracle.username)/$(get_conf database.oracle.password)@$(get_conf database.oracle.database)
    # Update TNS name ASAP
    # oracle-instantclient-tnsnames.ora rpm must be installed before
    test -x /etc/cron.hourly/tnsnames-update.cron && /etc/cron.hourly/tnsnames-update.cron
    ;;
  "postgres")
     echo "Configuring postgres database"
     DATABASETYPE=postgres
     DATABASEURL=postgresql:postgresql://$(get_conf database.postgres.username):$(get_conf database.postgres.password)@$(get_conf database.postgres.server)/$(get_conf database.postgres.database)
  ;;
  *)
    echo "Error unknown database type: $(get_conf database.type)"
    exit 1
  ;;
esac

cat <<EOF >>/tmp/database-rc.sh
export DATABASETYPE=${DATABASETYPE}
export DATABASEURL=${DATABASEURL}
EOF

exit 0
