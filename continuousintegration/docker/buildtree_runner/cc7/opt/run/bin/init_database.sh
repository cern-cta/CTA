#!/bin/bash

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
