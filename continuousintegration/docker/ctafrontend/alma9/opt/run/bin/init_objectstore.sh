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

OBJECTSTORE_CONFIG_DIR=/etc/config/objectstore

function get_conf {
  test -r ${OBJECTSTORE_CONFIG_DIR}/$1 && cat ${OBJECTSTORE_CONFIG_DIR}/$1 || echo -n UNDEF
}

OBJECTSTORETYPE=UNDEF
OBJECTSTOREURL=UNDEF

rm -f /tmp/objectstore-rc.sh

case "$(get_conf objectstore.type)" in
  "UNDEF")
    echo "objectstore configmap is not defined"
    ls ${OBJECTSTORE_CONFIG_DIR}
    exit 1
    ;;

  "ceph")
    echo "Configuring ceph objectstore"

    cat <<EOF >/etc/ceph/ceph.conf
[global]
   mon host = $(get_conf objectstore.ceph.mon):$(get_conf objectstore.ceph.monport)
EOF

    cat <<EOF >/etc/ceph/ceph.client.$(get_conf objectstore.ceph.id).keyring
[client.$(get_conf objectstore.ceph.id)]
    key = $(get_conf objectstore.ceph.key)
    caps mon = "allow r"
    caps osd = "allow rwx pool=$(get_conf objectstore.ceph.pool) namespace=$(get_conf objectstore.ceph.namespace)"
EOF
    OBJECTSTORETYPE=ceph
    OBJECTSTOREURL="rados://$(get_conf objectstore.ceph.id)@$(get_conf objectstore.ceph.pool):$(get_conf objectstore.ceph.namespace)"
    echo "export OBJECTSTORENAMESPACE=$(get_conf objectstore.ceph.namespace)" >> /tmp/objectstore-rc.sh
    echo "export OBJECTSTOREID=$(get_conf objectstore.ceph.id)" >> /tmp/objectstore-rc.sh
    echo "export OBJECTSTOREPOOL=$(get_conf objectstore.ceph.pool)" >> /tmp/objectstore-rc.sh
    ;;
  "file")
    echo "Configuring file objectstore"
    OBJECTSTORETYPE=file
    OBJECTSTOREURL=$(echo $(get_conf objectstore.file.path) | sed -e "s#%NAMESPACE#${MY_NAMESPACE}#")
  ;;
  *)
    echo "Error unknown objectstore type: $(get_conf objectstore.type)"
    exit 1
  ;;
esac

cat <<EOF >>/tmp/objectstore-rc.sh
export OBJECTSTORETYPE=${OBJECTSTORETYPE}
export OBJECTSTOREURL=${OBJECTSTOREURL}
EOF

exit 0
