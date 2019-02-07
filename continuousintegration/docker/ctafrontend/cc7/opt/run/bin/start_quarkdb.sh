#!/bin/bash

QUARKDB_CONFIG=/etc/config/eos/xrootd-quarkdb.cfg

QUARKDB_DIRECTORY=$(cat ${QUARKDB_CONFIG} | grep redis.database | sed -e 's/.*redis.database\ \+//')

yum-config-manager --enable eos-quarkdb

yum install -y quarkdb

quarkdb-create --path ${QUARKDB_DIRECTORY}

chown -R xrootd:xrootd ${QUARKDB_DIRECTORY}

cp -f ${QUARKDB_CONFIG} /etc/xrootd/xrootd-quarkdb.cfg

systemctl start xrootd@quarkdb

systemctl status xrootd@quarkdb
