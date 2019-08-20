#!/bin/bash

cat <<EOF >/etc/cta/castor-migration.conf
castor.db_login               oracle:castor/<password>@castor
castor.json                   true
castor.max_num_connections    1
castor.batch_size             100
castor.prefix                 /castor/cern.ch
eos.dry_run                   false
eos.prefix                    /eos/grpc
eos.endpoint                  eoscta:50051
eos.token                     migrationtesttoken
EOF

exit 0
