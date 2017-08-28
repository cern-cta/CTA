#!/bin/bash

. /opt/run/bin/init_pod.sh

yum-config-manager --enable cta-artifacts
yum-config-manager --enable ceph

# Install missing RPMs
yum -y install cta-cli cta-debuginfo xrootd-client eos-client

cat <<EOF > /etc/cta/cta-cli.conf
# The CTA frontend address in the form <FQDN>:<TCPPort>
# solved by kubernetes DNS server so KIS...
ctafrontend:10955
EOF

# sleep forever but exit immediately when pod is deleted
exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"
