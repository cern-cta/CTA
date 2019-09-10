#!/bin/bash

. /opt/run/bin/init_pod.sh

if [ ! -e /etc/buildtreeRunner ]; then
  yum-config-manager --enable cta-artifacts
  yum-config-manager --enable ceph

  # Install missing RPMs
  yum -y install cta-cli cta-systemtest-helpers cta-debuginfo xrootd-client eos-client jq python36

  ## Keep this temporary fix that may be needed if going to protobuf3-3.5.1 for CTA
  # Install eos-protobuf3 separately as eos is OK with protobuf3 but cannot use it..
  # Andreas is fixing eos-(client|server) rpms to depend on eos-protobuf3 instead
  # yum -y install eos-protobuf3

fi

cat <<EOF > /etc/cta/cta-cli.conf
# The CTA frontend address in the form <FQDN>:<TCPPort>
# solved by kubernetes DNS server so KIS...
cta.endpoint ctafrontend:10955
EOF

if [ "-${CI_CONTEXT}-" == '-nosystemd-' ]; then
  # sleep forever but exit immediately when pod is deleted
  exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"
else
  # Add a DNS cache on the client as kubernetes DNS complains about `Nameserver limits were exceeded`
  yum install -y systemd-resolved
  systemctl start systemd-resolved
fi
