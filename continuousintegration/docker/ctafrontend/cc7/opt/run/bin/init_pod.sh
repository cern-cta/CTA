#!/bin/bash

echo -n "Fixing reverse DNS for $(hostname): "
sed -i -c "s/^\($(hostname -i)\)\s\+.*$/\1 $(hostname -s).$(grep search /etc/resolv.conf | cut -d\  -f2) $(hostname -s)/" /etc/hosts
echo "DONE"

# Not needed anymore, keep it in case it comes back
#echo -n "Yum should resolve names using IPv4 DNS: "
#echo "ip_resolve=IPv4" >> /etc/yum.conf
#echo "DONE"
