#!/bin/bash -e

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

echo Removing kubernetes DNS...

sudo kubectl delete ns kube-system || true

echo -n Waiting kubernetes DNS termination
while (kubectl get ns kube-system  2> /dev/null > /dev/null); do echo -n .; sleep 1; done ; echo Done...

echo Setting up kubernetes DNS...
dns_clusterip='10.254.199.254'
#dns_domainname=`hostname -d`
dns_domainname=cluster.local
docker_ipaddress=`ifconfig docker0  | perl -e 'while(<>) { if (/inet\s+(\S+)\s/) { print $1."\n"; } }'`
sudo systemctl stop firewalld
sudo systemctl disable firewalld
#my_ipaddress=127.0.0.1
upper_level_dns=`perl -e 'while (<>) { if ($_=~ /nameserver (.*)/) { print $1."\n"; exit }}' < /etc/resolv.conf`

echo dns_clusterip=${dns_clusterip}
echo dns_domainname=${dns_domainname}
echo docker_ipaddress=${docker_ipaddress}
echo upper_level_dns=${upper_level_dns}

dnspoddir=$(mktemp -d)
cp kubernetes/dns-svc.yaml kubernetes/pod-dns.yaml ${dnspoddir}

perl -p -i -e 's/<%= \@dns_clusterip -%>/'"${dns_clusterip}"'/' ${dnspoddir}/dns-svc.yaml

perl -p -i -e 's/<%= \@dns_domainname -%>/'"${dns_domainname}"'/' ${dnspoddir}/pod-dns.yaml
perl -p -i -e 's/<%= \@my_ipaddress -%>/'"${docker_ipaddress}"'/' ${dnspoddir}/pod-dns.yaml
perl -p -i -e 's/<%= \@upper_level_dns -%>/'"${upper_level_dns}"'/' ${dnspoddir}/pod-dns.yaml

sudo kubectl create -f kubernetes/kube-system_ns.yaml || true
sudo kubectl create -f ${dnspoddir}/dns-svc.yaml
sudo kubectl create -f ${dnspoddir}/pod-dns.yaml

rm -rf ${dnspoddir}
