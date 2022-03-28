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

echo Installing kubernetes
sudo yum install -y kubernetes etcd flannel
sudo systemctl enable etcd
sudo systemctl start etcd
sudo mkdir -p /etc/kubernetes
sudo cp -rv kubernetes/* /etc/kubernetes
sudo perl -p -i -e 's/^(KUBELET_ARGS=).*$/$1"--allow_privileged=true --cluster-dns=10.254.199.254 --cluster-domain=cluster.local"/' /etc/kubernetes/kubelet
sudo perl -p -i -e 's/^(KUBELET_POD_INFRA_CONTAINER=)/#$1/' /etc/kubernetes/kubelet
# We put the config in 2 places as flanneld might fetch it from different places.
curl -L http://localhost:2379/v2/keys/flannel/network/config -XPUT --data-urlencode value@kubernetes/flannel-config.json
curl -L http://localhost:2379/v2/keys/atomic.io/network/config -XPUT --data-urlencode value@kubernetes/flannel-config.json
sudo systemctl enable flanneld
sudo systemctl start flanneld
sudo systemctl enable kubelet
sudo systemctl start kubelet
sudo perl -p -i -e 's/^(KUBE_ADMISSION_CONTROL)/#$1/' /etc/kubernetes/apiserver
sudo perl -p -i -e 's/^(KUBE_API_ARGS=).*$/$1"--allow_privileged=true"/' /etc/kubernetes/apiserver
sudo perl -p -i -e 's/^(KUBE_API_ADDRESS=).*$/$1"--insecure-bind-address=0.0.0.0"/' /etc/kubernetes/apiserver
sudo perl -p -i -e 's/^(KUBE_PROXY_ARGS=).*$/$1"--healthz-port=0 --master=http:\/\/127.0.0.1:8080 --oom-score-adj=-999 --proxy-mode=userspace --proxy-port-range=0-0"/' /etc/kubernetes/proxy
sudo systemctl enable kube-apiserver
sudo systemctl start kube-apiserver
sudo systemctl enable kube-proxy
sudo systemctl start kube-proxy
sudo systemctl enable kube-scheduler
sudo systemctl start kube-scheduler
sudo systemctl enable kube-controller-manager
sudo systemctl start kube-controller-manager

echo Setting up kubernetes DNS...
dns_clusterip='10.254.199.254'
#dns_domainname=`hostname -d`
dns_domainname=cluster.local
docker_ipaddress=`ifconfig docker0  | perl -e 'while(<>) { if (/inet\s+(\S+)\s/) { print $1."\n"; } }'`
sudo systemctl stop firewalld || true
sudo systemctl disable firewalld || true
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

echo "DONE. A reboot might be necessary to get kubernetes networking to work..."
